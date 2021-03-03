/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.cloud;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.common.util.Utils;

public class Replica extends ZkNodeProps {

  /**
   * The replica's state. In general, if the node the replica is hosted on is
   * not under {@code /live_nodes} in ZK, the replica's state should be
   * discarded.
   */
  public enum State {
    
    /**
     * The replica is ready to receive updates and queries.
     * <p>
     * <b>NOTE</b>: when the node the replica is hosted on crashes, the
     * replica's state may remain ACTIVE in ZK. To determine if the replica is
     * truly active, you must also verify that its {@link Replica#getNodeName()
     * node} is under {@code /live_nodes} in ZK (or use
     * {@link ZkStateReader#isNodeLive(String)} (String)}).
     * </p>
     */
    ACTIVE,

    LEADER,
    
    /**
     * The first state before {@link State#RECOVERING}. A node in this state
     * should be actively trying to move to {@link State#RECOVERING}.
     * <p>
     * <b>NOTE</b>: a replica's state may appear DOWN in ZK also when the node
     * it's hosted on gracefully shuts down. This is a best effort though, and
     * should not be relied on.
     * </p>
     */
    DOWN,
    
    /**
     * The node is recovering from the leader. This might involve peer-sync,
     * full replication or finding out things are already in sync.
     */
    RECOVERING,

    BUFFERING,
    
    /**
     * Recovery attempts have not worked, something is not right.
     * <p>
     * <b>NOTE</b>: This state doesn't matter if the node is not part of
     * {@code /live_nodes} in ZK; in that case the node is not part of the
     * cluster and it's state should be discarded.
     * </p>
     */
    RECOVERY_FAILED;
    
    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }

    public static String getShortState(State state) {
      if (state.equals(RECOVERY_FAILED)) {
        return "f";
      }
      return state.toString().substring(0,1).toLowerCase(Locale.ROOT);
    }

    public static State shortStateToState(String shortState) {
      if (shortState.equals("a")) {
        return State.ACTIVE;
      } if (shortState.equals("l")) {
        return State.LEADER;
      } else if (shortState.equals("r")) {
        return State.RECOVERING;
      } else if (shortState.equals("b")) {
        return State.BUFFERING;
      } else if (shortState.equals("d")) {
        return State.DOWN;
      } else if (shortState.equals("f")) {
        return State.RECOVERY_FAILED;
      }
      throw new IllegalStateException("Unknown state: " + shortState);
    }
    
    /** Converts the state string to a State instance. */
    public static State getState(String stateStr) {
      return stateStr == null ? null : State.valueOf(stateStr.toUpperCase(Locale.ROOT));
    }
  }

  public enum Type {
    /**
     * Writes updates to transaction log and indexes locally. Replicas of type {@link Type#NRT} support NRT (soft commits) and RTG. 
     * Any {@link Type#NRT} replica can become a leader. A shard leader will forward updates to all active {@link Type#NRT} and
     * {@link Type#TLOG} replicas. 
     */
    NRT,
    /**
     * Writes to transaction log, but not to index, uses replication. Any {@link Type#TLOG} replica can become leader (by first
     * applying all local transaction log elements). If a replica is of type {@link Type#TLOG} but is also the leader, it will behave 
     * as a {@link Type#NRT}. A shard leader will forward updates to all active {@link Type#NRT} and {@link Type#TLOG} replicas.
     */
    TLOG,
    /**
     * Doesn’t index or writes to transaction log. Just replicates from {@link Type#NRT} or {@link Type#TLOG} replicas. {@link Type#PULL}
     * replicas can’t become shard leaders (i.e., if there are only pull replicas in the collection at some point, updates will fail
     * same as if there is no leaders, queries continue to work), so they don’t even participate in elections.
     */
    PULL;

    public static Type get(String name){
      return name == null ? Type.NRT : Type.valueOf(name.toUpperCase(Locale.ROOT));
    }
  }

  public interface NodeNameToBaseUrl {
    String getBaseUrlForNodeName(final String nodeName);
  }

  private final String name;
  private final String nodeName;
  private State state;
  private final Type type;
  public final String slice, collection;
  private final String baseUrl;

  public Replica(String name, Map<String,Object> propMap, String collection, Long collectionId, String slice, NodeNameToBaseUrl nodeNameToBaseUrl) {
    super(propMap);
    this.collection = collection;
    this.slice = slice;
    this.name = name;

    this.nodeName = (String) propMap.get(ZkStateReader.NODE_NAME_PROP);

    String rawId = (String) propMap.get("id");

    if (!rawId.contains(":")) {
      this.id = Long.parseLong(rawId);
    }

    this.collectionId = collectionId;

    this.baseUrl = nodeNameToBaseUrl.getBaseUrlForNodeName(this.nodeName);
    type = Type.get((String) propMap.get(ZkStateReader.REPLICA_TYPE));
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.slice, "'slice' must not be null");
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.nodeName, "'node_name' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    Objects.requireNonNull(this.collectionId, "'collectionId' must not be null");

    if (propMap.get(ZkStateReader.STATE_PROP) != null) {
      if (propMap.get(ZkStateReader.STATE_PROP) instanceof State) {
        this.state = (State) propMap.get(ZkStateReader.STATE_PROP);
      } else {
        this.state = State.getState((String) propMap.get(ZkStateReader.STATE_PROP));
      }
    } else {
      this.state = State.DOWN;                         //Default to DOWN
      propMap.put(ZkStateReader.STATE_PROP, state.toString());
    }
  }

  public Replica(String name, Map<String,Object> propMap, String collection, Long collectionId, String slice, String baseUrl) {
    super(propMap);
    this.collection = collection;
    this.slice = slice;
    this.name = name;
    this.nodeName = (String) propMap.get(ZkStateReader.NODE_NAME_PROP);
    this.id = propMap.containsKey("id") ? Long.parseLong((String) propMap.get("id")) : null;
    this.collectionId = collectionId;

    Objects.requireNonNull(this.collectionId, "'collectionId' must not be null");
    this.baseUrl =  baseUrl;
    type = Type.get((String) propMap.get(ZkStateReader.REPLICA_TYPE));
    if (propMap.get(ZkStateReader.STATE_PROP) != null) {
      if (propMap.get(ZkStateReader.STATE_PROP) instanceof  State) {
        this.state = (State) propMap.get(ZkStateReader.STATE_PROP);
      } else {
        this.state = State.getState((String) propMap.get(ZkStateReader.STATE_PROP));
      }
    } else {
      this.state = State.DOWN;                         //Default to DOWN
      propMap.put(ZkStateReader.STATE_PROP, state.toString());
    }
  }

  Long id;
  final Long collectionId;

  public String getId() {
    return collectionId + "-" + id.toString();
  }

  public Long getCollectionId() {
    return collectionId;
  }


  public String getCollection(){
    return collection;
  }
  public String getSlice(){
    return slice;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    Replica replica = (Replica) o;

    return name.equals(replica.name);
  }

  /** Also known as coreNodeName. */
  public String getName() {
    return name;
  }

  public String getCoreUrl() {
    return getCoreUrl(getBaseUrl(), name);
  }
  public String getBaseUrl() {
    return baseUrl;
  }

  /** The name of the node this replica resides on */
  public String getNodeName() {
    return nodeName;
  }
  
  /** Returns the {@link State} of this replica. */
  public State getState() {
    return state;
  }

  // only to be used by ZkStateWriter currently
  public void setState(State state) {
    this.state = state;
    propMap.put(ZkStateReader.STATE_PROP, state.toString());
  }

  public boolean isActive(Set<String> liveNodes) {
    return this.nodeName != null && liveNodes.contains(this.nodeName) && this.state == State.ACTIVE;
  }
  
  public Type getType() {
    return this.type;
  }

  public String getProperty(String propertyName) {
    final String propertyKey;
    if (!propertyName.startsWith(ZkStateReader.PROPERTY_PROP_PREFIX)) {
      propertyKey = ZkStateReader.PROPERTY_PROP_PREFIX + propertyName;
    } else {
      propertyKey = propertyName;
    }
    final String propertyValue = getStr(propertyKey);
    return propertyValue;
  }

  public static String getCoreUrl(String baseUrl, String coreName) {
    StringBuilder sb = new StringBuilder(baseUrl.length() + coreName.length() + 1);
    sb.append(baseUrl);
    if (!baseUrl.endsWith("/")) sb.append("/");
    sb.append(coreName);
    return sb.toString();
  }

  @Override
  public String toString() {
    return name + "(" + getId() + ")" + ':' + Utils.toJSONString(propMap); // small enough, keep it on one line (i.e. no indent)
  }
}
