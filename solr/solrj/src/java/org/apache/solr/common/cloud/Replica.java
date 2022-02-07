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

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

public class Replica extends ZkNodeProps {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
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
     * {@link ClusterState#liveNodesContain(String)}).
     * </p>
     */
    ACTIVE("A"),
    
    /**
     * The first state before {@link State#RECOVERING}. A node in this state
     * should be actively trying to move to {@link State#RECOVERING}.
     * <p>
     * <b>NOTE</b>: a replica's state may appear DOWN in ZK also when the node
     * it's hosted on gracefully shuts down. This is a best effort though, and
     * should not be relied on.
     * </p>
     */
    DOWN("D"),
    
    /**
     * The node is recovering from the leader. This might involve peer-sync,
     * full replication or finding out things are already in sync.
     */
    RECOVERING("R"),
    
    /**
     * Recovery attempts have not worked, something is not right.
     * <p>
     * <b>NOTE</b>: This state doesn't matter if the node is not part of
     * {@code /live_nodes} in ZK; in that case the node is not part of the
     * cluster and it's state should be discarded.
     * </p>
     */
    RECOVERY_FAILED("F");

    /**short name for a state. Used to encode this in the state node see {@link PerReplicaStates.State}
     */
    public final String shortName;

    State(String c) {
      this.shortName = c;
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }
    
    /** Converts the state string to a State instance. */
    public static State getState(String stateStr) {
      return stateStr == null ? null : Replica.State.valueOf(stateStr.toUpperCase(Locale.ROOT));
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

  private final String name;
  private final String nodeName;
  private final String core;
  private final State state;
  private final Type type;
  public final String slice, collection;
  private PerReplicaStates.State replicaState;

  public Replica(String name, Map<String,Object> propMap, String collection, String slice) {
    super(propMap);
    this.collection = collection;
    this.slice = slice;
    this.name = name;
    this.nodeName = (String) propMap.get(ZkStateReader.NODE_NAME_PROP);
    this.core = (String) propMap.get(ZkStateReader.CORE_NAME_PROP);
    type = Type.get((String) propMap.get(ZkStateReader.REPLICA_TYPE));
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.slice, "'slice' must not be null");
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.nodeName, "'node_name' must not be null");
    Objects.requireNonNull(this.core, "'core' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    ClusterState.getReplicaStatesProvider().get().ifPresent(it -> {
      if(log.isDebugEnabled()) {
        log.debug("replica: {}, state fetched from per-replica state", name);
      }
      replicaState = it.getStates().get(name);
      if (replicaState!= null) {
        propMap.put(ZkStateReader.STATE_PROP, replicaState.state.toString().toLowerCase(Locale.ROOT));
        if (replicaState.isLeader) propMap.put(Slice.LEADER, "true");
      }
    }) ;
    if (replicaState == null) {
      if (propMap.get(ZkStateReader.STATE_PROP) != null) {
        this.state = Replica.State.getState((String) propMap.get(ZkStateReader.STATE_PROP));
      } else {
        this.state = Replica.State.ACTIVE;                         //Default to ACTIVE
        propMap.put(ZkStateReader.STATE_PROP, state.toString());
      }
    } else {
      this.state = replicaState.state;
    }
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

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  /** Also known as coreNodeName. */
  public String getName() {
    return name;
  }

  public String getCoreUrl() {
    return ZkCoreNodeProps.getCoreUrl(getBaseUrl(), core);
  }

  public String getBaseUrl(){
    return getStr(BASE_URL_PROP);
  }

  /** SolrCore name. */
  public String getCoreName() {
    return core;
  }

  /** The name of the node this replica resides on */
  public String getNodeName() {
    return nodeName;
  }
  
  /** Returns the {@link State} of this replica. */
  public State getState() {
    return state;
  }

  public boolean isActive(Set<String> liveNodes) {
    return this.nodeName != null && liveNodes.contains(this.nodeName) && this.state == Replica.State.ACTIVE;
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

  public Replica copyWith(PerReplicaStates.State state) {
    log.debug("A replica is updated with new state : {}", state);
    Map<String, Object> props = new LinkedHashMap<>(propMap);
    if (state == null) {
      props.put(ZkStateReader.STATE_PROP, State.DOWN.toString());
      props.remove(Slice.LEADER);
    } else {
      props.put(ZkStateReader.STATE_PROP, state.state.toString());
      if (state.isLeader) props.put(Slice.LEADER, "true");
    }
    Replica r = new Replica(name, props, collection, slice);
    r.replicaState = state;
    return r;
  }

  public PerReplicaStates.State getReplicaState() {
    return replicaState;
  }

  private static final Map<String, State> STATES = new HashMap<>();
  static {
    STATES.put(Replica.State.ACTIVE.shortName, Replica.State.ACTIVE);
    STATES.put(Replica.State.DOWN.shortName, Replica.State.DOWN);
    STATES.put(Replica.State.RECOVERING.shortName, Replica.State.RECOVERING);
    STATES.put(Replica.State.RECOVERY_FAILED.shortName, Replica.State.RECOVERY_FAILED);
  }
  public static State getState(String c) {
  return STATES.get(c);
  }

  public boolean isLeader() {
    if (replicaState != null) return replicaState.isLeader;
     return getStr(Slice.LEADER) != null;
  }
  @Override
  public String toString() {
    return name + ':' + Utils.toJSONString(propMap); // small enough, keep it on one line (i.e. no indent)
  }
}
