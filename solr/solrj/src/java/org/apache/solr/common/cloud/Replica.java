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
     * {@link ClusterState#liveNodesContain(String)}).
     * </p>
     */
    ACTIVE,
    
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
      return name == null ? Type.NRT : Type.valueOf(name);
    }
  }

  private final String name;
  private final String nodeName;
  private final State state;
  private final Type type;
  public final String slice, collection;

  public Replica(String name, Map<String,Object> propMap, String collection, String slice) {
    super(propMap);
    this.collection = collection;
    this.slice = slice;
    this.name = name;
    this.nodeName = (String) propMap.get(ZkStateReader.NODE_NAME_PROP);
    if (propMap.get(ZkStateReader.STATE_PROP) != null) {
      this.state = State.getState((String) propMap.get(ZkStateReader.STATE_PROP));
    } else {
      this.state = State.ACTIVE;                         //Default to ACTIVE
      propMap.put(ZkStateReader.STATE_PROP, state.toString());
    }
    type = Type.get((String) propMap.get(ZkStateReader.REPLICA_TYPE));
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
    return ZkCoreNodeProps.getCoreUrl(getStr(ZkStateReader.BASE_URL_PROP), getStr(ZkStateReader.CORE_NAME_PROP));
  }
  public String getBaseUrl(){
    return getStr(ZkStateReader.BASE_URL_PROP);
  }

  /** SolrCore name. */
  public String getCoreName() {
    return getStr(ZkStateReader.CORE_NAME_PROP);
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
    return this.nodeName != null && liveNodes.contains(this.nodeName) && this.state == State.ACTIVE;
  }
  
  public Type getType() {
    return this.type;
  }

  public String getProperty(String propertyName) {
    final String propertyKey;
    if (!propertyName.startsWith(ZkStateReader.PROPERTY_PROP_PREFIX)) {
      propertyKey = ZkStateReader.PROPERTY_PROP_PREFIX+propertyName;
    } else {
      propertyKey = propertyName;
    }
    final String propertyValue = getStr(propertyKey);
    return propertyValue;
  }

  @Override
  public String toString() {
    return name + ':' + Utils.toJSONString(propMap); // small enough, keep it on one line (i.e. no indent)
  }
}
