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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.ConditionalMapWriter.NON_NULL_VAL;
import static org.apache.solr.common.ConditionalMapWriter.dedupeKeyPredicate;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

public class Replica extends ZkNodeProps implements MapWriter {
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

    public static Type get(String name) {
      return name == null ? Type.NRT : Type.valueOf(name.toUpperCase(Locale.ROOT));
    }
  }

  // immutable
  public final String name; // coreNode name
  public final String node;
  public final String core;
  public final Type type;
  public final String shard, collection;
  private PerReplicaStates.State replicaState;

  // mutable
  private State state;

  public Replica(String name, Map<String,Object> map, String collection, String shard) {
    super(new HashMap<>());
    propMap.putAll(map);
    this.collection = collection;
    this.shard = shard;
    this.name = name;
    this.node = (String) propMap.get(ZkStateReader.NODE_NAME_PROP);
    this.core = (String) propMap.get(ZkStateReader.CORE_NAME_PROP);
    this.type = Type.get((String) propMap.get(ZkStateReader.REPLICA_TYPE));
    readPrs();
    // default to ACTIVE
    this.state = State.getState(String.valueOf(propMap.getOrDefault(ZkStateReader.STATE_PROP, State.ACTIVE.toString())));
    validate();

    propMap.put(BASE_URL_PROP, UrlScheme.INSTANCE.getBaseUrlForNodeName(this.node));
  }

  // clone constructor
  public Replica(String name, String node, String collection, String shard, String core,
                  State state, Type type, Map<String, Object> props) {
    super(new HashMap<>());
    this.name = name;
    this.node = node;
    this.state = state;
    this.type = type;
    this.collection = collection;
    this.shard = shard;
    this.core = core;
    if (props != null) {
      this.propMap.putAll(props);
    }
    readPrs();
    validate();
    propMap.put(BASE_URL_PROP, UrlScheme.INSTANCE.getBaseUrlForNodeName(this.node));
  }

  /**
   * This constructor uses a map with one key (coreNode name) and a value that
   * is a map containing all replica properties.
   * @param nestedMap nested map containing replica properties
   */
  @SuppressWarnings("unchecked")
  public Replica(Map<String, Object> nestedMap) {
    this.name = nestedMap.keySet().iterator().next();
    Map<String, Object> details = (Map<String, Object>) nestedMap.get(name);
    Objects.requireNonNull(details);
    details = Utils.getDeepCopy(details, 4);
    this.collection = String.valueOf(details.get("collection"));
    this.shard = String.valueOf(details.get("shard"));
    this.core = String.valueOf(details.get("core"));
    this.node = String.valueOf(details.get("node_name"));

    this.propMap.putAll(details);
    readPrs();
    type = Replica.Type.valueOf(String.valueOf(propMap.getOrDefault(ZkStateReader.REPLICA_TYPE, "NRT")));
    if(state == null) state = State.getState(String.valueOf(propMap.getOrDefault(ZkStateReader.STATE_PROP, "active")));
    validate();
    propMap.put(BASE_URL_PROP, UrlScheme.INSTANCE.getBaseUrlForNodeName(this.node));
  }

  private void readPrs() {
    ClusterState.getReplicaStatesProvider().get().ifPresent(it -> {
      log.debug("A replica  {} state fetched from per-replica state", name);
      replicaState = it.getStates().get(name);
      if(replicaState!= null) {
        propMap.put(ZkStateReader.STATE_PROP, replicaState.state.toString().toLowerCase(Locale.ROOT));
        if (replicaState.isLeader) propMap.put(Slice.LEADER, "true");
      }
    }) ;
  }

  private final void validate() {
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.core, "'core' must not be null");
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.shard, "'shard' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    Objects.requireNonNull(this.state, "'state' must not be null");
    Objects.requireNonNull(this.node, "'node' must not be null");
    // make sure all declared props are in the propMap
    propMap.put(ZkStateReader.COLLECTION_PROP, collection);
    propMap.put(ZkStateReader.SHARD_ID_PROP, shard);
    propMap.put(ZkStateReader.CORE_NODE_NAME_PROP, name);
    propMap.put(ZkStateReader.NODE_NAME_PROP, node);
    propMap.put(ZkStateReader.CORE_NAME_PROP, core);
    propMap.put(ZkStateReader.REPLICA_TYPE, type.toString());
    propMap.put(ZkStateReader.STATE_PROP, state.toString());
  }

  public String getCollection() {
    return collection;
  }

  public String getShard() {
    return shard;
  }

  @Override
  public Map<String, Object> getProperties() {
    return super.getProperties();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    Replica other = (Replica) o;

    return name.equals(other.name);
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

  public String getBaseUrl() {
    return getStr(BASE_URL_PROP);
  }

  /** SolrCore name. */
  public String getCoreName() {
    return core;
  }

  /** The name of the node this replica resides on */
  public String getNodeName() {
    return node;
  }
  
  /** Returns the {@link State} of this replica. */
  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
    propMap.put(ZkStateReader.STATE_PROP, this.state.toString());
  }

  public boolean isActive(Set<String> liveNodes) {
    return this.node != null && liveNodes.contains(this.node) && this.state == State.ACTIVE;
  }
  
  public Type getType() {
    return this.type;
  }

  public boolean isLeader() {
    return getBool(ZkStateReader.LEADER_PROP, false);
  }

  public Object get(String key, Object defValue) {
    Object o = get(key);
    if (o != null) {
      return o;
    } else {
      return defValue;
    }
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
    Replica r = new Replica(name, props, collection, shard);
    r.replicaState = state;
    return r;
  }

  public PerReplicaStates.State getReplicaState() {
    return replicaState;
  }

  public Object clone() {
    return new Replica(name, node, collection, shard, core, state, type, propMap);
  }

  @Override
  public void writeMap(MapWriter.EntryWriter ew) throws IOException {
    ew.put(name, _allPropsWriter());
  }

  private static final Map<String, State> STATES = new HashMap<>();
  static {
    STATES.put(Replica.State.ACTIVE.shortName, Replica.State.ACTIVE);
    STATES.put(Replica.State.DOWN.shortName, Replica.State.DOWN);
    STATES.put(Replica.State.RECOVERING.shortName, Replica.State.RECOVERING);
    STATES.put(Replica.State.RECOVERY_FAILED.shortName, Replica.State.RECOVERY_FAILED);
  }
  public static State getState(String  shortName) {
    return STATES.get(shortName);
  }


  private MapWriter _allPropsWriter() {
    BiPredicate<CharSequence, Object> p = dedupeKeyPredicate(new HashSet<>())
        .and(NON_NULL_VAL);
    return writer -> {
      // XXX this is why this class should be immutable - it's a mess !!!

      // propMap takes precedence because it's mutable and we can't control its
      // contents, so a third party may override some declared fields
      for (Map.Entry<String, Object> e : propMap.entrySet()) {
        final String key = e.getKey();
        // don't store the base_url as we can compute it from the node_name
        if (!BASE_URL_PROP.equals(key)) {
          writer.put(e.getKey(), e.getValue(), p);
        }
      }

      writer.put(ZkStateReader.CORE_NAME_PROP, core, p)
          .put(ZkStateReader.SHARD_ID_PROP, shard, p)
          .put(ZkStateReader.COLLECTION_PROP, collection, p)
          .put(ZkStateReader.NODE_NAME_PROP, node, p)
          .put(ZkStateReader.REPLICA_TYPE, type.toString(), p)
          .put(ZkStateReader.STATE_PROP, state.toString(), p);
    };
  }
  @Override
  public void write(JSONWriter jsonWriter) {
    Map<String, Object> map = new LinkedHashMap<>();
    // this serializes also our declared properties
    _allPropsWriter().toMap(map);
    jsonWriter.write(map);
  }

  @Override
  public String toString() {
    return name + ':' + Utils.toJSONString(propMap); // small enough, keep it on one line (i.e. no indent)
  }
}
