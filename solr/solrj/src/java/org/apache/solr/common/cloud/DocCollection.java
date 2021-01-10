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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.AUTO_ADD_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.READ_ONLY;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * Models a Collection in zookeeper (but that Java name is obviously taken, hence "DocCollection")
 */
public class DocCollection extends ZkNodeProps implements Iterable<Slice> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static final String DOC_ROUTER = "router";
  public static final String SHARDS = "shards";
  public static final String PER_REPLICA_STATE = "perReplicaState";
  public static final String STATE_FORMAT = "stateFormat";
  /** @deprecated to be removed in Solr 9.0 (see SOLR-14930)
   *
   */
  public static final String RULE = "rule";

  /** @deprecated to be removed in Solr 9.0 (see SOLR-14930)
   *
   */
  public static final String SNITCH = "snitch";

  private final int znodeVersion;

  private final String name;
  private final Map<String, Slice> slices;
  private final Map<String, Slice> activeSlices;
  private final Slice[] activeSlicesArr;
  private final Map<String, List<Replica>> nodeNameReplicas;
  private final Map<String, List<Replica>> nodeNameLeaderReplicas;
  private final DocRouter router;
  private final String znode;

  private final Integer replicationFactor;
  private final Integer numNrtReplicas;
  private final Integer numTlogReplicas;
  private final Integer numPullReplicas;
  private final Integer maxShardsPerNode;
  private final Boolean autoAddReplicas;
  private final String policy;
  private final Boolean readOnly;
  private final Boolean perReplicaState;
  private final Map<String, Replica> replicaMap = new HashMap<>();
  private volatile PerReplicaStates perReplicaStates;


  public DocCollection(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router) {
    this(name, slices, props, router, Integer.MAX_VALUE, ZkStateReader.CLUSTER_STATE);
  }

  /**
   * @param name  The name of the collection
   * @param slices The logical shards of the collection.  This is used directly and a copy is not made.
   * @param props  The properties of the slice.  This is used directly and a copy is not made.
   */
  public DocCollection(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router, int zkVersion, String znode) {
    super(props==null ? props = new HashMap<>() : props);
    // -1 means any version in ZK CAS, so we choose Integer.MAX_VALUE instead to avoid accidental overwrites
    this.znodeVersion = zkVersion == -1 ? Integer.MAX_VALUE : zkVersion;
    this.name = name;

    this.slices = slices;
    this.activeSlices = new HashMap<>();
    this.nodeNameLeaderReplicas = new HashMap<>();
    this.nodeNameReplicas = new HashMap<>();
    this.replicationFactor = (Integer) verifyProp(props, REPLICATION_FACTOR);
    this.numNrtReplicas = (Integer) verifyProp(props, NRT_REPLICAS, 0);
    this.numTlogReplicas = (Integer) verifyProp(props, TLOG_REPLICAS, 0);
    this.numPullReplicas = (Integer) verifyProp(props, PULL_REPLICAS, 0);
    this.maxShardsPerNode = (Integer) verifyProp(props, MAX_SHARDS_PER_NODE);
    this.perReplicaState = (Boolean) verifyProp(props, PER_REPLICA_STATE, Boolean.FALSE);
    Boolean autoAddReplicas = (Boolean) verifyProp(props, AUTO_ADD_REPLICAS);
    this.policy = (String) props.get(Policy.POLICY);
    ClusterState.getReplicaStatesProvider().get().ifPresent(it -> perReplicaStates = it.getStates());
    this.autoAddReplicas = autoAddReplicas == null ? Boolean.FALSE : autoAddReplicas;
    Boolean readOnly = (Boolean) verifyProp(props, READ_ONLY);
    this.readOnly = readOnly == null ? Boolean.FALSE : readOnly;
    
    verifyProp(props, RULE);
    verifyProp(props, SNITCH);
    Iterator<Map.Entry<String, Slice>> iter = slices.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry<String, Slice> slice = iter.next();
      if (slice.getValue().getState() == Slice.State.ACTIVE) {
        this.activeSlices.put(slice.getKey(), slice.getValue());
      }
      for (Replica replica : slice.getValue()) {
        addNodeNameReplica(replica);
        if (perReplicaState) {
          replicaMap.put(replica.getName(), replica);
        }
      }
    }
    this.activeSlicesArr = activeSlices.values().toArray(new Slice[activeSlices.size()]);
    this.router = router;
    this.znode = znode == null? ZkStateReader.CLUSTER_STATE : znode;
    assert name != null && slices != null;
  }

  /**Update our state with a state of a {@link Replica}
   * Used to create a new Collection State when only a replica is updated
   */
  public DocCollection copyWith( PerReplicaStates newPerReplicaStates) {
    log.debug("collection :{} going to be updated :  per-replica state :{} -> {}",
        name,
        getChildNodesVersion(), newPerReplicaStates.cversion);
    if (getChildNodesVersion() == newPerReplicaStates.cversion) return this;
    Set<String> modifiedReplicas = PerReplicaStates.findModifiedReplicas(newPerReplicaStates, this.perReplicaStates);
    if (modifiedReplicas.isEmpty()) return this; //nothing is modified
    Map<String, Slice> modifiedShards = new HashMap<>(getSlicesMap());
    for (String s : modifiedReplicas) {
      Replica replica = getReplica(s);
      if (replica != null) {
        Replica newReplica = replica.copyWith(newPerReplicaStates.get(s));
        Slice shard = modifiedShards.get(replica.slice);
        modifiedShards.put(replica.slice, shard.copyWith(newReplica));
      }
    }
    DocCollection result = new DocCollection(getName(), modifiedShards, propMap, router, znodeVersion, znode);
    result.perReplicaStates = newPerReplicaStates;
    return result;

  }

  private void addNodeNameReplica(Replica replica) {
    List<Replica> replicas = nodeNameReplicas.get(replica.getNodeName());
    if (replicas == null) {
      replicas = new ArrayList<>();
      nodeNameReplicas.put(replica.getNodeName(), replicas);
    }
    replicas.add(replica);

    if (replica.getStr(Slice.LEADER) != null) {
      List<Replica> leaderReplicas = nodeNameLeaderReplicas.get(replica.getNodeName());
      if (leaderReplicas == null) {
        leaderReplicas = new ArrayList<>();
        nodeNameLeaderReplicas.put(replica.getNodeName(), leaderReplicas);
      }
      leaderReplicas.add(replica);
    }
  }
  
  public static Object verifyProp(Map<String, Object> props, String propName) {
    return verifyProp(props, propName, null);
  }

  public static Object verifyProp(Map<String, Object> props, String propName, Object def) {
    Object o = props.get(propName);
    if (o == null) return def;
    switch (propName) {
      case MAX_SHARDS_PER_NODE:
      case REPLICATION_FACTOR:
      case NRT_REPLICAS:
      case PULL_REPLICAS:
      case TLOG_REPLICAS:
        return Integer.parseInt(o.toString());
      case PER_REPLICA_STATE:
      case AUTO_ADD_REPLICAS:
      case READ_ONLY:
        return Boolean.parseBoolean(o.toString());
      case "snitch":
      case "rule":
        return (List) o;
      default:
        return o;
    }

  }

  /**Use this to make an exact copy of DocCollection with a new set of Slices and every other property as is
   * @param slices the new set of Slices
   * @return the resulting DocCollection
   */
  public DocCollection copyWithSlices(Map<String, Slice> slices) {
    DocCollection result = new DocCollection(getName(), slices, propMap, router, znodeVersion, znode);
    result.perReplicaStates = perReplicaStates;
    return result;
  }

  /**
   * Return collection name.
   */
  public String getName() {
    return name;
  }

  public Slice getSlice(String sliceName) {
    return slices.get(sliceName);
  }

  /**
   * @param consumer consume shardName vs. replica
   */
  public void forEachReplica(BiConsumer<String, Replica> consumer) {
    slices.forEach((shard, slice) -> slice.getReplicasMap().forEach((s, replica) -> consumer.accept(shard, replica)));
  }

  /**
   * Gets the list of all slices for this collection.
   */
  public Collection<Slice> getSlices() {
    return slices.values();
  }


  /**
   * Return the list of active slices for this collection.
   */
  public Collection<Slice> getActiveSlices() {
    return activeSlices.values();
  }

  /**
   * Return array of active slices for this collection (performance optimization).
   */
  public Slice[] getActiveSlicesArr() {
    return activeSlicesArr;
  }

  /**
   * Get the map of all slices (sliceName-&gt;Slice) for this collection.
   */
  public Map<String, Slice> getSlicesMap() {
    return slices;
  }

  /**
   * Get the map of active slices (sliceName-&gt;Slice) for this collection.
   */
  public Map<String, Slice> getActiveSlicesMap() {
    return activeSlices;
  }

  /**
   * Get the list of replicas hosted on the given node or <code>null</code> if none.
   */
  public List<Replica> getReplicas(String nodeName) {
    return nodeNameReplicas.get(nodeName);
  }

  /**
   * Get the list of all leaders hosted on the given node or <code>null</code> if none.
   */
  public List<Replica> getLeaderReplicas(String nodeName) {
    return nodeNameLeaderReplicas.get(nodeName);
  }

  public int getZNodeVersion(){
    return znodeVersion;
  }
  public int getChildNodesVersion() {
    return perReplicaStates == null ? -1 : perReplicaStates.cversion;
  }

  public boolean isModified(int dataVersion, int childVersion) {
    if (dataVersion > znodeVersion) return true;
    if (childVersion > getChildNodesVersion()) return true;
    return false;

  }

  public int getStateFormat() {
    return ZkStateReader.CLUSTER_STATE.equals(znode) ? 1 : 2;
  }
  /**
   * @return replication factor for this collection or null if no
   *         replication factor exists.
   */
  public Integer getReplicationFactor() {
    return replicationFactor;
  }
  
  public boolean getAutoAddReplicas() {
    return autoAddReplicas;
  }
  
  public int getMaxShardsPerNode() {
    if (maxShardsPerNode == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, MAX_SHARDS_PER_NODE + " is not in the cluster state.");
    }
    //maxShardsPerNode=0 when policy is used. This variable is not important then
    return maxShardsPerNode == 0 ? Integer.MAX_VALUE : maxShardsPerNode;
  }

  public String getZNode(){
    return znode;
  }


  public DocRouter getRouter() {
    return router;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public String toString() {
    return "DocCollection("+name+"/" + znode + "/" + znodeVersion+" "
        + (perReplicaStates == null ? "": perReplicaStates.toString())+")="
        + toJSONString(this);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    LinkedHashMap<String, Object> all = new LinkedHashMap<>(slices.size() + 1);
    all.putAll(propMap);
    all.put(SHARDS, slices);
    jsonWriter.write(all);
  }

  public Replica getReplica(String coreNodeName) {
    if (perReplicaState) {
      return replicaMap.get(coreNodeName);
    }
    for (Slice slice : slices.values()) {
      Replica replica = slice.getReplica(coreNodeName);
      if (replica != null) return replica;
    }
    return null;
  }

  public Replica getLeader(String sliceName) {
    Slice slice = getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader();
  }

  /**
   * Check that all replicas in a collection are live
   *
   * @see CollectionStatePredicate
   */
  public static boolean isFullyActive(Set<String> liveNodes, DocCollection collectionState,
                                      int expectedShards, int expectedReplicas) {
    Objects.requireNonNull(liveNodes);
    if (collectionState == null)
      return false;
    int activeShards = 0;
    for (Slice slice : collectionState) {
      int activeReplicas = 0;
      for (Replica replica : slice) {
        if (replica.isActive(liveNodes) == false)
          return false;
        activeReplicas++;
      }
      if (activeReplicas != expectedReplicas)
        return false;
      activeShards++;
    }
    return activeShards == expectedShards;
  }

  @Override
  public Iterator<Slice> iterator() {
    return slices.values().iterator();
  }

  public List<Replica> getReplicas() {
    List<Replica> replicas = new ArrayList<>();
    for (Slice slice : this) {
      replicas.addAll(slice.getReplicas());
    }
    return replicas;
  }

  /**
   * @param predicate test against shardName vs. replica
   * @return the first replica that matches the predicate
   */
  public Replica getReplica(BiPredicate<String, Replica> predicate) {
    final Replica[] result = new Replica[1];
    forEachReplica((s, replica) -> {
      if (result[0] != null) return;
      if (predicate.test(s, replica)) {
        result[0] = replica;
      }
    });
    return result[0];
  }

  public List<Replica> getReplicas(EnumSet<Replica.Type> s) {
    List<Replica> replicas = new ArrayList<>();
    for (Slice slice : this) {
      replicas.addAll(slice.getReplicas(s));
    }
    return replicas;
  }

  /**
   * Get the shardId of a core on a specific node
   */
  public String getShardId(String nodeName, String coreName) {
    for (Slice slice : this) {
      for (Replica replica : slice) {
        if (Objects.equals(replica.getNodeName(), nodeName) && Objects.equals(replica.getCoreName(), coreName))
          return slice.getName();
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DocCollection == false)
      return false;
    DocCollection other = (DocCollection) that;
    return super.equals(that) && Objects.equals(this.znode, other.znode) && this.znodeVersion == other.znodeVersion;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, znodeVersion);
  }

  /**
   * @return the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#NRT} this collection was created with
   */
  public Integer getNumNrtReplicas() {
    return numNrtReplicas;
  }

  /**
   * @return the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#TLOG} this collection was created with
   */
  public Integer getNumTlogReplicas() {
    return numTlogReplicas;
  }

  /**
   * @return the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#PULL} this collection was created with
   */
  public Integer getNumPullReplicas() {
    return numPullReplicas;
  }

  /**
   * @return the policy associated with this collection if any
   */
  public String getPolicyName() {
    return policy;
  }

  public boolean isPerReplicaState() {
    return Boolean.TRUE.equals(perReplicaState);
  }

  public PerReplicaStates getPerReplicaStates() {
    return perReplicaStates;
  }


  public int getExpectedReplicaCount(Replica.Type type, int def) {
    Integer result = null;
    if (type == Replica.Type.NRT) result = numNrtReplicas;
    if (type == Replica.Type.PULL) result = numPullReplicas;
    if (type == Replica.Type.TLOG) result = numTlogReplicas;
    return result == null ? def : result;

  }
}
