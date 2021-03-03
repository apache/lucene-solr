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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.noggit.JSONWriter;

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

  public static final String DOC_ROUTER = "router";
  public static final String SHARDS = "shards";

  private final int znodeVersion;

  private final String name;
  private final Map<String, Slice> slices;
  private final DocRouter router;

  private final Integer replicationFactor;
  private final Integer numNrtReplicas;
  private final Integer numTlogReplicas;
  private final Integer numPullReplicas;
  private final Integer maxShardsPerNode;
  private final Boolean readOnly;
  private final boolean withStateUpdates;
  private final Long id;

  public DocCollection(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router) {
    this(name, slices, props, router, -1, false);
  }

  /**
   * @param name  The name of the collection
   * @param slices The logical shards of the collection.  This is used directly and a copy is not made.
   * @param props  The properties of the slice.  This is used directly and a copy is not made.
   * @param zkVersion The version of the Collection node in Zookeeper (used for conditional updates).
   */
  public DocCollection(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router, int zkVersion, boolean withStateUpdates) {
    super(props==null ? props = new HashMap<>() : props);
    this.znodeVersion = zkVersion;
    this.name = name;
    this.withStateUpdates = withStateUpdates;
    this.slices = slices;
    this.replicationFactor = (Integer) verifyProp(props, REPLICATION_FACTOR);
    this.numNrtReplicas = (Integer) verifyProp(props, NRT_REPLICAS, 0);
    this.numTlogReplicas = (Integer) verifyProp(props, TLOG_REPLICAS, 0);
    this.numPullReplicas = (Integer) verifyProp(props, PULL_REPLICAS, 0);
    this.maxShardsPerNode = (Integer) verifyProp(props, MAX_SHARDS_PER_NODE);
    Boolean readOnly = (Boolean) verifyProp(props, READ_ONLY);
    this.readOnly = readOnly == null ? Boolean.FALSE : readOnly;

    this.id = (Long) props.get("id");

    Objects.requireNonNull(this.id, "'id' must not be null");

    this.router = router;
    assert name != null && slices != null;
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
      case READ_ONLY:
        return Boolean.parseBoolean(o.toString());
      case "snitch":
      default:
        return o;
    }

  }

  /**Use this to make an exact copy of DocCollection with a new set of Slices and every other property as is
   * @param slices the new set of Slices
   * @return the resulting DocCollection
   */
  public DocCollection copyWithSlices(Map<String, Slice> slices){
    return new DocCollection(getName(), slices, propMap, router, znodeVersion, withStateUpdates);
  }

  public DocCollection copy(){
    return new DocCollection(getName(), slices, propMap, router, znodeVersion, withStateUpdates);
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
    List<Slice> activeSlices = new ArrayList<>(slices.size());
    slices.values().forEach(slice -> {
      if (slice.getState() == Slice.State.ACTIVE) {
        activeSlices.add(slice);
      }
    });
    Collections.shuffle(activeSlices);
    return activeSlices;
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
    Map<String, Slice> activeSlices = new HashMap<>(slices.size());
    slices.entrySet().forEach(sliceEntry -> {
      if (sliceEntry.getValue().getState() == Slice.State.ACTIVE) {
        activeSlices.put(sliceEntry.getKey(), sliceEntry.getValue());
      }
    });
    return activeSlices;
  }

  /**
   * Get the list of replicas hosted on the given node or <code>null</code> if none.
   */
  public List<Replica> getReplicas(String nodeName) {
    Iterator<Map.Entry<String, Slice>> iter = slices.entrySet().iterator();
    List<Replica> replicas = new ArrayList<>(slices.size());
    while (iter.hasNext()) {
      Map.Entry<String, Slice> slice = iter.next();
      for (Replica replica : slice.getValue()) {
        if (replica.getNodeName().equals(nodeName)) {
          replicas.add(replica);
        }
      }
    }
    return replicas;
  }

  /**
   * Get the list of all leaders hosted on the given node or <code>null</code> if none.
   */
  public List<Replica> getLeaderReplicas(String nodeName) {
    Iterator<Map.Entry<String, Slice>> iter = slices.entrySet().iterator();
    List<Replica> leaders = new ArrayList<>(slices.size());
    while (iter.hasNext()) {
      Map.Entry<String, Slice> slice = iter.next();
      Replica leader = slice.getValue().getLeader();
      if (leader != null && leader.getNodeName().equals(nodeName)) {
        leaders.add(leader);
      }

    }
    return leaders;
  }

  public int getZNodeVersion(){
    return znodeVersion;
  }

  public int getStateFormat() {
    return 2;
  }
  /**
   * @return replication factor for this collection or null if no
   *         replication factor exists.
   */
  public Integer getReplicationFactor() {
    return replicationFactor;
  }

  public int getMaxShardsPerNode() {
    if (maxShardsPerNode == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, MAX_SHARDS_PER_NODE + " is not in the cluster state.");
    }
    //maxShardsPerNode=0 when policy is used. This variable is not important then
    return maxShardsPerNode == 0 ? Integer.MAX_VALUE : maxShardsPerNode;
  }


  public DocRouter getRouter() {
    return router;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public String toString() {
    return "DocCollection("+name+":" + ":v=" + znodeVersion + " u=" + hasStateUpdates() + ")=" + toJSONString(this);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    LinkedHashMap<String, Object> all = new LinkedHashMap<>(slices.size() + 1);
    all.putAll(propMap);
    all.put(SHARDS, slices);
    jsonWriter.write(all);
  }

  public Replica getReplica(String coreName) {
    for (Slice slice : slices.values()) {
      Replica replica = slice.getReplica(coreName);
      if (replica != null) return replica;
    }
    return null;
  }

  public Replica getReplicaById(String id) {
    for (Slice slice : slices.values()) {
      Replica replica = slice.getReplicaById(id);
      if (replica != null) return replica;
    }
    return null;
  }

  public Map<String,Replica> getReplicaByIds() {
    Map<String,Replica> ids = new HashMap<>();
    for (Slice slice : slices.values()) {
      ids.putAll(slice.getReplicaByIds());
    }
    return ids;
  }

  public Slice getSlice(Replica replica) {
    for (Slice slice : slices.values()) {
      Replica r = slice.getReplica(replica.getName());
      if (r != null) return slice;
    }
    return null;
  }

  public Replica getLeader(String sliceName) {
    Slice slice = getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader();
  }

  public long getId() {
    return id;
  }

  public long getHighestReplicaId() {
    long[] highest = new long[1];
    List<Replica> replicas = getReplicas();
    replicas.forEach(replica -> highest[0] = Math.max(highest[0], replica.id));
    return highest[0] + 1;
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
        if (Objects.equals(replica.getNodeName(), nodeName) && Objects.equals(replica.getName(), coreName))
          return slice.getName();
      }
    }
    return null;
  }

  public String getShardId(String coreNodeName) {
    assert coreNodeName != null;

    for (Slice slice : this) {
      for (Replica replica : slice) {
        if (replica.getName().equals(coreNodeName)) {
          return slice.getName();
        }
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DocCollection == false)
      return false;
    DocCollection other = (DocCollection) that;
    return super.equals(that) && Objects.equals(this.name, other.name) && this.znodeVersion == other.znodeVersion;
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

  public int getExpectedReplicaCount(Replica.Type type, int def) {
    Integer result = null;
    if (type == Replica.Type.NRT) result = numNrtReplicas;
    if (type == Replica.Type.PULL) result = numPullReplicas;
    if (type == Replica.Type.TLOG) result = numTlogReplicas;
    return result == null ? def : result;

  }

  public boolean hasStateUpdates() {
    return withStateUpdates;
  }
}
