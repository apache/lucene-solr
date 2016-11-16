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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.noggit.JSONUtil;
import org.noggit.JSONWriter;

import static org.apache.solr.common.cloud.ZkStateReader.AUTO_ADD_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

/**
 * Models a Collection in zookeeper (but that Java name is obviously taken, hence "DocCollection")
 */
public class DocCollection extends ZkNodeProps implements Iterable<Slice> {

  public static final String DOC_ROUTER = "router";
  public static final String SHARDS = "shards";
  public static final String STATE_FORMAT = "stateFormat";
  public static final String RULE = "rule";
  public static final String SNITCH = "snitch";

  private final int znodeVersion;

  private final String name;
  private final Map<String, Slice> slices;
  private final Map<String, Slice> activeSlices;
  private final Map<String, List<Replica>> nodeNameReplicas;
  private final Map<String, List<Replica>> nodeNameLeaderReplicas;
  private final DocRouter router;
  private final String znode;

  private final Integer replicationFactor;
  private final Integer maxShardsPerNode;
  private final Boolean autoAddReplicas;


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
    this.maxShardsPerNode = (Integer) verifyProp(props, MAX_SHARDS_PER_NODE);
    Boolean autoAddReplicas = (Boolean) verifyProp(props, AUTO_ADD_REPLICAS);
    this.autoAddReplicas = autoAddReplicas == null ? false : autoAddReplicas;
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
      }
    }
    this.router = router;
    this.znode = znode == null? ZkStateReader.CLUSTER_STATE : znode;
    assert name != null && slices != null;
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
    Object o = props.get(propName);
    if (o == null) return null;
    switch (propName) {
      case MAX_SHARDS_PER_NODE:
      case REPLICATION_FACTOR:
        return Integer.parseInt(o.toString());
      case AUTO_ADD_REPLICAS:
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
  public DocCollection copyWithSlices(Map<String, Slice> slices){
    return new DocCollection(getName(), slices, propMap, router, znodeVersion,znode);
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
    return maxShardsPerNode;
  }

  public String getZNode(){
    return znode;
  }


  public DocRouter getRouter() {
    return router;
  }

  @Override
  public String toString() {
    return "DocCollection("+name+"/" + znode + "/" + znodeVersion + ")=" + JSONUtil.toJSON(this);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    LinkedHashMap<String, Object> all = new LinkedHashMap<>(slices.size() + 1);
    all.putAll(propMap);
    all.put(SHARDS, slices);
    jsonWriter.write(all);
  }

  public Replica getReplica(String coreNodeName) {
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

}
