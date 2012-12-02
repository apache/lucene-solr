package org.apache.solr.common.cloud;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.noggit.JSONWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocRouter.Range;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable state of the cloud. Normally you can get the state by using
 * {@link ZkStateReader#getClusterState()}.
 * @lucene.experimental
 */
public class ClusterState implements JSONWriter.Writable {
  private static Logger log = LoggerFactory.getLogger(ClusterState.class);
  
  private Integer zkClusterStateVersion;
  
  private final Map<String, DocCollection> collectionStates;  // Map<collectionName, Map<sliceName,Slice>>
  private final Set<String> liveNodes;

  private final Map<String,RangeInfo> rangeInfos = new HashMap<String,RangeInfo>();
  
  /**
   * Use this constr when ClusterState is meant for publication.
   * 
   * hashCode and equals will only depend on liveNodes and not clusterStateVersion.
   */
  public ClusterState(Set<String> liveNodes,
      Map<String, DocCollection> collectionStates) {
    this(null, liveNodes, collectionStates);
  }
  
  /**
   * Use this constr when ClusterState is meant for consumption.
   */
  public ClusterState(Integer zkClusterStateVersion, Set<String> liveNodes,
      Map<String, DocCollection> collectionStates) {
    this.zkClusterStateVersion = zkClusterStateVersion;
    this.liveNodes = new HashSet<String>(liveNodes.size());
    this.liveNodes.addAll(liveNodes);
    this.collectionStates = new HashMap<String, DocCollection>(collectionStates.size());
    this.collectionStates.putAll(collectionStates);
    addRangeInfos(collectionStates.keySet());
  }


  /**
   * Get properties of a shard/slice leader for specific collection, or null if one currently doesn't exist.
   */
  public Replica getLeader(String collection, String sliceName) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    Slice slice = coll.getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader();
  }
  
  /**
   * Get replica properties (if the slice is unknown) or null if replica is not found.
   * If the slice is known, do not use this method.
   * coreNodeName is the same as replicaName
   */
  public Replica getReplica(final String collection, final String coreNodeName) {
    return getReplica(collectionStates.get(collection), coreNodeName);
  }

  private Replica getReplica(DocCollection coll, String replicaName) {
    if (coll == null) return null;
    for(Slice slice: coll.getSlices()) {
      Replica replica = slice.getReplica(replicaName);
      if (replica != null) return replica;
    }
    return null;
  }

  private void addRangeInfos(Set<String> collections) {
    for (String collection : collections) {
      addRangeInfo(collection);
    }
  }

  /**
   * Get the Slice for collection.
   */
  public Slice getSlice(String collection, String sliceName) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    return coll.getSlice(sliceName);
  }

  public Map<String, Slice> getSlicesMap(String collection) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    return coll.getSlicesMap();
  }

  public Collection<Slice> getSlices(String collection) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    return coll.getSlices();
  }

  /**
   * Get the named DocCollection object, or thow an exception if it doesn't exist.
   */
  public DocCollection getCollection(String collection) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection:" + collection);
    }
    return coll;
  }

  /**
   * Get collection names.
   */
  public Set<String> getCollections() {
    return Collections.unmodifiableSet(collectionStates.keySet());
  }

  /**
   * @return Map&lt;collectionName, Map&lt;sliceName,Slice&gt;&gt;
   */
  public Map<String, DocCollection> getCollectionStates() {
    return Collections.unmodifiableMap(collectionStates);
  }

  /**
   * Get names of the currently live nodes.
   */
  public Set<String> getLiveNodes() {
    return Collections.unmodifiableSet(liveNodes);
  }

  /**
   * Get the slice/shardId for a core.
   * @param coreNodeName in the form of nodeName_coreName (the name of the replica)
   */
  public String getShardId(String coreNodeName) {
     // System.out.println("###### getShardId("+coreNodeName+") in " + collectionStates);
    for (DocCollection coll : collectionStates.values()) {
      for (Slice slice : coll.getSlices()) {
        if (slice.getReplicasMap().containsKey(coreNodeName)) return slice.getName();
      }
    }
    return null;
  }

  /**
   * Check if node is alive. 
   */
  public boolean liveNodesContain(String name) {
    return liveNodes.contains(name);
  }
  
  public RangeInfo getRanges(String collection) {
    // TODO: store this in zk
    RangeInfo rangeInfo = rangeInfos.get(collection);

    return rangeInfo;
  }

  private RangeInfo addRangeInfo(String collection) {
    List<Range> ranges;
    RangeInfo rangeInfo;
    rangeInfo = new RangeInfo();

    DocCollection coll = getCollection(collection);
    
    Set<String> shards = coll.getSlicesMap().keySet();
    ArrayList<String> shardList = new ArrayList<String>(shards.size());
    shardList.addAll(shards);
    Collections.sort(shardList);
    
    ranges = DocRouter.DEFAULT.partitionRange(shards.size(), Integer.MIN_VALUE, Integer.MAX_VALUE);
    
    rangeInfo.ranges = ranges;
    rangeInfo.shardList = shardList;
    rangeInfos.put(collection, rangeInfo);
    return rangeInfo;
  }

  /**
   * Get shard id for hash. This is used when determining which Slice the
   * document is to be submitted to.
   */
  public String getShard(int hash, String collection) {
    RangeInfo rangInfo = getRanges(collection);
    
    int cnt = 0;
    for (Range range : rangInfo.ranges) {
      if (range.includes(hash)) {
        return rangInfo.shardList.get(cnt);
      }
      cnt++;
    }
    
    throw new IllegalStateException("The DocRouter failed");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("live nodes:" + liveNodes);
    sb.append(" collections:" + collectionStates);
    return sb.toString();
  }

  /**
   * Create ClusterState by reading the current state from zookeeper. 
   */
  public static ClusterState load(SolrZkClient zkClient, Set<String> liveNodes) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    byte[] state = zkClient.getData(ZkStateReader.CLUSTER_STATE,
        null, stat, true);
    return load(stat.getVersion(), state, liveNodes);
  }
  
 
  /**
   * Create ClusterState from json string that is typically stored in zookeeper.
   * 
   * Use {@link ClusterState#load(SolrZkClient, Set)} instead, unless you want to
   * do something more when getting the data - such as get the stat, set watch, etc.
   * 
   * @param version zk version of the clusterstate.json file (bytes)
   * @param bytes clusterstate.json as a byte array
   * @param liveNodes list of live nodes
   * @return the ClusterState
   */
  public static ClusterState load(Integer version, byte[] bytes, Set<String> liveNodes) {
    // System.out.println("######## ClusterState.load:" + (bytes==null ? null : new String(bytes)));
    if (bytes == null || bytes.length == 0) {
      return new ClusterState(version, liveNodes, Collections.<String, DocCollection>emptyMap());
    }
    Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader.fromJSON(bytes);
    Map<String,DocCollection> collections = new LinkedHashMap<String,DocCollection>(stateMap.size());
    for (Entry<String, Object> entry : stateMap.entrySet()) {
      String collectionName = entry.getKey();
      DocCollection coll = collectionFromObjects(collectionName, (Map<String,Object>)entry.getValue());
      collections.put(collectionName, coll);
    }

    // System.out.println("######## ClusterState.load result:" + collections);
    return new ClusterState(version, liveNodes, collections);
  }

  private static DocCollection collectionFromObjects(String name, Map<String,Object> objs) {
    Map<String,Object> props = (Map<String,Object>)objs.get(DocCollection.PROPERTIES);
    if (props == null) props = Collections.emptyMap();
    DocRouter router = DocRouter.getDocRouter(props.get(DocCollection.DOC_ROUTER));
    Map<String,Slice> slices = makeSlices(objs);
    return new DocCollection(name, slices, props, router);
  }

  private static Map<String,Slice> makeSlices(Map<String,Object> genericSlices) {
    if (genericSlices == null) return Collections.emptyMap();
    Map<String,Slice> result = new LinkedHashMap<String, Slice>(genericSlices.size());
    for (Map.Entry<String,Object> entry : genericSlices.entrySet()) {
      String name = entry.getKey();
      if (DocCollection.PROPERTIES.equals(name)) continue;  // skip special properties entry
      Object val = entry.getValue();
      Slice s;
      if (val instanceof Slice) {
        s = (Slice)val;
      } else {
        s = new Slice(name, null, (Map<String,Object>)val);
      }
      result.put(name, s);
    }
    return result;
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(collectionStates);
  }
  
  private class RangeInfo {
    private List<Range> ranges;
    private ArrayList<String> shardList;
  }

  /**
   * The version of clusterstate.json in ZooKeeper.
   * 
   * @return null if ClusterState was created for publication, not consumption
   */
  public Integer getZkClusterStateVersion() {
    return zkClusterStateVersion;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((zkClusterStateVersion == null) ? 0 : zkClusterStateVersion.hashCode());
    result = prime * result + ((liveNodes == null) ? 0 : liveNodes.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ClusterState other = (ClusterState) obj;
    if (zkClusterStateVersion == null) {
      if (other.zkClusterStateVersion != null) return false;
    } else if (!zkClusterStateVersion.equals(other.zkClusterStateVersion)) return false;
    if (liveNodes == null) {
      if (other.liveNodes != null) return false;
    } else if (!liveNodes.equals(other.liveNodes)) return false;
    return true;
  }




}
