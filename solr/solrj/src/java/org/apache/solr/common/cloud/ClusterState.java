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
import org.apache.solr.common.cloud.HashPartitioner.Range;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable state of the cloud. Normally you can get the state by using
 * {@link ZkStateReader#getClusterState()}.
 */
public class ClusterState implements JSONWriter.Writable {
  private static Logger log = LoggerFactory.getLogger(ClusterState.class);
  
  private Integer zkClusterStateVersion;
  
  private final Map<String, Map<String,Slice>> collectionStates;  // Map<collectionName, Map<sliceName,Slice>>
  private final Set<String> liveNodes;
  
  private final HashPartitioner hp = new HashPartitioner();
  
  private final Map<String,RangeInfo> rangeInfos = new HashMap<String,RangeInfo>();
  private final Map<String,Map<String,ZkNodeProps>> leaders = new HashMap<String,Map<String,ZkNodeProps>>();


  
  /**
   * Use this constr when ClusterState is meant for publication.
   * 
   * hashCode and equals will only depend on liveNodes and not clusterStateVersion.
   */
  public ClusterState(Set<String> liveNodes,
      Map<String, Map<String,Slice>> collectionStates) {
    this(null, liveNodes, collectionStates);
  }
  
  /**
   * Use this constr when ClusterState is meant for consumption.
   */
  public ClusterState(Integer zkClusterStateVersion, Set<String> liveNodes,
      Map<String, Map<String,Slice>> collectionStates) {
    this.liveNodes = new HashSet<String>(liveNodes.size());
    this.liveNodes.addAll(liveNodes);
    this.collectionStates = new HashMap<String, Map<String,Slice>>(collectionStates.size());
    this.collectionStates.putAll(collectionStates);
    addRangeInfos(collectionStates.keySet());
    getShardLeaders();
  }

  private void getShardLeaders() {
    Set<Entry<String,Map<String,Slice>>> collections = collectionStates.entrySet();
    for (Entry<String,Map<String,Slice>> collection : collections) {
      Map<String,Slice> state = collection.getValue();
      Set<Entry<String,Slice>> slices = state.entrySet();
      for (Entry<String,Slice> sliceEntry : slices) {
        Slice slice = sliceEntry.getValue();
        Map<String,Replica> shards = slice.getReplicasMap();
        Set<Entry<String,Replica>> shardsEntries = shards.entrySet();
        for (Entry<String,Replica> shardEntry : shardsEntries) {
          ZkNodeProps props = shardEntry.getValue();
          if (props.containsKey(ZkStateReader.LEADER_PROP)) {
            Map<String,ZkNodeProps> leadersForCollection = leaders.get(collection.getKey());
            if (leadersForCollection == null) {
              leadersForCollection = new HashMap<String,ZkNodeProps>();
              leaders.put(collection.getKey(), leadersForCollection);
            }
            leadersForCollection.put(sliceEntry.getKey(), props);
            break; // we found the leader for this shard
          }
        }
      }
    }
  }

  /**
   * Get properties of a shard leader for specific collection.
   */
  public ZkNodeProps getLeader(String collection, String shard) {
    Map<String,ZkNodeProps> collectionLeaders = leaders.get(collection);
    if (collectionLeaders == null) return null;
    return collectionLeaders.get(shard);
  }
  
  /**
   * Get shard properties or null if shard is not found.
   */
  public Replica getShardProps(final String collection, final String coreNodeName) {
    Map<String, Slice> slices = getSlices(collection);
    if (slices == null) return null;
    for(Slice slice: slices.values()) {
      if(slice.getReplicasMap().get(coreNodeName)!=null) {
        return slice.getReplicasMap().get(coreNodeName);
      }
    }
    return null;
  }

  private void addRangeInfos(Set<String> collections) {
    for (String collection : collections) {
      addRangeInfo(collection);
    }
  }

  /**
   * Get the index Slice for collection.
   */
  public Slice getSlice(String collection, String slice) {
    if (collectionStates.containsKey(collection)
        && collectionStates.get(collection).containsKey(slice))
      return collectionStates.get(collection).get(slice);
    return null;
  }

  /**
   * Get all slices for collection.
   */
  public Map<String, Slice> getSlices(String collection) {
    if(!collectionStates.containsKey(collection))
      return null;
    return Collections.unmodifiableMap(collectionStates.get(collection));
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
  public Map<String, Map<String, Slice>> getCollectionStates() {
    return Collections.unmodifiableMap(collectionStates);
  }

  /**
   * Get names of the currently live nodes.
   */
  public Set<String> getLiveNodes() {
    return Collections.unmodifiableSet(liveNodes);
  }

  /**
   * Get shardId for core.
   * @param coreNodeName in the form of nodeName_coreName
   */
  public String getShardId(String coreNodeName) {
    for (Entry<String, Map<String, Slice>> states: collectionStates.entrySet()){
      for(Entry<String, Slice> slices: states.getValue().entrySet()) {
        for(Entry<String, Replica> shards: slices.getValue().getReplicasMap().entrySet()){
          if(coreNodeName.equals(shards.getKey())) {
            return slices.getKey();
          }
        }
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

    Map<String,Slice> slices = getSlices(collection);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find collection "
          + collection + " in " + this);
    }
    
    Set<String> shards = slices.keySet();
    ArrayList<String> shardList = new ArrayList<String>(shards.size());
    shardList.addAll(shards);
    Collections.sort(shardList);
    
    ranges = hp.partitionRange(shards.size(), Integer.MIN_VALUE, Integer.MAX_VALUE);
    
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
    
    throw new IllegalStateException("The HashPartitioner failed");
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
    if (bytes == null || bytes.length == 0) {
      return new ClusterState(version, liveNodes, Collections.<String, Map<String,Slice>>emptyMap());
    }
    // System.out.println("########## Loading ClusterState:" + new String(bytes));
    LinkedHashMap<String, Object> stateMap = (LinkedHashMap<String, Object>) ZkStateReader.fromJSON(bytes);
    HashMap<String,Map<String, Slice>> state = new HashMap<String,Map<String,Slice>>();

    for(String collectionName: stateMap.keySet()){
      Map<String, Object> collection = (Map<String, Object>)stateMap.get(collectionName);
      Map<String, Slice> slices = new LinkedHashMap<String,Slice>();

      for (Entry<String,Object> sliceEntry : collection.entrySet()) {
        Slice slice = new Slice(sliceEntry.getKey(), null, (Map<String,Object>)sliceEntry.getValue());
        slices.put(slice.getName(), slice);
      }
      state.put(collectionName, slices);
    }
    return new ClusterState(version, liveNodes, state);
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
