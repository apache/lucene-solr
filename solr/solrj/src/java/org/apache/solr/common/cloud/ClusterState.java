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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.noggit.JSONWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
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
  }


  /**
   * Get the lead replica for specific collection, or null if one currently doesn't exist.
   */
  public Replica getLeader(String collection, String sliceName) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    Slice slice = coll.getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader();
  }
  
  /**
   * Gets the replica by the core name (assuming the slice is unknown) or null if replica is not found.
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
  public boolean hasCollection(String coll){
    return collectionStates.get(coll)!=null;
  }


  /**
   * Get the named Slice for collection, or null if not found.
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
  
  public Map<String, Slice> getActiveSlicesMap(String collection) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    return coll.getActiveSlicesMap();
  }

  public Collection<Slice> getSlices(String collection) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    return coll.getSlices();
  }

  public Collection<Slice> getActiveSlices(String collection) {
    DocCollection coll = collectionStates.get(collection);
    if (coll == null) return null;
    return coll.getActiveSlices();
  }

  /**
   * Get the named DocCollection object, or throw an exception if it doesn't exist.
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

  public String getShardId(String baseUrl, String coreName) {
    // System.out.println("###### getShardId(" + baseUrl + "," + coreName + ") in " + collectionStates);
    for (DocCollection coll : collectionStates.values()) {
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          // TODO: for really large clusters, we could 'index' on this
          String rbaseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
          String rcore = replica.getStr(ZkStateReader.CORE_NAME_PROP);
          if (baseUrl.equals(rbaseUrl) && coreName.equals(rcore)) {
            return slice.getName();
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
  
  public static Aliases load(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return new Aliases();
    }
    Map<String,Map<String,String>> aliasMap = (Map<String,Map<String,String>>) ZkStateReader.fromJSON(bytes);

    return new Aliases(aliasMap);
  }

  private static DocCollection collectionFromObjects(String name, Map<String,Object> objs) {
    Map<String,Object> props;
    Map<String,Slice> slices;

    Map<String,Object> sliceObjs = (Map<String,Object>)objs.get(DocCollection.SHARDS);
    if (sliceObjs == null) {
      // legacy format from 4.0... there was no separate "shards" level to contain the collection shards.
      slices = makeSlices(objs);
      props = Collections.emptyMap();
    } else {
      slices = makeSlices(sliceObjs);
      props = new HashMap<String, Object>(objs);
      objs.remove(DocCollection.SHARDS);
    }

    Object routerObj = props.get(DocCollection.DOC_ROUTER);
    DocRouter router;
    if (routerObj == null) {
      router = DocRouter.DEFAULT;
    } else if (routerObj instanceof String) {
      // back compat with Solr4.4
      router = DocRouter.getDocRouter((String)routerObj);
    } else {
      Map routerProps = (Map)routerObj;
      router = DocRouter.getDocRouter(routerProps.get("name"));
    }

    return new DocCollection(name, slices, props, router);
  }

  private static Map<String,Slice> makeSlices(Map<String,Object> genericSlices) {
    if (genericSlices == null) return Collections.emptyMap();
    Map<String,Slice> result = new LinkedHashMap<String, Slice>(genericSlices.size());
    for (Map.Entry<String,Object> entry : genericSlices.entrySet()) {
      String name = entry.getKey();
      Object val = entry.getValue();
      if (val instanceof Slice) {
        result.put(name, (Slice)val);
      } else if (val instanceof Map) {
        result.put(name, new Slice(name, null, (Map<String,Object>)val));
      }
    }
    return result;
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(collectionStates);
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
