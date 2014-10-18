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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable state of the cloud. Normally you can get the state by using
 * {@link ZkStateReader#getClusterState()}.
 * @lucene.experimental
 */
public class ClusterState implements JSONWriter.Writable {
  private static Logger log = LoggerFactory.getLogger(ClusterState.class);
  
  private Integer znodeVersion;
  
  private final Map<String, CollectionRef> collectionStates;
  private Set<String> liveNodes;


  /**
   * Use this constr when ClusterState is meant for publication.
   * 
   * hashCode and equals will only depend on liveNodes and not clusterStateVersion.
   */
  @Deprecated
  public ClusterState(Set<String> liveNodes,
      Map<String, DocCollection> collectionStates) {
    this(null, liveNodes, collectionStates);
  }

  /**
   * Use this constr when ClusterState is meant for consumption.
   */
  public ClusterState(Integer znodeVersion, Set<String> liveNodes,
      Map<String, DocCollection> collectionStates) {
    this(liveNodes, getRefMap(collectionStates),znodeVersion);
  }

  private static Map<String, CollectionRef> getRefMap(Map<String, DocCollection> collectionStates) {
    Map<String, CollectionRef> collRefs =  new LinkedHashMap<>(collectionStates.size());
    for (Entry<String, DocCollection> entry : collectionStates.entrySet()) {
      final DocCollection c = entry.getValue();
      collRefs.put(entry.getKey(), new CollectionRef(c));
    }
    return collRefs;
  }

  /**Use this if all the collection states are not readily available and some needs to be lazily loaded
   */
  public ClusterState(Set<String> liveNodes, Map<String, CollectionRef> collectionStates, Integer znodeVersion){
    this.znodeVersion = znodeVersion;
    this.liveNodes = new HashSet<>(liveNodes.size());
    this.liveNodes.addAll(liveNodes);
    this.collectionStates = new LinkedHashMap<>(collectionStates);
  }


  public ClusterState copyWith(Map<String,DocCollection> modified){
    ClusterState result = new ClusterState(liveNodes, new LinkedHashMap<>(collectionStates), znodeVersion);
    for (Entry<String, DocCollection> e : modified.entrySet()) {
      DocCollection c = e.getValue();
      if(c == null) {
        result.collectionStates.remove(e.getKey());
        continue;
      }
      result.collectionStates.put(c.getName(), new CollectionRef(c));
    }
    return result;
  }


  /**
   * Get the lead replica for specific collection, or null if one currently doesn't exist.
   */
  public Replica getLeader(String collection, String sliceName) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    Slice slice = coll.getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader();
  }
  private Replica getReplica(DocCollection coll, String replicaName) {
    if (coll == null) return null;
    for (Slice slice : coll.getSlices()) {
      Replica replica = slice.getReplica(replicaName);
      if (replica != null) return replica;
    }
    return null;
  }

  public boolean hasCollection(String coll) {
    return  collectionStates.containsKey(coll) ;
  }

  /**
   * Gets the replica by the core name (assuming the slice is unknown) or null if replica is not found.
   * If the slice is known, do not use this method.
   * coreNodeName is the same as replicaName
   */
  public Replica getReplica(final String collection, final String coreNodeName) {
    return getReplica(getCollectionOrNull(collection), coreNodeName);
  }

  /**
   * Get the named Slice for collection, or null if not found.
   */
  public Slice getSlice(String collection, String sliceName) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getSlice(sliceName);
  }

  public Map<String, Slice> getSlicesMap(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getSlicesMap();
  }
  
  public Map<String, Slice> getActiveSlicesMap(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getActiveSlicesMap();
  }

  public Collection<Slice> getSlices(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getSlices();
  }

  public Collection<Slice> getActiveSlices(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getActiveSlices();
  }


  /**
   * Get the named DocCollection object, or throw an exception if it doesn't exist.
   */
  public DocCollection getCollection(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection : " + collection);
    return coll;
  }


  public DocCollection getCollectionOrNull(String coll) {
    CollectionRef ref = collectionStates.get(coll);
    return ref == null? null:ref.get();
  }

  /**
   * Get collection names.
   */
  public Set<String> getCollections() {
    return collectionStates.keySet();
  }


  /**
   * Get names of the currently live nodes.
   */
  public Set<String> getLiveNodes() {
    return Collections.unmodifiableSet(liveNodes);
  }

  public String getShardId(String nodeName, String coreName) {
    return getShardId(null, nodeName, coreName);
  }

  public String getShardId(String collectionName, String nodeName, String coreName) {
    Collection<CollectionRef> states = collectionStates.values();
    if (collectionName != null) {
      CollectionRef c = collectionStates.get(collectionName);
      if (c != null) states = Collections.singletonList( c );
    }

    for (CollectionRef ref : states) {
      DocCollection coll = ref.get();
      if(coll == null) continue;// this collection go tremoved in between, skip
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          // TODO: for really large clusters, we could 'index' on this
          String rnodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
          String rcore = replica.getStr(ZkStateReader.CORE_NAME_PROP);
          if (nodeName.equals(rnodeName) && coreName.equals(rcore)) {
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

  public static ClusterState load(Integer version, byte[] bytes, Set<String> liveNodes) {
    return load(version, bytes, liveNodes, ZkStateReader.CLUSTER_STATE);
  }
  /**
   * Create ClusterState from json string that is typically stored in zookeeper.
   * 
   * @param version zk version of the clusterstate.json file (bytes)
   * @param bytes clusterstate.json as a byte array
   * @param liveNodes list of live nodes
   * @return the ClusterState
   */
  public static ClusterState load(Integer version, byte[] bytes, Set<String> liveNodes, String znode) {
    // System.out.println("######## ClusterState.load:" + (bytes==null ? null : new String(bytes)));
    if (bytes == null || bytes.length == 0) {
      return new ClusterState(version, liveNodes, Collections.<String, DocCollection>emptyMap());
    }
    Map<String, Object> stateMap = (Map<String, Object>) ZkStateReader.fromJSON(bytes);
    Map<String,CollectionRef> collections = new LinkedHashMap<>(stateMap.size());
    for (Entry<String, Object> entry : stateMap.entrySet()) {
      String collectionName = entry.getKey();
      DocCollection coll = collectionFromObjects(collectionName, (Map<String,Object>)entry.getValue(), version, znode);
      collections.put(collectionName, new CollectionRef(coll));
    }

    return new ClusterState( liveNodes, collections,version);
  }


  public static Aliases load(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return new Aliases();
    }
    Map<String,Map<String,String>> aliasMap = (Map<String,Map<String,String>>) ZkStateReader.fromJSON(bytes);

    return new Aliases(aliasMap);
  }

  private static DocCollection collectionFromObjects(String name, Map<String, Object> objs, Integer version, String znode) {
    Map<String,Object> props;
    Map<String,Slice> slices;

    Map<String,Object> sliceObjs = (Map<String,Object>)objs.get(DocCollection.SHARDS);
    if (sliceObjs == null) {
      // legacy format from 4.0... there was no separate "shards" level to contain the collection shards.
      slices = makeSlices(objs);
      props = Collections.emptyMap();
    } else {
      slices = makeSlices(sliceObjs);
      props = new HashMap<>(objs);
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
      router = DocRouter.getDocRouter((String) routerProps.get("name"));
    }

    return new DocCollection(name, slices, props, router, version, znode);
  }

  private static Map<String,Slice> makeSlices(Map<String,Object> genericSlices) {
    if (genericSlices == null) return Collections.emptyMap();
    Map<String,Slice> result = new LinkedHashMap<>(genericSlices.size());
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
    if (collectionStates.size() == 1) {
      CollectionRef ref = collectionStates.values().iterator().next();
      DocCollection docCollection = ref.get();
      if (docCollection.getStateFormat() > 1) {
        jsonWriter.write(Collections.singletonMap(docCollection.getName(), docCollection));
        // serializing a single DocCollection that is persisted outside of clusterstate.json
        return;
      }
    }

    LinkedHashMap<String , DocCollection> map = new LinkedHashMap<>();
    for (Entry<String, CollectionRef> e : collectionStates.entrySet()) {
      // using this class check to avoid fetching from ZK in case of lazily loaded collection
      if (e.getValue().getClass() == CollectionRef.class) {
        // check if it is a lazily loaded collection outside of clusterstate.json
        DocCollection coll = e.getValue().get();
        if (coll.getStateFormat() == 1) {
          map.put(coll.getName(),coll);
        }
      }
    }
    jsonWriter.write(map);
  }

  /**
   * The version of clusterstate.json in ZooKeeper.
   * 
   * @return null if ClusterState was created for publication, not consumption
   */
  public Integer getZkClusterStateVersion() {
    return znodeVersion;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((znodeVersion == null) ? 0 : znodeVersion.hashCode());
    result = prime * result + ((liveNodes == null) ? 0 : liveNodes.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ClusterState other = (ClusterState) obj;
    if (znodeVersion == null) {
      if (other.znodeVersion != null) return false;
    } else if (!znodeVersion.equals(other.znodeVersion)) return false;
    if (liveNodes == null) {
      if (other.liveNodes != null) return false;
    } else if (!liveNodes.equals(other.liveNodes)) return false;
    return true;
  }



  /**
   * Internal API used only by ZkStateReader
   */
  void setLiveNodes(Set<String> liveNodes){
    this.liveNodes = liveNodes;
  }

  /**For internal use only
   */
  Map<String, CollectionRef> getCollectionStates() {
    return collectionStates;
  }

  public static class CollectionRef {
    private final DocCollection coll;

    public CollectionRef(DocCollection coll) {
      this.coll = coll;
    }

    public DocCollection get(){
      return coll;
    }

  }

}
