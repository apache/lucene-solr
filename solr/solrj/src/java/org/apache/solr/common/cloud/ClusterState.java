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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.Watcher;
import org.noggit.JSONWriter;

/**
 * Immutable state of the cloud. Normally you can get the state by using
 * {@link ZkStateReader#getClusterState()}.
 * @lucene.experimental
 */
public class ClusterState implements JSONWriter.Writable {
  
  private final Integer znodeVersion;

  private final Map<String, CollectionRef> collectionStates, immutableCollectionStates;
  private Set<String> liveNodes;

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
    this.immutableCollectionStates = Collections.unmodifiableMap(collectionStates);
  }


  /**
   * Returns a new cluster state object modified with the given collection.
   *
   * @param collectionName the name of the modified (or deleted) collection
   * @param collection     the collection object. A null value deletes the collection from the state
   * @return the updated cluster state which preserves the current live nodes and zk node version
   */
  public ClusterState copyWith(String collectionName, DocCollection collection) {
    ClusterState result = new ClusterState(liveNodes, new LinkedHashMap<>(collectionStates), znodeVersion);
    if (collection == null) {
      result.collectionStates.remove(collectionName);
    } else {
      result.collectionStates.put(collectionName, new CollectionRef(collection));
    }
    return result;
  }


  /**
   * Get the lead replica for specific collection, or null if one currently doesn't exist.
   * @deprecated Use {@link DocCollection#getLeader(String)} instead
   */
  @Deprecated
  public Replica getLeader(String collection, String sliceName) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    Slice slice = coll.getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader();
  }

  /**
   * Returns true if the specified collection name exists, false otherwise.
   *
   * Implementation note: This method resolves the collection reference by calling
   * {@link CollectionRef#get()} which can make a call to ZooKeeper. This is necessary
   * because the semantics of how collection list is loaded have changed in SOLR-6629.
   * Please see javadocs in {@link ZkStateReader#refreshCollectionList(Watcher)}
   */
  public boolean hasCollection(String collectionName) {
    return getCollectionOrNull(collectionName) != null;
  }

  /**
   * Gets the replica by the core node name (assuming the slice is unknown) or null if replica is not found.
   * If the slice is known, do not use this method.
   * coreNodeName is the same as replicaName
   *
   * @deprecated use {@link DocCollection#getReplica(String)} instead
   */
  @Deprecated
  public Replica getReplica(final String collection, final String coreNodeName) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    for (Slice slice : coll.getSlices()) {
      Replica replica = slice.getReplica(coreNodeName);
      if (replica != null) return replica;
    }
    return null;
  }

  /**
   * Get the named Slice for collection, or null if not found.
   *
   * @deprecated use {@link DocCollection#getSlice(String)} instead
   */
  @Deprecated
  public Slice getSlice(String collection, String sliceName) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getSlice(sliceName);
  }

  /**
   * @deprecated use {@link DocCollection#getSlicesMap()} instead
   */
  @Deprecated
  public Map<String, Slice> getSlicesMap(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getSlicesMap();
  }

  /**
   * @deprecated use {@link DocCollection#getActiveSlicesMap()} instead
   */
  @Deprecated
  public Map<String, Slice> getActiveSlicesMap(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getActiveSlicesMap();
  }

  /**
   * @deprecated use {@link DocCollection#getSlices()} instead
   */
  @Deprecated
  public Collection<Slice> getSlices(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) return null;
    return coll.getSlices();
  }

  /**
   * @deprecated use {@link DocCollection#getActiveSlices()} instead
   */
  @Deprecated
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

  public CollectionRef getCollectionRef(String coll) {
    return  collectionStates.get(coll);
  }

  /**
   * Returns the corresponding {@link DocCollection} object for the given collection name
   * if such a collection exists. Returns null otherwise.
   *
   * Implementation note: This method resolves the collection reference by calling
   * {@link CollectionRef#get()} which may make a call to ZooKeeper. This is necessary
   * because the semantics of how collection list is loaded have changed in SOLR-6629.
   * Please see javadocs in {@link ZkStateReader#refreshCollectionList(Watcher)}
   */
  public DocCollection getCollectionOrNull(String collectionName) {
    CollectionRef ref = collectionStates.get(collectionName);
    return ref == null ? null : ref.get();
  }

  /**
   * Get collection names.
   *
   * Implementation note: This method resolves the collection reference by calling
   * {@link CollectionRef#get()} which can make a call to ZooKeeper. This is necessary
   * because the semantics of how collection list is loaded have changed in SOLR-6629.
   * Please see javadocs in {@link ZkStateReader#refreshCollectionList(Watcher)}
   *
   * @deprecated use {@link #getCollectionsMap()} to avoid a second lookup for lazy collections
   */
  @Deprecated
  public Set<String> getCollections() {
    Set<String> result = new HashSet<>();
    for (Entry<String, CollectionRef> entry : collectionStates.entrySet()) {
      if (entry.getValue().get() != null) {
        result.add(entry.getKey());
      }
    }
    return result;
  }

  /**
   * Get a map of collection name vs DocCollection objects
   *
   * Implementation note: This method resolves the collection reference by calling
   * {@link CollectionRef#get()} which can make a call to ZooKeeper. This is necessary
   * because the semantics of how collection list is loaded have changed in SOLR-6629.
   * Please see javadocs in {@link ZkStateReader#refreshCollectionList(Watcher)}
   *
   * @return a map of collection name vs DocCollection object
   */
  public Map<String, DocCollection> getCollectionsMap()  {
    Map<String, DocCollection> result = new HashMap<>(collectionStates.size());
    for (Entry<String, CollectionRef> entry : collectionStates.entrySet()) {
      DocCollection collection = entry.getValue().get();
      if (collection != null) {
        result.put(entry.getKey(), collection);
      }
    }
    return result;
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
    sb.append("collections:" + collectionStates);
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
    Map<String, Object> stateMap = (Map<String, Object>) Utils.fromJSON(bytes);
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
    Map<String,Map<String,String>> aliasMap = (Map<String,Map<String,String>>) Utils.fromJSON(bytes);

    return new Aliases(aliasMap);
  }

  // TODO move to static DocCollection.loadFromMap
  private static DocCollection collectionFromObjects(String name, Map<String, Object> objs, Integer version, String znode) {
    Map<String,Object> props;
    Map<String,Slice> slices;

    Map<String,Object> sliceObjs = (Map<String,Object>)objs.get(DocCollection.SHARDS);
    if (sliceObjs == null) {
      // legacy format from 4.0... there was no separate "shards" level to contain the collection shards.
      slices = Slice.loadAllFromMap(objs);
      props = Collections.emptyMap();
    } else {
      slices = Slice.loadAllFromMap(sliceObjs);
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

  @Override
  public void write(JSONWriter jsonWriter) {
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
   * @deprecated true cluster state spans many ZK nodes, stop depending on the version number of the shared node!
   */
  @Deprecated
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

  /** Be aware that this may return collections which may not exist now.
   * You can confirm that this collection exists after verifying
   * CollectionRef.get() != null
   */
  public Map<String, CollectionRef> getCollectionStates() {
    return immutableCollectionStates;
  }

  public static class CollectionRef {
    protected final AtomicInteger gets = new AtomicInteger();
    private final DocCollection coll;

    public int getCount(){
      return gets.get();
    }

    public CollectionRef(DocCollection coll) {
      this.coll = coll;
    }

    public DocCollection get(){
      gets.incrementAndGet();
      return coll;
    }

    public boolean isLazilyLoaded() { return false; }
    
    @Override
    public String toString() {
      if (coll != null) {
        return coll.toString();
      } else {
        return "null DocCollection ref";
      }
    }

  }

}
