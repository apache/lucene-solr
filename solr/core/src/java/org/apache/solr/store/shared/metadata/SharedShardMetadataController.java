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
package org.apache.solr.store.shared.metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class that manages metadata for shared index-based collections in Solr Cloud and 
 * ZooKeeper.
 */
public class SharedShardMetadataController {

  public static final String METADATA_NODE_DEFAULT_VALUE = "-1";
  public static final String SUFFIX_NODE_NAME = "metadataSuffix";
  
  private SolrCloudManager cloudManager;
  private DistribStateManager stateManager;
  /* 
   * Naive in-memory cache without any cache eviction logic used to cache zk version values.
   * TODO - convert to a more memory efficient and intelligent caching strategy. 
   */ 
  private ConcurrentHashMap<String, VersionedData> cache;
  
  public SharedShardMetadataController(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    cache = new ConcurrentHashMap<>();
  }
  
  @VisibleForTesting
  public SharedShardMetadataController(SolrCloudManager cloudManager, ConcurrentHashMap<String, VersionedData> cache) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    this.cache = cache;
  }
  
  /**
   * Creates a new metadata node if it doesn't exist for shared shared index whose correctness metadata 
   * is managed by ZooKeeper
   *  
   * @param collectionName name of the collection that needs a metadata node
   * @param shardName name of the shard that needs a metadata node
   */
  public void ensureMetadataNodeExists(String collectionName, String shardName) throws IOException {
    ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    if (!collection.getSharedIndex()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't create a metadataNode for collection " + collectionName + " that is not"
          + " of type Shared for shard " + shardName);
    }
    
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    Map<String, Object> nodeProps = new HashMap<>();
    nodeProps.put(SUFFIX_NODE_NAME, METADATA_NODE_DEFAULT_VALUE);
    
    createPersistentNodeIfNonExistent(metadataPath, Utils.toJSON(nodeProps));
  }

  /**
   * If the update is successful, the VersionedData will contain the new version as well as the 
   * value of the data just written. Successful updates will cache the new VersionedData while 
   * unsuccesful ones will invalidate any existing entries for the corresponding collectionName,
   * shardName combination.
   * 
   * Specify version to be -1 to skip the node version check before update. 
   * 
   * @param collectionName name of the collection being updated
   * @param shardName name of the shard that owns the metadataSuffix node
   * @param value the value to be written to ZooKeeper
   * @param version the ZooKeeper node version to conditionally update on
   */
  public VersionedData updateMetadataValueWithVersion(String collectionName, String shardName, String value, int version) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      Map<String, Object> nodeProps = new HashMap<>();
      nodeProps.put(SUFFIX_NODE_NAME, value);
      
      VersionedData data = stateManager.setAndGetResult(metadataPath, Utils.toJSON(nodeProps), version);
      cache.put(getCacheKey(collectionName, shardName), data);
      return data;
    } catch (BadVersionException e) {
      cache.remove(getCacheKey(collectionName, shardName));
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating path: " + metadataPath
          + " due to mismatching versions", e);
    } catch (IOException | NoSuchElementException | KeeperException e) {
      cache.remove(getCacheKey(collectionName, shardName));
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating path: " + metadataPath
          + " in ZooKeeper", e);
    } catch (InterruptedException e) {
      cache.remove(getCacheKey(collectionName, shardName));
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating path: " + metadataPath
          + " in ZooKeeper due to interruption", e);
    }
  }
  
  /**
   * The returned VersionedData will contain the version of the node as well as the contents of the node.
   * The data content of VersionedData will be a byte array that needs to be converted into a {@link Map}.
   * 
   * If readFromCache is true, we'll attempt to read from an in-memory cache the VersionedData based on 
   * the collectionName and shardName and return that if it exists. There is no gaurantee this cache
   * entry is not stale.
   * 
   * @param collectionName name of the collection being updated
   * @param shardName name of the shard that owns the metadataSuffix node
   */
  public VersionedData readMetadataValue(String collectionName, String shardName, boolean readfromCache) throws SolrException {
    if (readfromCache) {
      VersionedData cachedEntry = cache.get(getCacheKey(collectionName, shardName));
      if (cachedEntry != null) {
        return cachedEntry;
      }
    }
    return readMetadataValue(collectionName, shardName);
  }
  
  /**
   * The returned VersionedData will contain the version of the node as well as the contents of the node.
   * The data content of VersionedData will be a byte array that needs to be converted into a {@link Map}.
   * 
   * @param collectionName name of the collection being updated
   * @param shardName name of the shard that owns the metadataSuffix node
   */
  public VersionedData readMetadataValue(String collectionName, String shardName) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      return stateManager.getData(metadataPath, null);      
    } catch (IOException | NoSuchElementException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading data from path: " + metadataPath
          + " in ZooKeeper", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading data from path: " + metadataPath
          + " in ZooKeeper", e);
    }
  }
  
  /**
   * Removes the metadata node on zookeeper for the given collection and shard name. 
   * 
   * @param collectionName name of the collection being updated
   * @param shardName name of the shard that owns the metadataSuffix node
   */
  public void cleanUpMetadataNodes(String collectionName, String shardName) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      stateManager.removeRecursively(metadataPath, /* ignoreMissing */ true, /* includeRoot */ true);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error deleting path " + 
          metadataPath + " in Zookeeper", e);
    } finally {
      cache.clear();
    }
  }
  
  /**
   * Clears any cached version value if it exists for the corresponding collection and shard
   * 
   * @param collectionName name of the collection being updated
   * @param shardName name of the shard that owns the metadataSuffix node
   */
  public void clearCachedVersion(String collectionName, String shardName) {
    cache.remove(getCacheKey(collectionName, shardName));
  }
  
  private void createPersistentNodeIfNonExistent(String path, byte[] data) {
    try {
      if (!stateManager.hasData(path)) {
        try {
          stateManager.makePath(path, data, CreateMode.PERSISTENT, /* failOnExists */ false);
        } catch (AlreadyExistsException e) {
          // it's okay if another beats us creating the node
        }
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating path " + path
          + " in Zookeeper", e);
    } catch (IOException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating path " + path
          + " in Zookeeper", e);
    }
  }
  
  protected String getMetadataBasePath(String collectionName, String shardName) {
    return ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/" + ZkStateReader.SHARD_LEADERS_ZKNODE + "/" + shardName;
  }
  
  protected String getCacheKey(String collectionName, String shardName) {
    return collectionName + "_" + shardName;
  }
  
  @VisibleForTesting
  protected ConcurrentHashMap<String, VersionedData> getVersionedDataCache() {
    return cache;
  }
}
