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

/**
 * Class that manages metadata for shared index-based collections in Solr Cloud and 
 * ZooKeeper.
 */
public class SharedShardMetadataController {

  public static final String METADATA_NODE_DEFAULT_VALUE = "-1";
  public static final String SUFFIX_NODE_NAME = "metadataSuffix";
  
  private SolrCloudManager cloudManager;
  private DistribStateManager stateManager;

  public SharedShardMetadataController(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
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
   * If the update is successful, the returned {@link SharedShardVersionMetadata} will contain the new version as well as the 
   * value of the data just written. 
   * 
   * Specify version to be -1 to skip the node version check before update. 
   * 
   * @param collectionName name of the collection being updated
   * @param shardName name of the shard that owns the metadataSuffix node
   * @param value the value to be written to ZooKeeper
   * @param version the ZooKeeper node version to conditionally update on
   */
  public SharedShardVersionMetadata updateMetadataValueWithVersion(String collectionName, String shardName, String value, int version) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      Map<String, Object> nodeProps = new HashMap<>();
      nodeProps.put(SUFFIX_NODE_NAME, value);

      VersionedData data = stateManager.setAndGetResult(metadataPath, Utils.toJSON(nodeProps), version);
      Map<String, String> nodeUserData = (Map<String, String>) Utils.fromJSON(data.getData());
      String metadataSuffix = nodeUserData.get(SharedShardMetadataController.SUFFIX_NODE_NAME);
      return new SharedShardVersionMetadata(data.getVersion(), metadataSuffix);
    } catch (BadVersionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating path: " + metadataPath
          + " due to mismatching versions", e);
    } catch (IOException | NoSuchElementException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating path: " + metadataPath
          + " in ZooKeeper", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating path: " + metadataPath
          + " in ZooKeeper due to interruption", e);
    }
  }
  
  /**
   * Reads the {@link SharedShardVersionMetadata} for the shard from zookeeper. 
   * 
   * @param collectionName name of the shared collection
   * @param shardName name of the shard that owns the metadataSuffix node
   */
  public SharedShardVersionMetadata readMetadataValue(String collectionName, String shardName) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      VersionedData data = stateManager.getData(metadataPath, null);
      Map<String, String> nodeUserData = (Map<String, String>) Utils.fromJSON(data.getData());
      String metadataSuffix = nodeUserData.get(SharedShardMetadataController.SUFFIX_NODE_NAME);
      return  new SharedShardVersionMetadata(data.getVersion(), metadataSuffix);
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
    }
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

  /**
   * This represents correctness metadata for a shard of a shared collection {@link DocCollection#getSharedIndex()}
   */
  public static class SharedShardVersionMetadata {
    /**
     * version of zookeeper node maintaining the metadata
     */
    private final int version;
    /**
     * Unique value of the metadataSuffix for the last persisted shard index in the shared store.
     */
    private final String metadataSuffix;

    public SharedShardVersionMetadata(int version, String metadataSuffix) {
      this.version = version;
      this.metadataSuffix = metadataSuffix;
    }

    public int getVersion() {
      return version;
    }

    public String getMetadataSuffix() {
      return metadataSuffix;
    }
  }
}
