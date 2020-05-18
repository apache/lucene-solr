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
package org.apache.solr.store.shared;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SharedStoreConfig;
import org.apache.solr.store.blob.metadata.BlobCoreSyncer;
import org.apache.solr.store.blob.process.BlobDeleteManager;
import org.apache.solr.store.blob.process.BlobProcessUtil;
import org.apache.solr.store.blob.process.CorePullTracker;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;

import com.google.common.annotations.VisibleForTesting;

/**
 * Provides access to Shared Store processes. Note that this class is meant to be 
 * more generic in the future and provide a cleaner API but for now we'll expose
 * the underlying implementations
 */
public class SharedStoreManager {
  
  private CoreContainer coreContainer;
  private SharedShardMetadataController sharedShardMetadataController;
  private BlobStorageProvider blobStorageProvider;
  private BlobDeleteManager blobDeleteManager;
  private BlobProcessUtil blobProcessUtil;
  private CorePullTracker corePullTracker;
  private BlobCoreSyncer blobCoreSyncer;
  private SharedCoreConcurrencyController sharedCoreConcurrencyController;
  private SharedStoreConfig config;

  public SharedStoreManager(CoreContainer coreContainer, SharedStoreConfig config) {
    this.coreContainer = coreContainer;
    ZkController zkController = coreContainer.getZkController();
    
    blobStorageProvider = new BlobStorageProvider();
    blobDeleteManager = new BlobDeleteManager(getBlobStorageProvider().getClient());
    corePullTracker = new CorePullTracker();
    sharedShardMetadataController = new SharedShardMetadataController(zkController.getSolrCloudManager());
    sharedCoreConcurrencyController = new SharedCoreConcurrencyController();
    blobCoreSyncer = new BlobCoreSyncer();
    blobProcessUtil = new BlobProcessUtil();
    this.config = config;
  }
  
  /**
   * Start blob processes that depend on an initiated {@link SharedStoreManager} in {@link CoreContainer}
   */
  public void load() {
    blobProcessUtil.load(this);
  }

  public SharedShardMetadataController getSharedShardMetadataController() {
    return sharedShardMetadataController;
  }
  
  public BlobStorageProvider getBlobStorageProvider() {
    return blobStorageProvider;
  }
  
  public BlobDeleteManager getBlobDeleteManager() {
    return blobDeleteManager;
  }
  
  public BlobProcessUtil getBlobProcessManager() {
    return blobProcessUtil;
  }
  
  public CorePullTracker getCorePullTracker() {
    return corePullTracker;
  }
  
  public BlobCoreSyncer getBlobCoreSyncer() {
    return blobCoreSyncer;
  }

  public SharedCoreConcurrencyController getSharedCoreConcurrencyController() {
    return sharedCoreConcurrencyController;
  }
  
  public CoreContainer getCoreContainer() {
    return coreContainer;
  }
  
  public SharedStoreConfig getConfig() {
    return config;
  }
  
  public void shutdown() {
    if (blobProcessUtil != null) {
      blobProcessUtil.shutdown();
    }
    if (blobDeleteManager != null) {
      blobDeleteManager.shutdown();
    }
  }

  @VisibleForTesting
  public void initConcurrencyController(SharedCoreConcurrencyController concurrencyController) {
    this.sharedCoreConcurrencyController = concurrencyController;
  }
  
  @VisibleForTesting
  public void initBlobStorageProvider(BlobStorageProvider blobStorageProvider) {
    this.blobStorageProvider = blobStorageProvider;
  }
  
  @VisibleForTesting
  public void initBlobProcessUtil(BlobProcessUtil processUtil) {
    if (blobProcessUtil != null) {
      blobProcessUtil.shutdown();
    }
    blobProcessUtil = processUtil;
  }
  
  @VisibleForTesting
  public void initBlobDeleteManager(BlobDeleteManager blobDeleteManager) {
    if (this.blobDeleteManager != null) {
      this.blobDeleteManager.shutdown();
    }
    this.blobDeleteManager = blobDeleteManager;
  }

}