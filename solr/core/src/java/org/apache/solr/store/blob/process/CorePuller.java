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

package org.apache.solr.store.blob.process;

import java.lang.invoke.MethodHandles;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.metadata.ServerSideMetadata;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.shared.SharedCoreConcurrencyController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes synchronous pulls of cores from the shared store.
 */
public class CorePuller {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Brings a local core up to date with the shard's index in the shared store.
   * 
   * @param core core to be pulled
   * @param sharedShardName identifier for the shard index data located on a shared store
   * @param shardVersionMetadata metadata pointing to the version of shard's index in the shared store to be pulled
   * @param isLeaderPulling whether pull is requested by a leader replica or not
   */
  public void pullCoreFromSharedStore(SolrCore core, String sharedShardName,
                                      SharedShardMetadataController.SharedShardVersionMetadata shardVersionMetadata,
                                      boolean isLeaderPulling) {
    CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    String collectionName = cloudDescriptor.getCollectionName();
    String shardName = cloudDescriptor.getShardId();
    String coreName = core.getName();
    try {
      log.info("Initiating pull for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
      CoreContainer coreContainer = core.getCoreContainer();
      SharedCoreConcurrencyController concurrencyController = coreContainer.getSharedStoreManager().getSharedCoreConcurrencyController();
      if (SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE.equals(shardVersionMetadata.getMetadataSuffix())) {
        //no-op pull
        BlobCoreMetadata emptyBlobCoreMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(sharedShardName);
        concurrencyController.updateCoreVersionMetadata(collectionName, shardName, coreName, shardVersionMetadata, emptyBlobCoreMetadata, isLeaderPulling);
        log.info("Pull successful, nothing to pull, collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
        return;
      }
      concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreConcurrencyController.SharedCoreStage.BLOB_PULL_STARTED);
      try {
        // Get blob metadata
        String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(shardVersionMetadata.getMetadataSuffix());
        CoreStorageClient blobClient = coreContainer.getSharedStoreManager().getBlobStorageProvider().getClient();
        BlobCoreMetadata blobCoreMetadata = blobClient.pullCoreMetadata(sharedShardName, blobCoreMetadataName);
        if (null == blobCoreMetadata) {
          // Zookepeer and blob are out of sync, could be due to eventual consistency model in blob or something else went wrong.
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "cannot get core.metadata file from shared store, blobCoreMetadataName=" + blobCoreMetadataName +
                  " shard=" + shardName +
                  " collectionName=" + collectionName +
                  " sharedShardName=" + sharedShardName);
        } else if (blobCoreMetadata.getIsDeleted()) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "core.metadata file is marked deleted in shared store, blobCoreMetadataName=" + blobCoreMetadataName +
                  " shard=" + shardName +
                  " collectionName=" + collectionName +
                  " sharedShardName=" + sharedShardName);
        } else if (blobCoreMetadata.getIsCorrupt()) {
          log.warn("core.Metadata file is marked corrupt, skipping sync, collection=" + collectionName +
              " shard=" + shardName + " coreName=" + coreName + " sharedShardName=" + sharedShardName);
          return;
        }

        // Get local metadata + resolve with blob metadata. Given we're doing a pull, don't need to reserve commit point
        // We do need to compute a directory hash to verify after pulling or before switching index dirs that no local 
        // changes occurred concurrently
        ServerSideMetadata serverMetadata = new ServerSideMetadata(coreName, coreContainer,
            /* reserveCommit */ false, /* captureDirHash */ true);
        SharedStoreResolutionUtil.SharedMetadataResolutionResult resolutionResult = SharedStoreResolutionUtil.resolveMetadata(serverMetadata, blobCoreMetadata);
        PushPullData pushPullData = new PushPullData.Builder()
            .setCollectionName(collectionName)
            .setShardName(shardName)
            .setCoreName(coreName)
            .setSharedStoreName(sharedShardName)
            .build();

        if (resolutionResult.getFilesToPull().size() > 0) {
          BlobDeleteManager deleteManager = coreContainer.getSharedStoreManager().getBlobDeleteManager();
          CorePushPull cp = new CorePushPull(blobClient, deleteManager, pushPullData, resolutionResult, serverMetadata, blobCoreMetadata);
          cp.pullUpdateFromBlob(/* waitForSearcher */ true);
          concurrencyController.updateCoreVersionMetadata(pushPullData.getCollectionName(), pushPullData.getShardName(), pushPullData.getCoreName(),
              shardVersionMetadata, blobCoreMetadata, isLeaderPulling);
        } else {
          log.warn("Why there are no files to pull even when we do not match with the version in zk? " +
              "collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
        }
      } finally {
        concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreConcurrencyController.SharedCoreStage.BLOB_PULL_FINISHED);
      }
    } catch (Exception ex) {
      // wrap every thrown exception in a solr exception
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error trying to pull from the shared store," +
          " collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName, ex);
    }
  }

}
