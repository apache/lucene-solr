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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.metadata.ServerSideMetadata;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.shared.SharedCoreConcurrencyController;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreStage;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreVersionMetadata;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController.SharedShardVersionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes synchronous pushes of core updates to the shared store.
 * 
 * Pushes will be triggered at the end of an indexing batch when a shard's index data has 
 * changed locally and needs to be persisted to the shared store. 
 */
public class CorePusher {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Pushes a core to the shared store.
   * @param core core to be pushed
   * @param sharedShardName identifier for the shard index data located on the shared store
   */
  public void pushCoreToSharedStore(SolrCore core, String sharedShardName) {
    CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    String collectionName = cloudDescriptor.getCollectionName();
    String shardName = cloudDescriptor.getShardId();
    String coreName = core.getName();
    try {
      log.info("Initiating push for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
      PushPullData pushPullData = new PushPullData.Builder()
          .setCollectionName(collectionName)
          .setShardName(shardName)
          .setCoreName(coreName)
          .setSharedStoreName(sharedShardName)
          .build();
      pushCoreToSharedStore(core, pushPullData);
    } catch (Exception ex) {
      // wrap every thrown exception in a solr exception
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error trying to push to the shared store," +
          " collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName, ex);
    }
  }

  /**
   * Pushes the local core updates to the shared store and logs whether the push succeeded or failed.
   */
  private void pushCoreToSharedStore(SolrCore core, PushPullData pushPullData) throws Exception {
    try {
      CoreContainer coreContainer = core.getCoreContainer();
      String collectionName = pushPullData.getCollectionName();
      String shardName = pushPullData.getShardName();
      String coreName = pushPullData.getCoreName();
      SharedCoreConcurrencyController concurrencyController = coreContainer.getSharedStoreManager().getSharedCoreConcurrencyController();
      ReentrantLock corePushLock = concurrencyController.getCorePushLock(collectionName, shardName, coreName);
      // TODO: Timebox the following push logic, in case we are stuck for long time. We would also need to respect any
      //       time constraints that comes with indexing request since we will be doing wasteful work if the client has already 
      //       bailed on us. This might be better done as part of bigger work item where we spike out how to allocate/configure
      //       time quotas in cooperation with client.
      // acquire push lock to serialize blob updates so that concurrent updates can't race with each other 
      // and cause push failures because one was able to advance zk to newer version.
      //
      // Only need to read this if we ever establish starvation on push lock and want to do something about it:
      // First thing we do after acquiring the lock is to see if latest commit's generation is equal to what we pushed last.
      // If equal then we have nothing to push, it's an optimization that saves us from creating somewhat expensive ServerSideMetadata.
      // That equality check can also be duplicated here before acquiring the push lock but that would just be a simple optimization.
      // It will not save us from starvation. Because that condition being "true" also means that there is no active pusher,
      // so there is no question of starvation.
      // One option could be to snapshot a queue of pusher threads, we are working on behalf of,
      // before we capture the commit point to push. Once finished pushing, we can dismiss all those threads together.
      long startTimeMs = BlobStoreUtils.getCurrentTimeMs();
      corePushLock.lock();
      try {
        long lockAcquisitionTime = BlobStoreUtils.getCurrentTimeMs() - startTimeMs;
        try {
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BLOB_PUSH_STARTED);

          IndexCommit latestCommit = core.getDeletionPolicy().getLatestCommit();
          if (latestCommit == null) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Core " + coreName + " has no available commit point");
          }

          SharedCoreVersionMetadata coreVersionMetadata = concurrencyController.getCoreVersionMetadata(collectionName, shardName, coreName);
          if (latestCommit.getGeneration() == coreVersionMetadata.getBlobCoreMetadata().getGeneration()) {
            // Everything up to latest commit point has already been pushed 
            // This can happen if another indexing batch comes in and acquires the push lock first and ends up pushing segments 
            // produced by this indexing batch.
            // This optimization saves us from creating somewhat expensive ServerSideMetadata.
            // Note1! At this point we might not be the leader and the shared store might have already received pushes from
            //        new leader. It is still ok to declare success since our indexing batch was correctly pushed earlier
            //        by another thread before the new leader could have pushed its batches.
            // Note2! It is important to note that we are piggybacking on cached BlobCoreMetadata's generation number here.
            //        A more accurate representation of this optimization would be to add a "lastGenerationPushed" property
            //        to per core cache and update it to whatever generation gets pushed successfully and to -1 on each 
            //        successful pull. But that is not necessary since local core will be on blobCoreMetadata's generation
            //        number after pull and on push blobCoreMetadata get generation number from local core.
            //        The reason it is important to call it out here is that BlobCoreMetadata' generation also gets
            //        persisted to the shared store which is not the requirement for this optimization.
            log.info("Nothing to push, pushLockTime=" + lockAcquisitionTime + " pushPullData=" + pushPullData.toString());
            return;
          }

          // Resolve the differences (if any) between the local shard index data and shard index data on the shared store
          // Reserving the commit point so it can be saved while pushing files to the shared store.
          // We don't need to compute a directory hash for the push scenario as we only need it to verify local 
          // index changes during pull
          ServerSideMetadata localCoreMetadata = new ServerSideMetadata(coreName, coreContainer, 
              /* reserveCommit */ true, /* captureDirHash */ false);
          SharedMetadataResolutionResult resolutionResult = SharedStoreResolutionUtil.resolveMetadata(
              localCoreMetadata, coreVersionMetadata.getBlobCoreMetadata());

          if (resolutionResult.getFilesToPush().isEmpty()) {
            log.warn("Why there is nothing to push even when there is a newer commit point since last push," +
                " pushLockTime=" + lockAcquisitionTime + " pushPullData=" + pushPullData.toString());
            return;
          }

          BlobStorageProvider blobProvider = coreContainer.getSharedStoreManager().getBlobStorageProvider();
          CoreStorageClient blobClient = blobProvider.getClient();
          BlobDeleteManager deleteManager = coreContainer.getSharedStoreManager().getBlobDeleteManager();
          String newMetadataSuffix = BlobStoreUtils.generateMetadataSuffix();
          // begin the push process 
          CorePushPull pushPull = new CorePushPull(blobClient, deleteManager, pushPullData, resolutionResult, localCoreMetadata, coreVersionMetadata.getBlobCoreMetadata());
          BlobCoreMetadata blobCoreMetadata = pushPull.pushToBlobStore(coreVersionMetadata.getMetadataSuffix(), newMetadataSuffix);
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BLOB_PUSHED);
          // at this point we've pushed the new metadata file with the newMetadataSuffix and now need to write to zookeeper
          SharedShardMetadataController shardSharedMetadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
          SharedShardVersionMetadata newShardVersionMetadata = null;
          try {
            newShardVersionMetadata = shardSharedMetadataController.updateMetadataValueWithVersion(collectionName, shardName,
                newMetadataSuffix, coreVersionMetadata.getVersion());
          } catch (Exception ex) {
            boolean isVersionMismatch = false;
            Throwable cause = ex;
            while (cause != null) {
              if (cause instanceof BadVersionException) {
                isVersionMismatch = true;
                break;
              }
              cause = cause.getCause();
            }
            if (isVersionMismatch) {
              // conditional update of zookeeper failed, take away soft guarantee of equality.
              // That will make sure before processing next indexing batch, we sync with zookeeper and pull from the shared store.
              concurrencyController.updateCoreVersionMetadata(collectionName, shardName, coreName, false);
            }
            throw ex;
          }
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.ZK_UPDATE_FINISHED);

          assert newMetadataSuffix.equals(newShardVersionMetadata.getMetadataSuffix());
          // after successful update to zookeeper, update core version metadata with new version info
          // and we can also give soft guarantee that core is up to date w.r.to the shared store, until unless failures happen and leadership changes 
          concurrencyController.updateCoreVersionMetadata(collectionName, shardName, coreName, newShardVersionMetadata, blobCoreMetadata, /* softGuaranteeOfEquality */ true);
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.LOCAL_CACHE_UPDATE_FINISHED);
          log.info("Successfully pushed to the shared store," +
              " pushLockTime=" + lockAcquisitionTime + " pushPullData=" + pushPullData.toString());
        } finally {
            concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BLOB_PUSH_FINISHED);
        }
      } finally {
        corePushLock.unlock();
      }
      // TODO - make error handling a little nicer?
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher was interrupted while pushing to the shared store", e);
    } catch (SolrException e) {
      Throwable t = e.getCause();
      if (t instanceof BadVersionException) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher failed to push because the node "
            + "version doesn't match, requestedVersion=" + ((BadVersionException) t).getRequested(), t);
      }
      throw e;
    }
  }
}