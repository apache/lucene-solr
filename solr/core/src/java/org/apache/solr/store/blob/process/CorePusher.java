package org.apache.solr.store.blob.process;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
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
 * This class executes synchronous pushes of core updates to blob store. See the implementation of asynchronous pulls
 * in {@link CorePullerFeeder}.
 * 
 * Pushes will be triggered from {@link CoreUpdateTracker}, which Solr code notifies when a shard's index data has 
 * changed locally and needs to be persisted to a shared store (blob store). 
 */
public class CorePusher {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CoreContainer coreContainer;

  public CorePusher(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  /**
   * Pushes the local core updates to the Blob store and logs whether the push succeeded or failed.
   */
  public void pushCoreToBlob(PushPullData pushPullData) throws Exception {
    BlobStorageProvider blobProvider = coreContainer.getSharedStoreManager().getBlobStorageProvider();
    CoreStorageClient blobClient = blobProvider.getClient();
    BlobDeleteManager deleteManager = coreContainer.getSharedStoreManager().getBlobDeleteManager();
    try {
      String shardName = pushPullData.getShardName();
      String collectionName = pushPullData.getCollectionName();
      String coreName = pushPullData.getCoreName();
      SharedCoreConcurrencyController concurrencyController = coreContainer.getSharedStoreManager().getSharedCoreConcurrencyController();
      ReentrantLock corePushLock = concurrencyController.getCorePushLock(collectionName, shardName, coreName);
      String snapshotDirPath = null;
      // TODO: Timebox the following push logic, in case we are stuck for long time. We would also need to respect any
      //       time constraints that comes with indexing request since we will be doing wasteful work if the client has already 
      //       bailed on us. This might be better done as part of bigger work item where we spike out how to allocate/configure
      //       time quotas in cooperation with client.
      // acquire push lock to serialize blob updates so that concurrent updates can't race with each other 
      // and cause push failures because one was able to advance zk to newer version.
      //
      // Only need to read this if we ever establish starvation on push lock and want to do something about it:
      // First thing we do after acquiring the lock is to see if latest commit's generation is equal to what we pushed last.
      // If equal then we have nothing to push, its an optimization that saves us from creating somewhat expensive ServerSideMetadata.
      // That equality check can also be duplicated here before acquiring the push lock but that would just be a simple optimization.
      // It will not save us from starvation. Because that condition being "true" also means that there is no active pusher,
      // so there is no question of starvation.
      // One option could be to snapshot a queue of pusher threads, we are working on behalf of,
      // before we capture the commit point to push. Once finished pushing, we can dismiss all those threads together.
      long startTimeMs = System.currentTimeMillis();
      corePushLock.lock();
      try {
        long lockAcquisitionTime = System.currentTimeMillis() - startTimeMs;
        SolrCore core = coreContainer.getCore(coreName);
        if (core == null) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Can't find core " + coreName);
        }
        try {
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BlobPushStarted);

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
            log.info(String.format("Nothing to push, pushLockTime=%s pushPullData=%s", lockAcquisitionTime, pushPullData.toString()));
            return;
          }

          log.info("Push to shared store initiating with PushPullData= " + pushPullData.toString());
          // Resolve the differences between the local shard index data and shard index data on shared store
          // if there is any
          ServerSideMetadata localCoreMetadata = new ServerSideMetadata(coreName, coreContainer, /* takeSnapshot */true);
          snapshotDirPath = localCoreMetadata.getSnapshotDirPath();
          SharedMetadataResolutionResult resolutionResult = SharedStoreResolutionUtil.resolveMetadata(
              localCoreMetadata, coreVersionMetadata.getBlobCoreMetadata());

          if (resolutionResult.getFilesToPush().isEmpty()) {
            log.warn(String.format("Why there is nothing to push even when there is a newer commit point since last push," +
                " pushLockTime=%s pushPullData=%s", lockAcquisitionTime, pushPullData.toString()));
            return;
          }

          String newMetadataSuffix = BlobStoreUtils.generateMetadataSuffix();
          // begin the push process 
          CorePushPull pushPull = new CorePushPull(blobClient, deleteManager, pushPullData, resolutionResult, localCoreMetadata, coreVersionMetadata.getBlobCoreMetadata());
          BlobCoreMetadata blobCoreMetadata = pushPull.pushToBlobStore(coreVersionMetadata.getMetadataSuffix(), newMetadataSuffix);
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BlobPushed);
          // at this point we've pushed the new metadata file with the newMetadataSuffix and now need to write to zookeeper
          SharedShardMetadataController shardSharedMetadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
          SharedShardVersionMetadata newShardVersionMetadata = null;
          try {
            newShardVersionMetadata = shardSharedMetadataController.updateMetadataValueWithVersion(pushPullData.getCollectionName(), pushPullData.getShardName(),
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
              // That will make sure before processing next indexing batch, we sync with zookeeper and pull from shared store.
              concurrencyController.updateCoreVersionMetadata(collectionName, shardName, coreName, false);
            }
            throw ex;
          }
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.ZkUpdateFinished);

          assert newMetadataSuffix.equals(newShardVersionMetadata.getMetadataSuffix());
          // after successful update to zookeeper, update core version metadata with new version info
          // and we can also give soft guarantee that core is up to date w.r.to shared store, until unless failures happen and leadership changes 
          concurrencyController.updateCoreVersionMetadata(collectionName, shardName, coreName, newShardVersionMetadata, blobCoreMetadata, /* softGuaranteeOfEquality */ true);
          concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.LocalCacheUpdateFinished);
          log.info(String.format("Successfully pushed to shared store, pushLockTime=%s pushPullData=%s", lockAcquisitionTime, pushPullData.toString()));
        } finally {
          try {
            concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreStage.BlobPushFinished);
            if (snapshotDirPath != null) {
              // we are done with push we can now remove the snapshot directory
              removeSnapshotDirectory(core, snapshotDirPath);
            }
          } finally {
            core.close();
          }
        }
      } finally {
        corePushLock.unlock();
      }
      // TODO - make error handling a little nicer?
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher was interrupted while pushing to blob store", e);
    } catch (IndexNotFoundException infe) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher failed because the core " + pushPullData.getCoreName() +
          " for the shard " + pushPullData.getShardName() + " was not found", infe);
    } catch (SolrException e) {
      Throwable t = e.getCause();
      if (t instanceof BadVersionException) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher failed to push because the node "
            + "version doesn't match.", t);
      }
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CorePusher failed to push shard index for "
          + pushPullData.getShardName() + " due to unexpected exception", e);
    }
  }

  private void removeSnapshotDirectory(SolrCore core, String snapshotDirPath) throws IOException {
    Directory snapshotDir = core.getDirectoryFactory().get(snapshotDirPath, DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
    try {
      core.getDirectoryFactory().doneWithDirectory(snapshotDir);
      core.getDirectoryFactory().remove(snapshotDir);
    } catch (Exception e) {
      log.warn("Cannot remove snapshot directory " + snapshotDirPath, e);
    } finally {
      core.getDirectoryFactory().release(snapshotDir);
    }
  }
}