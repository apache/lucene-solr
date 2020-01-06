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

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.metadata.CorePushPull;
import org.apache.solr.store.blob.metadata.ServerSideMetadata;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil;
import org.apache.solr.store.blob.metadata.SharedStoreResolutionUtil.SharedMetadataResolutionResult;
import org.apache.solr.store.blob.process.CorePullerFeeder.PullCoreInfo;
import org.apache.solr.store.blob.provider.BlobStorageProvider;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.blob.util.DeduplicatingList;
import org.apache.solr.store.shared.SharedCoreConcurrencyController;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreStage;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreVersionMetadata;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController.SharedShardVersionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code for pulling updates on a specific core to the Blob store.
 */
public class CorePullTask implements DeduplicatingList.Deduplicatable<String> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Minimum delay between to pull retries for a given core. Setting this higher than the push retry to reduce noise
   * we get from a flood of queries for a stale core
   * 
   * TODO: make configurable
   */
  private static final long MIN_RETRY_DELAY_MS = 20000;

  private final CoreContainer coreContainer;
  private final PullCoreInfo pullCoreInfo;
  
  /**
   * Data structure injected as dependencies that track the cores that have been created and not pulled. 
   * This should be passed in via a constructor from CorePullerFeeder where it is defined
   * and are unique per CorePullerFeeder (itself a singleton).
   */
  private final Set<String> coresCreatedNotPulledYet;
  
  private final long queuedTimeMs;
  private int attempts;
  private long lastAttemptTimestamp;
  private final PullCoreCallback callback;

  CorePullTask(CoreContainer coreContainer, PullCoreInfo pullCoreInfo, PullCoreCallback callback, Set<String> coresCreatedNotPulledYet) {
    this(coreContainer, pullCoreInfo, System.nanoTime(), 0, 0L, callback, coresCreatedNotPulledYet);
  }

  @VisibleForTesting
  CorePullTask(CoreContainer coreContainer, PullCoreInfo pullCoreInfo, long queuedTimeMs, int attempts,
      long lastAttemptTimestamp, PullCoreCallback callback, Set<String> coresCreatedNotPulledYet) {
    this.coreContainer = coreContainer;
    this.pullCoreInfo = pullCoreInfo;
    this.queuedTimeMs = queuedTimeMs;
    this.attempts = attempts;
    this.lastAttemptTimestamp = lastAttemptTimestamp;
    this.callback = callback;
    this.coresCreatedNotPulledYet = coresCreatedNotPulledYet;
  }
  
  /**
   * Needed for the {@link CorePullTask} to be used in a {@link DeduplicatingList}.
   */
  @Override
  public String getDedupeKey() {
    return this.pullCoreInfo.getCoreName();
  }

  /**
   * Needed for the {@link CorePullTask} to be used in a {@link DeduplicatingList}.
   */
  static class PullTaskMerger implements DeduplicatingList.Merger<String, CorePullTask> {
    /**
     * Given two tasks (that have not yet started executing!) that target the same shard (and would basically do the
     * same things were they both executed), returns a merged task that can replace both and that retains the oldest
     * enqueue time and the smallest number of attempts, so we don't "lose" retries because of the merge yet we
     * correctly report that tasks might have been waiting for execution for a long while.
     * 
     * @return a merged {@link CorePullTask} that can replace the two tasks passed as parameters.
     */
    @Override
    public CorePullTask merge(CorePullTask task1, CorePullTask task2) {
      // The asserts below are not guaranteed by construction but we know that's the case
      assert task1.coreContainer == task2.coreContainer;
      assert task1.callback == task2.callback;

      int mergedAttempts;
      long mergedLatAttemptsTimestamp;

      // Synchronizing on the tasks separately to not risk deadlock (even though in 
      // practice there's only one concurrent call to this method anyway since it's 
      // called from DeduplicatingList.addDeduplicated() and we synchronize on the
      // list there).
      synchronized (task1) {
        mergedAttempts = task1.attempts;
        mergedLatAttemptsTimestamp = task1.lastAttemptTimestamp;
      }

      synchronized (task2) {
        // We allow more opportunities to try as the core is changed again by Solr...
        mergedAttempts = Math.min(mergedAttempts, task2.attempts);
        // ...and base the delay computation on the time of last attempt
        mergedLatAttemptsTimestamp = Math.max(mergedLatAttemptsTimestamp, task2.lastAttemptTimestamp);
      }

      PullCoreInfo mergedPullCoreInfo = CorePullerFeeder.PullCoreInfoMerger.mergePullCoreInfos(task1.pullCoreInfo, task2.pullCoreInfo);
      
      // Try to set the callback for the deduplicated task to TASK_MERGED
      try {
        task2.callback.finishedPull(task2, null, CoreSyncStatus.TASK_MERGED, "CorePullTask merged with duplicate task in queue.");
      } catch (Exception e) {
        // Do nothing - tried to set callback for deduplicated task; if an error is thrown, ignore
      }
      
      // We merge the tasks.
      return new CorePullTask(task1.coreContainer, mergedPullCoreInfo,
          Math.min(task1.queuedTimeMs, task2.queuedTimeMs), mergedAttempts, mergedLatAttemptsTimestamp,
          task1.callback, task1.coresCreatedNotPulledYet);
    }
  }

  public synchronized void setAttempts(int attempts) {
    this.attempts = attempts;
  }

  public synchronized int getAttempts() {
    return this.attempts;
  }

  synchronized void setLastAttemptTimestamp(long lastAttemptTimestamp) {
    this.lastAttemptTimestamp = lastAttemptTimestamp;
  }

  /**
   * This method is only used in this class for now because the "reenqueue with delay" implementation is imperfect.
   * Longer term, such a reenqueue should be handled outside this class.
   */
  synchronized long getLastAttemptTimestamp() {
    return this.lastAttemptTimestamp;
  }

  public PullCoreInfo getPullCoreInfo() {
    return pullCoreInfo;
  }
  
  public long getQueuedTimeMs() {
    return this.queuedTimeMs;
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
  }

  /**
   * Pulls the local core updates from the Blob store then calls the task callback to notify the
   * {@link CorePullerFeeder} of success or failure of the operation, give an indication of the reason the periodic
   * puller can decide to retry or not.
   */
  void pullCoreFromBlob(boolean isLeaderPulling) throws InterruptedException {
    BlobCoreMetadata blobMetadata = null;
    if (coreContainer.isShutDown()) {
      this.callback.finishedPull(this, blobMetadata, CoreSyncStatus.SHUTTING_DOWN, null);
      // TODO could throw InterruptedException here or interrupt ourselves if we wanted to signal to
      // CorePullerThread to stop everything.
      return;
    }

    // Copying the non final variables so we're clean wrt the Java memory model and values do not change as we go
    // (even though we know that no other thread can be working on this CorePullTask when we handle it here).
    final int attemptsCopy = getAttempts();
    final long lastAttemptTimestampCopy = getLastAttemptTimestamp();

    if (attemptsCopy != 0) {
      long now = System.nanoTime();
      if (now - lastAttemptTimestampCopy < MIN_RETRY_DELAY_MS) {
        Thread.sleep(MIN_RETRY_DELAY_MS - now + lastAttemptTimestampCopy);
      }
    }

    SharedCoreConcurrencyController concurrencyController = coreContainer.getSharedStoreManager().getSharedCoreConcurrencyController();
    CoreSyncStatus syncStatus = CoreSyncStatus.FAILURE;
    // Auxiliary information related to pull outcome. It can be metadata resolver message which can be null or exception detail in case of failure 
    String message = null;
    try {
      // Do the sequence of actions required to pull a core from the Blob store.
      BlobStorageProvider blobProvider = coreContainer.getSharedStoreManager().getBlobStorageProvider(); 
      CoreStorageClient blobClient = blobProvider.getClient();

      SharedCoreVersionMetadata coreVersionMetadata = concurrencyController.getCoreVersionMetadata(pullCoreInfo.getCollectionName(),
          pullCoreInfo.getShardName(),
          pullCoreInfo.getCoreName());

      SharedShardMetadataController metadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
      SharedShardVersionMetadata shardVersionMetadata =  metadataController.readMetadataValue(pullCoreInfo.getCollectionName(), pullCoreInfo.getShardName());
      
      if(concurrencyController.areVersionsEqual(coreVersionMetadata, shardVersionMetadata)) {
        // already in sync
        this.callback.finishedPull(this, coreVersionMetadata.getBlobCoreMetadata(), CoreSyncStatus.SUCCESS_EQUIVALENT, null);
        return;
      } 
      if (SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE.equals(shardVersionMetadata.getMetadataSuffix())) {
        // no-op pull
        BlobCoreMetadata emptyBlobCoreMetadata = BlobCoreMetadataBuilder.buildEmptyCoreMetadata(pullCoreInfo.getSharedStoreName());
        concurrencyController.updateCoreVersionMetadata(pullCoreInfo.getCollectionName(), pullCoreInfo.getShardName(), pullCoreInfo.getCoreName(), 
            shardVersionMetadata, emptyBlobCoreMetadata, isLeaderPulling);
        this.callback.finishedPull(this, emptyBlobCoreMetadata, CoreSyncStatus.SUCCESS_EQUIVALENT, null);
        return;
      }

      concurrencyController.recordState(pullCoreInfo.getCollectionName(), pullCoreInfo.getShardName(), pullCoreInfo.getCoreName(), SharedCoreStage.BLOB_PULL_STARTED);
      // Get blob metadata
      String blobCoreMetadataName = BlobStoreUtils.buildBlobStoreMetadataName(shardVersionMetadata.getMetadataSuffix());
      blobMetadata = blobClient.pullCoreMetadata(pullCoreInfo.getSharedStoreName(), blobCoreMetadataName);
      
      // Handle callback
      if (blobMetadata == null) {
        syncStatus = CoreSyncStatus.BLOB_MISSING;
        this.callback.finishedPull(this, blobMetadata, syncStatus, null);
        return;
      } else if (blobMetadata.getIsDeleted()) {
        syncStatus = CoreSyncStatus.BLOB_DELETED_FOR_PULL;
        this.callback.finishedPull(this, blobMetadata, syncStatus, "deleted flag is set on core in Blob store. Not pulling.");
        return;
      } else if (blobMetadata.getIsCorrupt()) {
        // TODO this might mean we have no local core at this stage. If that's the case, we may need to do something about it so that Core App does not immediately reindex into a new core...
        //      likely changes needed here for W-5388477 Blob store corruption repair
        syncStatus = CoreSyncStatus.BLOB_CORRUPT;
        this.callback.finishedPull(this, blobMetadata, syncStatus, "corrupt flag is set on core in Blob store. Not pulling.");
        return;
      }

      // TODO unknown core pulls in context of solr cloud
      if (!coreExists(pullCoreInfo.getCoreName())) {
        if (pullCoreInfo.shouldCreateCoreIfAbsent()) {
          // We set the core as created awaiting pull before creating it, otherwise it's too late.
          // If we get to this point, we're setting the "created not pulled yet" status of the core here (only place
          // in the code where this happens) and we're clearing it in the finally below.
          // We're not leaking entries in coresCreatedNotPulledYet that might stay there forever...
          synchronized (coresCreatedNotPulledYet) {
            coresCreatedNotPulledYet.add(pullCoreInfo.getSharedStoreName());
          }
          createCore(pullCoreInfo);
        } else {
          syncStatus = CoreSyncStatus.LOCAL_MISSING_FOR_PULL;
          this.callback.finishedPull(this, blobMetadata, syncStatus, null);
          return;
        }
      }

      // Get local metadata + resolve with blob metadata. Given we're doing a pull, don't need to reserve commit point
      ServerSideMetadata serverMetadata = new ServerSideMetadata(pullCoreInfo.getCoreName(), coreContainer, false);
      SharedMetadataResolutionResult resolutionResult = SharedStoreResolutionUtil.resolveMetadata(
          serverMetadata, blobMetadata);
      
      // If there is nothing to pull, we should report SUCCESS_EQUIVALENT and do nothing.
      // If we call pullUpdateFromBlob with an empty list of files to pull, we'll see an NPE down the line.
      // TODO: might be better to handle this error in CorePushPull.pullUpdateFromBlob
      if (resolutionResult.getFilesToPull().size() > 0) {
        BlobDeleteManager deleteManager = coreContainer.getSharedStoreManager().getBlobDeleteManager();
        CorePushPull cp = new CorePushPull(blobClient, deleteManager, pullCoreInfo, resolutionResult, serverMetadata, blobMetadata);
        // TODO: we are computing/tracking attempts but we are not passing it along
        cp.pullUpdateFromBlob(/* waitForSearcher */ true);
        concurrencyController.updateCoreVersionMetadata(pullCoreInfo.getCollectionName(), pullCoreInfo.getShardName(), pullCoreInfo.getCoreName(), 
            shardVersionMetadata, blobMetadata, isLeaderPulling);
        syncStatus = CoreSyncStatus.SUCCESS;
      } else {
        log.warn(String.format(Locale.ROOT,
            "Why there are no files to pull even when we do not match with the version in zk? collection=%s shard=%s core=%s",
            pullCoreInfo.getCollectionName(), pullCoreInfo.getShardName(), pullCoreInfo.getCoreName()));
        syncStatus = CoreSyncStatus.SUCCESS_EQUIVALENT;
      }

      // The following call can fail if blob is corrupt (in non trivial ways, trivial ways are identified by other cases)
      // pull was successful
      // if (CorePullerFeeder.isEmptyCoreAwaitingPull(coreContainer, pullCoreInfo.getCoreName())) {
      //   the javadoc for pulledBlob suggests that it is only meant to be called if we pulled from scratch
      //   therefore only limiting this call when we created the local core for this pull ourselves
      //   BlobTransientLog.get().getCorruptCoreTracker().pulledBlob(pullCoreInfo.coreName, blobMetadata);
      // }
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      syncStatus = CoreSyncStatus.FAILURE;
      message = Throwables.getStackTraceAsString(e);
      log.warn("Failed (attempt=" + attemptsCopy + ") to pull core " + pullCoreInfo.getSharedStoreName(), e);
    } finally {
      // No matter how the pull ends (success or any kind of error), we don't want to consider the core as awaiting pull,
      // since it doesn't anymore (code is inline here rather than in a method or in notifyEndOfPull() to make
      // it clear how coresCreatedNotPulledYet is managed).
      synchronized (coresCreatedNotPulledYet) {
        // TODO: Can we move this business of core creation and deletion outside of this task so that
        //       we may not sub-optimally repeatedly create/delete core in case of reattempt of a transient pull error?
        //       or get whether a reattempt will be made or not, and if there is a guaranteed reattempt then do not delete it
        if (coresCreatedNotPulledYet.remove(pullCoreInfo.getSharedStoreName())) {
          if (!syncStatus.isSuccess()) {
            // If we created the core and we could not pull successfully then we should cleanup after ourselves by deleting it
            // otherwise queries can incorrectly return 0 results from that core.
            if (coreExists(pullCoreInfo.getCoreName())) {
              try {
                // try to delete core within 3 minutes. In future when we time box our pull task then we 
                // need to make sure this value is within that bound. 
                // CoreDeleter.deleteCoreByName(coreContainer, pullCoreInfo.coreName, 3, TimeUnit.MINUTES);
                // TODO: need to migrate deleter
              } catch (Exception ex) {
                // TODO: should we gack?
                //       can we do anything more here since we are unable to delete and we are leaving an empty core behind
                //       when we should not. Should we keep the core in coresCreatedNotPulledYet and try few more times
                //       but at some point we would have to let it go
                //       So may be, few more attempts here and then gack
                log.warn("CorePullTask successfully created local core but failed to pull it" +
                    " and now is unable to delete that local core " + pullCoreInfo.getCoreName(), ex);
              }
            }
          }
        }
      }
    }
    this.callback.finishedPull(this, blobMetadata, syncStatus, message);
    concurrencyController.recordState(pullCoreInfo.getCollectionName(), pullCoreInfo.getShardName(), pullCoreInfo.getCoreName(), SharedCoreStage.BLOB_PULL_FINISHED);
  }

  void finishedPull(BlobCoreMetadata blobCoreMetadata, CoreSyncStatus syncStatus, String message) throws InterruptedException {
    this.callback.finishedPull(this, blobCoreMetadata, syncStatus, message);
  }

  /**
   * Returns true if the given core exists.
   */
  private boolean coreExists(String coreName) {

    SolrCore core = null;
    File coreIndexDir = new File(coreContainer.getCoreRootDirectory() + "/" + coreName);
    if (coreIndexDir.exists()) {
      core = coreContainer.getCore(coreName);
    }

    log.info("Core " + coreName + " expected in dir " + coreIndexDir.getAbsolutePath() + " exists=" + coreIndexDir.exists()
    + " and location.instanceDirectory.getAbsolutePath()=" + coreIndexDir.getAbsolutePath());

    if (core != null) {
      // Core exists.
      core.close();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Creates a local (empty) core. This is required before we can fill this core with data pulled from Blob.
   */
  private void createCore(PullCoreInfo pci) throws Exception {

    log.info("About to create local core " + pci.getCoreName());

    ZkController controller = coreContainer.getZkController();
    DocCollection collection = controller.getZkStateReader().
        getClusterState().getCollection(pci.getCollectionName());
    Collection<Replica> replicas = collection.getReplicas();
    Replica replica = null;
    for (Replica r : replicas) {
      if (r.getCoreName().equals(pci.getCoreName())) {
        replica = r;
        break;
      }
    }

    if (replica == null) {
      throw new Exception("Replica " + pci.getCoreName() + " for collection " +
          pci.getCollectionName() + " does not exist in ZK");
    }

    Map<String, String> coreProperties = BlobStoreUtils.getSharedCoreProperties(controller.getZkStateReader(), collection, replica);

    coreContainer.create(pci.getCoreName(), coreContainer.getCoreRootDirectory().resolve(pci.getCoreName()),
        coreProperties, false);

    // TODO account for corrupt cores
  }

  /**
   * A callback for {@link CorePullTask} to notify after the pull attempt completes. The callback can enqueue a new
   * attempt to try again the core if the attempt failed.
   */
  public interface PullCoreCallback {
    /**
     * The task to pull the given coreInfo has completed.
     *
     * @param pullTask
     *            The core push task that has finished.
     * @param blobMetadata
     *            The blob metadata used to make the pull attempt
     * @param status
     *            How things went for the task
     * @param message
     *            Human readable message explaining a failure, or <code>null</code> if no message available.
     */
    public void finishedPull(CorePullTask pullTask, BlobCoreMetadata blobMetadata, CoreSyncStatus status, String message)
        throws InterruptedException;
  }
}
