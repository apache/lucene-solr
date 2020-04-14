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

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.process.CorePuller;
import org.apache.solr.store.blob.process.CorePusher;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for bringing a stale SHARED core upto date by pulling from the shared store at the start 
 * of an indexing batch and pushing the updated core at the end of a successfully committed indexing batch. 
 */
public class SharedCoreIndexingBatchProcessor implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * Time indexing thread needs to wait to try acquiring pull write lock before checking if someone else has already done the pull.
   */
  public static int SECONDS_TO_WAIT_INDEXING_PULL_WRITE_LOCK = 5;
  /**
   * Max attempts by indexing thread to try acquiring pull write lock before bailing out. Ideally bail out scenario should never happen.
   * If it does then either we are too slow in pulling and can tune this value or something else is wrong.
   */
  public static int MAX_ATTEMPTS_INDEXING_PULL_WRITE_LOCK = 10;

  private final SolrCore core;
  private final String collectionName;
  private final String shardName;
  private final String sharedShardName;
  private final CorePusher corePusher;
  private final CorePuller corePuller;
  private IndexingBatchState state;
  private ReentrantReadWriteLock corePullLock;

  public SharedCoreIndexingBatchProcessor(SolrCore core, ClusterState clusterState) {
    this.core = core;
    CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    collectionName = cloudDescriptor.getCollectionName();
    shardName = cloudDescriptor.getShardId();

    DocCollection collection = clusterState.getCollection(collectionName);
    if (!collection.getSharedIndex()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, collectionName + " is not a shared collection.");
    }

    Slice shard = collection.getSlicesMap().get(shardName);
    if (shard == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown shard=" + shardName + " in collection=" + collectionName);
    }

    sharedShardName = (String) shard.get(ZkStateReader.SHARED_SHARD_NAME);
    corePuller = new CorePuller();
    corePusher = new CorePusher();
    state = IndexingBatchState.NOT_STARTED;
  }

  /**
   * Should be called whenever a document is about to be added/deleted from the SHARED core. If it is the first doc
   * of the core, this method will mark  the start of an indexing batch and bring a stale SHARED core upto date by
   * pulling from the shared store.
   */
  public void addOrDeleteGoingToBeIndexedLocally() {
    // Following logic is built on the assumption that one particular instance of this processor
    // will solely be consumed by a single thread. And all the documents of indexing batch will be processed by this one instance. 
    String coreName = core.getName();
    if (IndexingBatchState.NOT_STARTED.equals(state)) {
      startIndexingBatch();
    } else if (IndexingBatchState.STARTED.equals(state)) {
      // do nothing, we only use this method to start an indexing batch once
    } else if (IndexingBatchState.COMMITTED.equals(state)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Why are we adding/deleting a doc through an already committed indexing batch?" +
              " collection=" + collectionName + " shard=" + shardName + " core=" + coreName);
    } else {
      throwUnknownStateError();
    }
  }

  @VisibleForTesting
  protected void startIndexingBatch() {
    // Following pull logic should only run once before the first add/delete of an indexing batch is processed by this processor

    assert IndexingBatchState.NOT_STARTED.equals(state);

    if (corePullLock != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "How come we already have a pull read lock?" +
          " collection=" + collectionName + " shard=" + shardName + " core=" + core.getName());
    }

    String coreName = core.getName();
    CoreContainer coreContainer = core.getCoreContainer();
    SharedCoreConcurrencyController concurrencyController = coreContainer.getSharedStoreManager().getSharedCoreConcurrencyController();
    corePullLock = concurrencyController.getCorePullLock(collectionName, shardName, coreName);
    // from this point on wards we should always exit this method with read lock (no matter failure or what)
    try {
      concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreConcurrencyController.SharedCoreStage.INDEXING_BATCH_RECEIVED);
      state = IndexingBatchState.STARTED;
      SharedCoreConcurrencyController.SharedCoreVersionMetadata coreVersionMetadata = concurrencyController.getCoreVersionMetadata(collectionName, shardName, coreName);
      /**
       * we only need to sync if there is no soft guarantee of being in sync.
       * if there is one we will rely on that, and if we turned out to be wrong indexing will fail at push time
       * and will remove this guarantee in {@link CorePusher#pushCoreToBlob(PushPullData)}
       */
      if (!coreVersionMetadata.isSoftGuaranteeOfEquality()) {
        SharedShardMetadataController metadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
        SharedShardMetadataController.SharedShardVersionMetadata shardVersionMetadata = metadataController.readMetadataValue(collectionName, shardName);
        if (!concurrencyController.areVersionsEqual(coreVersionMetadata, shardVersionMetadata)) {
          acquireWriteLockAndPull(collectionName, shardName, coreName, coreContainer);
        }
      }
    } finally {
      // acquire lock for the whole duration of update
      // we should always leave with read lock acquired(failure or success), since it is the job of close method to release it
      corePullLock.readLock().lock();
      concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreConcurrencyController.SharedCoreStage.LOCAL_INDEXING_STARTED);
    }
  }

  private void acquireWriteLockAndPull(String collectionName, String shardName, String coreName, CoreContainer coreContainer) {
    // There is a likelihood that many indexing requests came at once and realized we are out of sync.
    // They all would try to acquire write lock. One of them makes progress to pull from shared store.
    // After that regular indexing will see soft guarantee of equality and moves straight to indexing
    // under read lock. Now it is possible that new indexing keeps coming in and read lock is never free.
    // In that case the poor guys that came in earlier and wanted to pull will still be struggling(starving) to
    // acquire write lock. Since we know that write lock is only needed by one to do the work, we will
    // try time boxed acquisition and in case of failed acquisition we will see if some one else has already completed the pull.
    // We will make few attempts before we bail out. Ideally bail out scenario should never happen.
    // If it does then either we are too slow in pulling and can tune following parameters or something else is wrong.
    int attempt = 1;
    while (true) {
      SharedCoreConcurrencyController concurrencyController = coreContainer.getSharedStoreManager().getSharedCoreConcurrencyController();
      try {
        // try acquiring write lock
        if (corePullLock.writeLock().tryLock(SECONDS_TO_WAIT_INDEXING_PULL_WRITE_LOCK, TimeUnit.SECONDS)) {
          try {
            // while acquiring write lock things might have updated, should reestablish if pull is still needed
            SharedCoreConcurrencyController.SharedCoreVersionMetadata coreVersionMetadata = concurrencyController.getCoreVersionMetadata(collectionName, shardName, coreName);
            if (!coreVersionMetadata.isSoftGuaranteeOfEquality()) {
              SharedShardMetadataController metadataController = coreContainer.getSharedStoreManager().getSharedShardMetadataController();
              SharedShardMetadataController.SharedShardVersionMetadata shardVersionMetadata = metadataController.readMetadataValue(collectionName, shardName);
              if (!concurrencyController.areVersionsEqual(coreVersionMetadata, shardVersionMetadata)) {
                getCorePuller().pullCoreFromSharedStore(core, sharedShardName, shardVersionMetadata, /* isLeaderPulling */true);
              }
            }
          } finally {
            corePullLock.writeLock().unlock();
          }
          // write lock acquisition was successful and we are in sync with shared store
          break;
        } else {
          // we could not acquire write lock but see if some other thread has already done the pulling
          SharedCoreConcurrencyController.SharedCoreVersionMetadata coreVersionMetadata = concurrencyController.getCoreVersionMetadata(collectionName, shardName, coreName);
          if (coreVersionMetadata.isSoftGuaranteeOfEquality()) {
            log.info("Indexing thread timed out trying to acquire the pull write lock. " +
                "But some other thread has done the pulling so we are good. " +
                "attempt=" + attempt + " collection=" + collectionName + " shard=" + shardName + " core=" + coreName);
            break;
          }
          // no one else has pulled yet either, lets make another attempt ourselves
          attempt++;
          if (attempt > MAX_ATTEMPTS_INDEXING_PULL_WRITE_LOCK) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Indexing thread failed to acquire write lock for pull in " +
                (SECONDS_TO_WAIT_INDEXING_PULL_WRITE_LOCK * MAX_ATTEMPTS_INDEXING_PULL_WRITE_LOCK) + " seconds." +
                " And no one other thread either has done the pull during that time. " +
                " collection=" + collectionName + " shard=" + shardName + " core=" + coreName);
          }
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Indexing thread interrupted while trying to acquire pull write lock." +
            " collection=" + collectionName + " shard=" + shardName + " core=" + coreName, ie);
      }
    }
  }

  @VisibleForTesting
  protected CorePuller getCorePuller() {
    return corePuller;
  }

  /**
   * Should be called after the SHARED core is successfully hard committed locally. This method will push the updated
   * core to the shared store. If there was no local add/delete of a document for this processor then the push will be
   * skipped.
   */
  public void hardCommitCompletedLocally() {
    finishIndexingBatch();
  }

  protected void finishIndexingBatch() {
    String coreName = core.getName();
    if (IndexingBatchState.NOT_STARTED.equals(state)) {
      // Since we did not added/deleted a single document therefore it is an isolated commit.
      // Few ways isolated commit can manifest:
      // 1. Client issuing a separate commit command after the update command.
      // 2. SolrJ client issuing a separate follow up commit command to affected shards than actual indexing request
      //    even when SolrJ client's caller issued a single update command with commit=true.
      // 3. The replica that received the indexing batch first was either a follower replica or the leader of another 
      //    shard and only did the job of forwarding the docs to their rightful leader. Therefore, at the end it has
      //    nothing to commit.
      // Shared replica has a hard requirement of processing each indexing batch with a hard commit(either explicit or
      // implicit) because that is how, at the end of an indexing batch, synchronous push to shared store gets hold
      // of the segment files on local disk.
      // Therefore, isolated commits are meaningless for SHARED replicas and we can ignore writing to shared store. 
      // If we ever need an isolated commit to write to shared store for some scenario, we should first understand if a
      // pull from shared store was done or not(why not) and push should happen under corePullLock.readLock()
      state = IndexingBatchState.COMMITTED;
      log.info("Isolated commit encountered for a SHARED replica, ignoring writing to shared store." +
          " collection=" + collectionName + " shard=" + shardName + " core=" + coreName);
    } else if (IndexingBatchState.STARTED.equals(state)) {
      if (corePullLock == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "How were we able to start an indexing batch without acquiring a pull read lock?" +
                " collection=" + collectionName + " shard=" + shardName + " core=" + coreName);
      }
      SharedCoreConcurrencyController concurrencyController = core.getCoreContainer().getSharedStoreManager().getSharedCoreConcurrencyController();
      concurrencyController.recordState(collectionName, shardName, coreName, SharedCoreConcurrencyController.SharedCoreStage.LOCAL_INDEXING_FINISHED);
      state = IndexingBatchState.COMMITTED;
      getCorePusher().pushCoreToSharedStore(core, sharedShardName);
    } else if (IndexingBatchState.COMMITTED.equals(state)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Why are we committing an already committed indexing batch?" +
              " collection=" + collectionName + " shard=" + shardName + " core=" + coreName);
    } else {
      throwUnknownStateError();
    }
  }

  private void throwUnknownStateError() {
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Programmer's error, unknown IndexingBatchState" + state
        + " collection=" + collectionName + " shard=" + shardName + " core=" + core.getName());
  }

  @VisibleForTesting
  protected CorePusher getCorePusher() {
    return corePusher;
  }

  @Override
  public void close() {
    if (!IndexingBatchState.NOT_STARTED.equals(state)) {
      try {
        SharedCoreConcurrencyController concurrencyController =
            core.getCoreContainer().getSharedStoreManager().getSharedCoreConcurrencyController();
        concurrencyController.recordState(collectionName, shardName, core.getName(),
            SharedCoreConcurrencyController.SharedCoreStage.INDEXING_BATCH_FINISHED);
      } catch (Exception ex) {
        SolrException.log(log, "Error recording the finish of a SHARED core indexing batch", ex);
      }
    }
    if (corePullLock != null) {
      try {
        // release read lock
        corePullLock.readLock().unlock();
      } catch (Exception ex) {
        SolrException.log(log, "Error releasing pull read lock of a SHARED core", ex);
      }
    }
  }

  private enum IndexingBatchState {
    NOT_STARTED,
    STARTED,
    COMMITTED
  }
}
