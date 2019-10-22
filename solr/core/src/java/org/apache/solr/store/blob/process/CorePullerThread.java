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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Throwables;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.util.DeduplicatingList;
import org.apache.solr.store.shared.SharedCoreConcurrencyController;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreVersionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread (there are a few of these created in {@link CorePullerFeeder#run}) that dequeues {@link CorePullTask} from a
 * {@link DeduplicatingList} and executes them forever or until interrupted (whichever comes first). The
 * {@link DeduplicatingList} is fed by {@link CorePullerFeeder} getting its data from {@link CorePullTracker} via a
 * different {@link DeduplicatingList}.
 */
public class CorePullerThread implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final DeduplicatingList<String, CorePullTask> workQueue;
  private final CorePullerFeeder pullerFeeder;

  CorePullerThread(CorePullerFeeder pullerFeeder, DeduplicatingList<String, CorePullTask> workQueue) {
    this.workQueue = workQueue;
    this.pullerFeeder = pullerFeeder;
  }

  @Override
  public void run() {
    // Thread runs until interrupted (which is the right way to tell a thread to stop running)
    while (true) {
      CorePullTask task = null;
      try {
        // This call blocks if work queue is empty
        task = workQueue.removeFirst();

        CorePullerFeeder.PullCoreInfo info = task.getPullCoreInfo();
        String collectionName = info.getCollectionName();
        String shardName = info.getShardName();
        String coreName = info.getCoreName();
        SharedCoreConcurrencyController concurrencyController = task.getCoreContainer().getSharedStoreManager().getSharedCoreConcurrencyController();

        SharedCoreVersionMetadata coreVersionMetadata = concurrencyController.getCoreVersionMetadata(collectionName, shardName, coreName);
        // TODO: On leaders we can rely on soft guarantee of equality since indexing can correct them if they are wrong
        //       There is a work item to make use of that knowledge and not enqueue pulls on leaders when soft guarantee of equality
        //       is available. But until that work item is done this a stop gap measure to dismiss those unnecessary pulls.
        //       The reason it is important to add this:
        //       - is to prevent those unnecessary pulls to contend on pull write lock and cause any trouble to indexing which needs read lock.
        //       The reason it is considered a stop gap measure:
        //       - we need not to enqueue pull requests to begin with
        //       - this isLeader computation is not complete, it does not handle cases when core is absent
        //       - this isLeader computation might not be performant and efficient. I don't know if caching is involved here or not.
        boolean isLeaderPulling = isLeader(task.getCoreContainer(), collectionName, shardName, coreName);
        if (coreVersionMetadata.isSoftGuaranteeOfEquality() && isLeaderPulling) {
          // already in sync
          task.finishedPull(coreVersionMetadata.getBlobCoreMetadata(), CoreSyncStatus.SUCCESS_EQUIVALENT, null);
          // move to next task
          continue;
        }

        ReentrantReadWriteLock corePullLock = concurrencyController.getCorePullLock(info.getCollectionName(), info.getShardName(), info.getCoreName());
        // Try to acquire write lock, if possible. otherwise, we don't want to hold this background thread
        // because it can be used for other cores in the mean time
        // Avoiding the barging overload #tryLock() to let default fairness scheme play out, although, the way
        // we are using this lock barging overload would not disrupt things that much 
        if (corePullLock.writeLock().tryLock(0, TimeUnit.MILLISECONDS)) {
          try {
            // TODO: we should timebox this request in case we are stuck for long time
            task.pullCoreFromBlob(isLeaderPulling);
          } finally {
            corePullLock.writeLock().unlock();
          }
        } else {
          log.info(String.format("Could not acquire pull write lock, going back to task queue, pullCoreInfo=%s", task.getPullCoreInfo().toString()));
          workQueue.addDeduplicated(task, true);
        }
      } catch (InterruptedException ie) {
        log.info("Puller thread " + Thread.currentThread().getName()
            + " got interrupted. Shutting down Blob CorePullerFeeder if not already.");

        // Stop the puller feeder that will close the other threads and re-interrupt ourselves
        pullerFeeder.close();
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        // Exceptions other than InterruptedException should not stop the business
        String taskInfo = "";
        if (task != null) {
          try {
            taskInfo = String.format("Attempt=%s to pull core %s ", task.getAttempts(), task.getPullCoreInfo().getCoreName());
            task.finishedPull(null, CoreSyncStatus.FAILURE, Throwables.getStackTraceAsString(e));
          } catch (Exception fpe) {
            log.warn("Cleaning up of pull task encountered a failure.", fpe);
          }
        }
        log.warn("CorePullerThread encountered a failure. " + taskInfo, e);
      }
    }
  }

  // TODO: This is temporary, see detailed note where it is consumed above.
  private boolean isLeader(CoreContainer coreContainer, String collectionName, String shardName, String coreName) throws InterruptedException {
    try {
      if (!coreContainer.isZooKeeperAware()) {
        // not solr cloud
        return false;
      }

      CoreDescriptor coreDescriptor = coreContainer.getCoreDescriptor(coreName);
      if (coreDescriptor == null) {
        // core descriptor does not exist
        return false;
      }

      SolrCore core = coreContainer.getCore(coreName);
      if (core != null) {
        core.close();
      } else {
        // core does not exist
        return false;
      }

      CloudDescriptor cd = coreDescriptor.getCloudDescriptor();
      if (cd == null || cd.getReplicaType() != Replica.Type.SHARED) {
        // not a shared replica
        return false;
      }

      ZkController zkController = coreContainer.getZkController();
      Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(cd.getCollectionName(), cd.getShardId());
      if (leaderReplica == null || !cd.getCoreNodeName().equals(leaderReplica.getName())) {
        // not a leader replica
        return false;
      }

      return true;
    } catch (InterruptedException ie) {
      throw ie;
    } catch (Exception ex) {
      log.warn(String.format("Could not establish if current replica is leader for the given core, collection=%s shard=%s core=%s",
          collectionName, shardName, coreName), ex);
      // we will proceed further as we are not a leader
    }
    return false;
  }
}
