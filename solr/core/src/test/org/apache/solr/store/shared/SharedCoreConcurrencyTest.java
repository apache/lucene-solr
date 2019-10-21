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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.process.BlobProcessUtil;
import org.apache.solr.store.blob.process.CorePullTask;
import org.apache.solr.store.blob.process.CorePullerFeeder;
import org.apache.solr.store.blob.process.CoreSyncStatus;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreStage;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests around synchronization of concurrent indexing, pushes and pulls
 * happening on a core of a shared collection {@link DocCollection#getSharedIndex()}
 * todo: add tests for failover scenarios and involve query pulls 
 */
public class SharedCoreConcurrencyTest extends SolrCloudSharedStoreTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static Path sharedStoreRootPath;

  @BeforeClass
  public static void setupClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");
  }

  /**
   * Tests issuing random number of concurrent indexing requests in a given range for a shared core and making sure they all succeed.
   */
  @Test
  public void testHighConcurrentIndexing() throws Exception {
    int maxIndexingThreads = 100;
    int maxDocsPerThread = 100;
    testConcurrentIndexing(maxIndexingThreads, maxDocsPerThread);
  }

  /**
   * Test ensuring two indexing requests interleave in desired ways and succeed.
   */
  //  @Test todo: leaking threads, test only issue
  public void TODOtestPossibleInterleaving() throws Exception {

    // completely serialized
    testConcurrentIndexing(
        new IndexingThreadInterleaver(
            SharedCoreStage.BlobPushFinished,
            SharedCoreStage.LocalIndexingStarted,
            true));

    // second pushes for first one too
    testConcurrentIndexing(
        new IndexingThreadInterleaver(
            SharedCoreStage.LocalIndexingFinished, 
            SharedCoreStage.BlobPushFinished,
            true));
  }

  /**
   * Test ensuring two indexing requests do not interleave in impossible ways even when forced and still succeed.
   */
  //  @Test todo: leaking threads, test only issue
  public void TODOtestImpossibleInterleaving() throws Exception {
    // push critical section
    testConcurrentIndexing(
        new IndexingThreadInterleaver(
            SharedCoreStage.ZkUpdateFinished,
            SharedCoreStage.ZkUpdateFinished,
            false));

    // another push critical section
    testConcurrentIndexing(
        new IndexingThreadInterleaver(
            SharedCoreStage.ZkUpdateFinished,
            SharedCoreStage.LocalCacheUpdateFinished,
            false));
  }

  private void testConcurrentIndexing(int maxIndexingThreads, int maxDocsPerThread) throws Exception {
    int numIndexingThreads = new Random().nextInt(maxIndexingThreads) + 1;;
    testConcurrentIndexing(numIndexingThreads, maxDocsPerThread, null);
  }

  private void testConcurrentIndexing(IndexingThreadInterleaver interleaver) throws Exception {
    testConcurrentIndexing(2, 10, interleaver);
  }

  /**
   * Start desired number of concurrent indexing threads with each indexing random number of docs between 1 and maxDocsPerThread.
   *
   * At the end it verifies everything got indexed successfully on leader. No critical section got breached.
   * Also verify the integrity of shared store contents by pulling them on a follower replica.
   */
  private void testConcurrentIndexing(int numIndexingThreads, int maxDocsPerThread, IndexingThreadInterleaver interleaver) throws Exception {
    setupCluster(2);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // this map tracks the async pull queues per solr process
    Map<String, Map<String, CountDownLatch>> solrProcessesTaskTracker = new HashMap<>();

    JettySolrRunner solrProcess1 = cluster.getJettySolrRunner(0);
    CoreStorageClient storageClient1 = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient1), solrProcess1);
    Map<String, CountDownLatch> asyncPullLatches1 = configureTestBlobProcessForNode(solrProcess1);

    JettySolrRunner solrProcess2 = cluster.getJettySolrRunner(1);
    CoreStorageClient storageClient2 = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient2), solrProcess2);
    Map<String, CountDownLatch> asyncPullLatches2 = configureTestBlobProcessForNode(solrProcess2);

    solrProcessesTaskTracker.put(solrProcess1.getNodeName(), asyncPullLatches1);
    solrProcessesTaskTracker.put(solrProcess2.getNodeName(), asyncPullLatches2);

    String collectionName = "sharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 2;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);

    // get the leader replica and follower replicas
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");
    Replica followerReplica = null;
    for (Replica repl : collection.getSlice("shard1").getReplicas()) {
      if (repl.getName() != shardLeaderReplica.getName()) {
        followerReplica = repl;
        break;
      }
    }

    
   JettySolrRunner shardLeaderSolrRunner = null;
   for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
     if(solrRunner.getNodeName().equals(shardLeaderReplica.getNodeName())){
       shardLeaderSolrRunner = solrRunner;
       break;
     }
   }
   List<String> progress = new ArrayList<>();

   if(interleaver != null) {
     configureTestSharedConcurrencyControllerForNode(shardLeaderSolrRunner, progress, interleaver);
   } else {
     configureTestSharedConcurrencyControllerForNode(shardLeaderSolrRunner, progress);
   }

    AtomicInteger totalDocs= new AtomicInteger(0);
    log.info("numIndexingThreads=" + numIndexingThreads);
    Thread[] indexingThreads = new Thread[numIndexingThreads];
    ConcurrentLinkedQueue<String> indexingErrors = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < numIndexingThreads; i++) {
      indexingThreads[i] = new Thread(() -> {
        try {
          // index between 1 to maxDocsPerThread docs
          int numDocs = new Random().nextInt(maxDocsPerThread) + 1;
          log.info("numDocs=" + numDocs);
          UpdateRequest updateReq = new UpdateRequest();
          for (int j = 0; j < numDocs; j++) {
            int docId = totalDocs.incrementAndGet();
            updateReq.add("id", Integer.toString(docId));
          }
          updateReq.commit(cloudClient, collectionName);
        } catch (Exception ex) {
          indexingErrors.add(ex.getMessage());
        }
      });
    }

    for (int i = 0; i < numIndexingThreads; i++) {
      indexingThreads[i].start();
    }

    for (int i = 0; i < numIndexingThreads; i++) {
      indexingThreads[i].join();
    }

    log.info("totalDocs=" + totalDocs.intValue());

    assertTrue(indexingErrors.toString(), indexingErrors.isEmpty());

    if(interleaver != null) {
      assertNull(interleaver.error, interleaver.error);
    }

    assertFalse("no progress recorded", progress.isEmpty());

    log.info(progress.toString());

    assertCriticalSections(progress);

    // verify the update wasn't forwarded to the follower and it didn't commit by checking the core
    // this gives us confidence that the subsequent query we do triggers the pull
    CoreContainer replicaCC = getCoreContainer(followerReplica.getNodeName());
    SolrCore core = null;
    SolrClient followerDirectClient = null;
    SolrClient leaderDirectClient = null;
    try {
      core = replicaCC.getCore(followerReplica.getCoreName());
      // the follower should only have the default segments file
      assertEquals(1, core.getDeletionPolicy().getLatestCommit().getFileNames().size());

      // query the leader directly to verify it should have the document
      leaderDirectClient = getHttpSolrClient(shardLeaderReplica.getBaseUrl() + "/" + shardLeaderReplica.getCoreName());
      ModifiableSolrParams params = new ModifiableSolrParams();
      params
          .set("q", "*:*")
          .set("distrib", "false");
      QueryResponse resp = leaderDirectClient.query(params);
      assertEquals(totalDocs.intValue(), resp.getResults().getNumFound());

      // we want to wait until the pull completes so set up a count down latch for the follower's
      // core that we'll wait until pull finishes for
      CountDownLatch latch = new CountDownLatch(1);
      Map<String, CountDownLatch> asyncPullTasks = solrProcessesTaskTracker.get(followerReplica.getNodeName());
      asyncPullTasks.put(followerReplica.getCoreName(), latch);

      // query the follower directly to trigger the pull, this query should yield no results
      // as it returns immediately 
      followerDirectClient = getHttpSolrClient(followerReplica.getBaseUrl() + "/" + followerReplica.getCoreName());
      resp = followerDirectClient.query(params);
      assertEquals(0, resp.getResults().getNumFound());

      // wait until pull is finished
      assertTrue(latch.await(120, TimeUnit.SECONDS));

      // do another query to verify we've pulled everything
      resp = followerDirectClient.query(params);

      // verify we pulled
      assertTrue(core.getDeletionPolicy().getLatestCommit().getFileNames().size() > 1);

      // verify the document is present
      assertEquals(totalDocs.intValue(), resp.getResults().getNumFound());
    } finally {
      if (leaderDirectClient != null) {
        leaderDirectClient.close();
      }
      if (followerDirectClient != null) {
        followerDirectClient.close();
      }
      if (core != null) {
        core.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
      // clean up the shared store. The temp dir should clean up itself after the test class finishes
      FileUtils.cleanDirectory(sharedStoreRootPath.toFile());
    }
  }

  private void assertCriticalSections(List<String> progress) {
    String currentThreadId = null;
    SharedCoreStage currentStage = null;
    String prevThreadId = null;
    SharedCoreStage prevStage = null;
    int activeIndexers = 0; // number of threads that have started indexing and not finished 
    int activeBlobPushers = 0; // number of threads that are actively pushing at any given time
    for (String p : progress) {
      String[] parts = p.split("\\.");
      currentThreadId = parts[0];
      currentStage = SharedCoreStage.valueOf(parts[1]);
      if (currentStage == SharedCoreStage.LocalIndexingStarted) {
        activeIndexers++;
      } else if (currentStage == SharedCoreStage.BlobPushStarted) {
        activeBlobPushers++;
      } else if (currentStage == SharedCoreStage.BlobPushFinished) {
        // both blob pushing and indexing finish at this stage 
        activeBlobPushers--;
        activeIndexers--;
      }

      // making sure no other activity takes place during pull
      if (prevStage == SharedCoreStage.BlobPullStarted) {
        assertTrue("Pull critical section breached, currentStage=" + p, prevThreadId==currentThreadId && currentStage == SharedCoreStage.BlobPullFinished);
      }

      // making sure indexing is not disrupted by a pull from blob
      assertFalse("Indexing breached by a pull, currentStage=" + p,
          activeIndexers > 0 &&
              (currentStage == SharedCoreStage.BlobPullStarted || currentStage == SharedCoreStage.BlobPullFinished));

      // making sure push to blob are not disrupted by another push to blob
      assertFalse("Blob push breached by another blob push,  currentStage=" + p, activeBlobPushers > 1);

      prevThreadId = currentThreadId;
      prevStage = currentStage;
    }
  }

  private Map<String, CountDownLatch> configureTestBlobProcessForNode(JettySolrRunner runner) {
    Map<String, CountDownLatch> asyncPullTracker = new HashMap<>();

    CorePullerFeeder cpf = new CorePullerFeeder(runner.getCoreContainer()) {
      @Override
      protected CorePullTask.PullCoreCallback getCorePullTaskCallback() {
        return new CorePullTask.PullCoreCallback() {
          @Override
          public void finishedPull(CorePullTask pullTask, BlobCoreMetadata blobMetadata, CoreSyncStatus status,
                                   String message) throws InterruptedException {
            CountDownLatch latch = asyncPullTracker.get(pullTask.getPullCoreInfo().getCoreName());
            if(latch != null) {
              latch.countDown();
            }
          }
        };
      }
    };

    BlobProcessUtil testUtil = new BlobProcessUtil(runner.getCoreContainer(), cpf);
    setupTestBlobProcessUtilForNode(testUtil, runner);
    return asyncPullTracker;
  }

  private void configureTestSharedConcurrencyControllerForNode(JettySolrRunner runner, List<String> progress) {
    SharedCoreConcurrencyController concurrencyController = new SharedCoreConcurrencyController(runner.getCoreContainer()) {
      Object recorderLock = new Object();
      @Override
      public void recordState(String collectionName, String shardName, String coreName, SharedCoreStage stage) {
        super.recordState(collectionName, shardName, coreName, stage);

        synchronized (recorderLock) {
          progress.add(Thread.currentThread().getId() + "." + stage.name());
        }

      }
    };
    setupTestSharedConcurrencyControllerForNode(concurrencyController, runner);
  }

  private void configureTestSharedConcurrencyControllerForNode(JettySolrRunner runner, List<String> progress, IndexingThreadInterleaver interleaver) {
    SharedCoreConcurrencyController concurrencyController = new SharedCoreConcurrencyController(runner.getCoreContainer()) {
      Object recorderLock = new Object();
      long firstThreadId = -1;
      long secondThreadId = -1;
      boolean firstAchieved = false;
      boolean secondAchieved = false;
      boolean secondAlreadyWaitingToStart = false;
      CountDownLatch secondWaitingToStart = new CountDownLatch(1);
      CountDownLatch firstWaitingForSecondToCompleteItsDesiredState = new CountDownLatch(1);
      int timeoutSeconds = 10;
      @Override
      public void recordState(String collectionName, String shardName, String coreName, SharedCoreStage stage) {
        super.recordState(collectionName, shardName, coreName, stage);

        CountDownLatch latchToWait = null;
        boolean latchShouldSucceed = true;
        String succeedError = null;
        String failureError = null;
        String interruptionError = null;
        // compute stalling decision under synchronized block and then stall(if needed) outside of synchronized block
        synchronized (recorderLock) {
          progress.add(Thread.currentThread().getId() + "." + stage.name());

          if (interleaver.error != null) {
            // already errored out
            return;
          }

          long currentThreadId = Thread.currentThread().getId();
          if (stage == SharedCoreStage.IndexingBatchReceived) {
            // identify first and second thread and initialize their thread ids
            // first is whichever comes first
            if (firstThreadId == -1) {
              firstThreadId = currentThreadId;
            } else {
              if(secondThreadId != -1) {
                interleaver.error = "why there is a third indexing thread?";
                return;
              }
              secondThreadId = currentThreadId;
            }
          }

          if (firstAchieved && secondAchieved) {
            // nothing left
            return;
          } else if (!secondAlreadyWaitingToStart && currentThreadId == secondThreadId) {
            secondAlreadyWaitingToStart = true;
            latchToWait = secondWaitingToStart;
            latchShouldSucceed = true;
            failureError = "second failed to wait for first to reach at desired state";
            interruptionError = "second was interrupted while waiting for first to reach at desired state";
          } else if (currentThreadId == firstThreadId && stage == interleaver.firstStage) {
            // first reached to desired stage, now release second
            secondWaitingToStart.countDown();
            latchToWait = firstWaitingForSecondToCompleteItsDesiredState;
            latchShouldSucceed = interleaver.isPossible;
            succeedError = "first was able to wait for second to reach at desired state, even when it was not possible";
            failureError = "first failed to wait for second to reach at desired state";
            interruptionError = "first was interrupted while waiting for second to reach at desired state";
            firstAchieved = true;
          } else if (currentThreadId == secondThreadId && stage == interleaver.secondStage) {
            // second also reached desired state, we are done
            if (!firstAchieved) {
              interleaver.error = "how come second reached its desired without first reaching its";
            }
            firstWaitingForSecondToCompleteItsDesiredState.countDown();
            secondAchieved = true;
          }
        }

        if (latchToWait != null) {
          // need to stall this stage on the provided latch
          try {
            boolean successfulWait = latchToWait.await(timeoutSeconds, TimeUnit.SECONDS);
            if (successfulWait && !latchShouldSucceed) {
              interleaver.error = succeedError;
            } else if (!successfulWait && latchShouldSucceed) {
              interleaver.error = failureError;
            }
          } catch (InterruptedException iex) {
            interleaver.error = interruptionError;
          }
        }
      }
    };
    setupTestSharedConcurrencyControllerForNode(concurrencyController, runner);
  }

  /**
   * 1. Second thread is stalled after {@link SharedCoreStage#IndexingBatchReceived} 
   * 2. First thread is run up to firstStage then stalled
   * 3. Second thread is resumed and run up to secondStage
   * 4. First thread is resumed
   */
  private static class IndexingThreadInterleaver {
    private final SharedCoreStage firstStage;
    /**
     * Let second indexing thread run up to this stage before resuming stalled first thread
     */
    private final SharedCoreStage secondStage;
    private final boolean isPossible;
    private String error = null;

    private IndexingThreadInterleaver(SharedCoreStage firstStage,
                                      SharedCoreStage secondStage,
                                      boolean isPossible) {
      this.firstStage = firstStage;
      this.secondStage = secondStage;
      this.isPossible = isPossible;
    }
  }
}
