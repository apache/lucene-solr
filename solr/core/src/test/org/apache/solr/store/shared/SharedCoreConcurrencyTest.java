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

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.shared.SharedCoreConcurrencyController.SharedCoreStage;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests around synchronization of concurrent indexing, pushes and pulls
 * happening on a core of a shared collection {@link DocCollection#getSharedIndex()}
 */
public class SharedCoreConcurrencyTest extends SolrCloudSharedStoreTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION_NAME = "sharedCollection";
  private static final String SHARD_NAME = "shard1";
  /**
   * Number of serial indexing iterations for each test. This is the main setting, queries and failover iterations
   * stop after indexing ends. Higher the value, longer the tests would run.
   */
  private static int INDEXING_ITERATIONS = TEST_NIGHTLY ? 100 : 20;
  /**
   * Maximum number of concurrent indexing requests per indexing iteration.
   */
  private static int MAX_NUM_OF_CONCURRENT_INDEXING_REQUESTS_PER_ITERATION = 10;
  /**
   * Maximum number of docs per indexing request.
   */
  private static int MAX_NUM_OF_DOCS_PER_INDEXING_REQUEST = 100;
  /**
   * Indexing can fail because of leader failures (especially when test {@link #includeFailovers()}).
   * The test will re-attempt up till this number of times before bailing out. For test to succeed,
   * indexing request have to succeed in these many attempts.
   */
  private static int MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST = 10;
  /**
   * Maximum number of concurrent query requests per query iteration.
   */
  private static int MAX_NUM_OF_CONCURRENT_QUERY_REQUESTS_PER_ITERATION = 10;
  /**
   * Indexing is faster than indexing, to pace it better with indexing, a delay is added between each query iteration.
   */
  private static int DELAY_MS_BETWEEN_EACH_QUERY_ITERATION = 50;
  /**
   * Minimum time between each failover.
   */
  private static int DELAY_MS_BETWEEN_EACH_FAILOVER_ITERATION = 500;
  /**
   * Path for local shared store
   */
  private static Path sharedStoreRootPath;

  /**
   * Manages test state from start to end.
   */
  private TestState testState;

  @BeforeClass
  public static void setupClass() throws Exception {
    sharedStoreRootPath = createTempDir("tempDir");
  }

  @Before
  public void setupTest() throws Exception {
    int numNodes = 4;
    setupCluster(numNodes);
    testState = new TestState();
    setupSolrNodesForTest();

    int maxShardsPerNode = 1;
    // One less than number of nodes.
    // The extra node will be used at the end of test to verify 
    // the contents of shared store by querying for all docs on a new replica.
    int numReplicas = numNodes - 1;
    // Later on we can consider choosing random number of shards and replicas.
    // To handle multiple shards, we need to update code where SHARD_NAME is used.
    setupSharedCollectionWithShardNames(COLLECTION_NAME, maxShardsPerNode, numReplicas, SHARD_NAME);
  }

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtils.cleanDirectory(sharedStoreRootPath.toFile());
  }

  /**
   * Tests that concurrent indexing succeed.
   */
  @Test
  public void testIndexing() throws Exception {
    final boolean includeDeletes = false;
    includeIndexing(includeDeletes);
    run();
  }

  /**
   * Tests that concurrent indexing with concurrent queries succeed.
   */
  @Test
  public void testIndexingQueries() throws Exception {
    final boolean includeDeletes = false;
    includeIndexing(includeDeletes);
    includeQueries();
    run();
  }

  /**
   * Tests that concurrent indexing with deletes and concurrent queries succeed.
   */
  @Test
  public void testIndexingQueriesDeletes() throws Exception {
    final boolean includeDeletes = true;
    includeIndexing(includeDeletes);
    includeQueries();
    run();
  }

  /**
   * Tests that concurrent indexing with deletes, concurrent queries and explicit failovers succeed.
   */
  // @Test 
  // TODO: This test flaps time to time. The symptom of the failure is missing docs i.e. indexing is declared successful
  //       but query could not reproduce all of the docs. I was able to repro this with NRT collection on vanilla 8.3 too. 
  //       I have not root caused it yet. Keeping this test disabled, until the problem is root caused and fixed. 
  public void todoTestIndexingQueriesDeletesFailovers() throws Exception { 
    final boolean includeDeletes = true;
    includeIndexing(includeDeletes);
    includeQueries();
    includeFailovers();
    run();
  }

  /**
   * It starts all the threads that are included in the test (indexing, queries and failovers) in parallel.
   * Then wait for them to finish (run length depends on {@link #INDEXING_ITERATIONS}).
   * At the end it makes sures that no critical section was breached and no unexpected error occurred.
   * Then verify the contents of shared store by querying for all docs on a new replica.
   */
  private void run() throws Exception {
    testState.startIncludedThreads();
    testState.waitForThreadsToStop();
    analyzeCoreConcurrencyStagesForBreaches();
    testState.checkErrors();
    Replica newReplica = addReplica();
    queryNewReplicaAndVerifyAllDocsFound(newReplica);
  }

  /**
   * Adds a thread to test, that goes over {@link #INDEXING_ITERATIONS} or until it is interrupted.
   * In each iteration it creates between 1 and {@link #MAX_NUM_OF_CONCURRENT_INDEXING_REQUESTS_PER_ITERATION} threads
   * by calling {@link #createIndexingThreads(int, int, boolean)}, starts them concurrently and wait for them to finish
   * before going to next iteration. Each indexing thread adds between 1 and  {@link #MAX_NUM_OF_DOCS_PER_INDEXING_REQUEST}
   * docs.
   *
   * @param includeDeletes whether to randomly mark some docs for deletion and delete them in subsequent indexing requests
   *                       or not
   */
  private void includeIndexing(boolean includeDeletes) {
    Thread t = new Thread(() -> {
      try {
        for (int i = 0; i < INDEXING_ITERATIONS && !testState.stopRunning.get(); i++) {
          int numIndexingThreads = random().nextInt(MAX_NUM_OF_CONCURRENT_INDEXING_REQUESTS_PER_ITERATION) + 1;
          int numDocsToAddPerThread = random().nextInt(MAX_NUM_OF_DOCS_PER_INDEXING_REQUEST) + 1;
          Thread[] indexingThreads = createIndexingThreads(numIndexingThreads, numDocsToAddPerThread, includeDeletes);
          for (int j = 0; j < numIndexingThreads; j++) {
            indexingThreads[j].start();
          }
          for (int j = 0; j < numIndexingThreads; j++) {
            indexingThreads[j].join();
          }
          if (Thread.interrupted()) {
            // we have been interrupted so we will stop running
            testState.stopRunning.set(true);
          }
        }
      } catch (Exception ex) {
        testState.indexingErrors.add(ex.getMessage());
      }
      // everything else stops running when indexing finishes
      testState.stopRunning.set(true);
    });
    testState.includeThread(t);
  }

  /**
   * Creates {@code numIndexingThreads} threads with each adding {@code numDocsToAddPerThread}.
   *
   * @param includeDeletes whether to randomly mark some docs for deletion and delete them in subsequent indexing requests
   *                       or not
   */
  private Thread[] createIndexingThreads(int numIndexingThreads, int numDocsToAddPerThread, boolean includeDeletes) throws Exception {
    log.info("numIndexingThreads=" + numIndexingThreads);
    Thread[] indexingThreads = new Thread[numIndexingThreads];
    for (int i = 0; i < numIndexingThreads && !testState.stopRunning.get(); i++) {
      indexingThreads[i] = new Thread(() -> {
        List<String> idsToAdd = new ArrayList<>();
        // prepare the list of docs to add and delete outside the reattempt loop
        for (int j = 0; j < numDocsToAddPerThread; j++) {
          String docId = Integer.toString(testState.docIdGenerator.incrementAndGet());
          idsToAdd.add(docId);
        }
        List<String> idsToDelete = testState.idBatchesToDelete.poll();

        // attempt until succeeded or max attempts 
        for (int j = 0; j < MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST; j++) {
          try {
            String message = "attempt=" + (j + 1) + " numDocsToAdd=" + numDocsToAddPerThread + " docsToAdd=" + idsToAdd.toString();
            if (idsToDelete != null) {
              message += " numDocsToDelete=" + idsToDelete.size() + " docsToDelete=" + idsToDelete.toString();
            }
            log.info(message);

            UpdateRequest updateReq = new UpdateRequest();
            for (int k = 0; k < idsToAdd.size(); k++) {
              updateReq.add("id", idsToAdd.get(k));
            }
            if (includeDeletes && idsToDelete != null) {
              updateReq.deleteById(idsToDelete);
            }
            processUpdateRequest(updateReq);

            testState.numDocsIndexed.addAndGet(numDocsToAddPerThread);
            if (idsToDelete != null) {
              testState.idsDeleted.addAll(idsToDelete);
            }

            // randomly select some docs that can be deleted
            if (includeDeletes) {
              List<String> idsThatCanBeDeleted = new ArrayList<>();
              for (String indexedId : idsToAdd) {
                if (random().nextBoolean()) {
                  idsThatCanBeDeleted.add(indexedId);
                }
              }
              if (!idsThatCanBeDeleted.isEmpty()) {
                testState.idBatchesToDelete.offer(idsThatCanBeDeleted);
              }
            }
            // indexing was successful, stop attempting
            break;
          } catch (Exception ex) {
            // last attempt also failed, record the error
            if (j == MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST - 1) {
              testState.indexingErrors.add(Throwables.getStackTraceAsString(ex));
            }
          }
        }
      });
    }
    return indexingThreads;
  }

  /**
   * Sends update request to the server, randomly choosing whether to send it with commit=true or not
   */
  private void processUpdateRequest(UpdateRequest request) throws Exception {
    UpdateResponse response = random().nextBoolean()
        ? request.process(cluster.getSolrClient(), COLLECTION_NAME)
        : request.commit(cluster.getSolrClient(), COLLECTION_NAME);

    if (response.getStatus() != 0) {
      throw new RuntimeException("Update request failed with status=" + response.getStatus());
    }
  }

  /**
   * Adds a thread to test, that goes over iterations until the test is stopped {@link TestState#stopRunning}.
   * In each iteration it creates between 1 and {@link #MAX_NUM_OF_CONCURRENT_QUERY_REQUESTS_PER_ITERATION} threads
   * by calling {@link #createQueryThreads(int)}, starts them concurrently and wait for them to finish
   * before going to next iteration. To pace it better with indexing, {@link #DELAY_MS_BETWEEN_EACH_QUERY_ITERATION}
   * delay is added between each query iteration.
   */
  private void includeQueries() throws Exception {
    Thread t = new Thread(() -> {
      try {
        while (!testState.stopRunning.get()) {
          int numQueryThreads = random().nextInt(MAX_NUM_OF_CONCURRENT_QUERY_REQUESTS_PER_ITERATION) + 1;
          Thread[] indexingThreads = createQueryThreads(numQueryThreads);
          for (int j = 0; j < numQueryThreads; j++) {
            indexingThreads[j].start();
          }
          for (int j = 0; j < numQueryThreads; j++) {
            indexingThreads[j].join();
          }
          Thread.sleep(DELAY_MS_BETWEEN_EACH_QUERY_ITERATION);
        }
      } catch (Exception ex) {
        testState.queryErrors.add(ex.getMessage());
      }
    });
    testState.includeThread(t);
  }

  /**
   * Creates {@code numQueryThreads} threads with each querying all docs "*:*"
   */
  private Thread[] createQueryThreads(int numQueryThreads) throws Exception {
    log.info("numQueryThreads=" + numQueryThreads);
    Thread[] queryThreads = new Thread[numQueryThreads];
    for (int i = 0; i < numQueryThreads && !testState.stopRunning.get(); i++) {
      queryThreads[i] = new Thread(() -> {
        try {
          /** 
           * Don't have a way to ensure freshness of results yet. When we add something for query freshness later 
           * we may use that here.
           *
           * {@link SolrProcessTracker#corePullTracker} cannot help in concurrent query scenarios since there
           * is no one-to-one guarantee between query and an async pull.
           */
          cluster.getSolrClient().query(COLLECTION_NAME, new ModifiableSolrParams().set("q", "*:*"));
        } catch (Exception ex) {
          testState.queryErrors.add(Throwables.getStackTraceAsString(ex));
        }
      });
    }
    return queryThreads;
  }

  /**
   * Adds a thread to test, that goes over iterations until the test is stopped {@link TestState#stopRunning}.
   * In each iteration it failovers to new leader by calling {@link #failOver()}. It waits
   * for {@link #DELAY_MS_BETWEEN_EACH_FAILOVER_ITERATION} between each iteration.
   */
  private void includeFailovers() throws Exception {
    Thread t = new Thread(() -> {
      try {
        while (!testState.stopRunning.get()) {
          failOver();
          Thread.sleep(DELAY_MS_BETWEEN_EACH_FAILOVER_ITERATION);
        }
      } catch (Exception ex) {
        testState.failoverError = Throwables.getStackTraceAsString(ex);
      }
    });
    testState.includeThread(t);
  }

  /**
   * Kills the current leader and waits for the new leader to be selected and then brings back up the killed leader
   * as a follower replica. Before bringing back up the replica it randomly decides to delete its core directory.
   */
  private void failOver() throws Exception {
    DocCollection collection = getCollection();
    Replica leaderReplicaBeforeSwitch = collection.getLeader(SHARD_NAME);
    final String leaderReplicaNameBeforeSwitch = leaderReplicaBeforeSwitch.getName();
    JettySolrRunner shardLeaderSolrRunnerBeforeSwitch = cluster.getReplicaJetty(leaderReplicaBeforeSwitch);
    File leaderIndexDirBeforeSwitch = new File(shardLeaderSolrRunnerBeforeSwitch.getCoreContainer().getCoreRootDirectory()
        + "/" + leaderReplicaBeforeSwitch.getCoreName());

    shardLeaderSolrRunnerBeforeSwitch.stop();
    cluster.waitForJettyToStop(shardLeaderSolrRunnerBeforeSwitch);
    waitForState("Timed out waiting for new replica to become leader", COLLECTION_NAME, (liveNodes, collectionState) -> {
      Slice slice = collectionState.getSlice(SHARD_NAME);
      if (slice.getLeader() == null) {
        return false;
      }
      if (slice.getLeader().getName().equals(leaderReplicaNameBeforeSwitch)) {
        return false;
      }

      return true;
    });

    if (random().nextBoolean()) {
      FileUtils.deleteDirectory(leaderIndexDirBeforeSwitch);
    }

    shardLeaderSolrRunnerBeforeSwitch.start();
    cluster.waitForNode(shardLeaderSolrRunnerBeforeSwitch, -1);

    waitForState("Timed out waiting for restarted replica to become active", COLLECTION_NAME, (liveNodes, collectionState) -> {
      Slice slice = collectionState.getSlice(SHARD_NAME);
      if (slice.getReplica(leaderReplicaNameBeforeSwitch).getState() != Replica.State.ACTIVE) {
        return false;
      }
      return true;
    });

    setupSolrProcess(shardLeaderSolrRunnerBeforeSwitch);
  }

  /**
   * Goes over all the lives of a node(node gets a new life on restart) and then goes over each core's concurrency stages
   * in each life. Logs the concurrency stages in the order they occurred and then analyze those stages to make sure no
   * critical section was breached.
   */
  private void analyzeCoreConcurrencyStagesForBreaches() {
    // Goes over each node
    for (Map.Entry<String, List<SolrProcessTracker>> nodeTracker :
        testState.solrNodesTracker.entrySet()) {
      String nodeName = nodeTracker.getKey();
      int lifeCountForNode = nodeTracker.getValue().size();
      // Goes over each life of a node
      for (int i = 0; i < lifeCountForNode; i++) {
        ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStageTracker = nodeTracker.getValue().get(i).coreConcurrencyStageTracker;
        if (coreConcurrencyStageTracker.isEmpty()) {
          log.info("life " + (i + 1) + "/" + lifeCountForNode + " of node " + nodeName + " is empty");
        } else {
          // Goes over each core
          for (Map.Entry<String, ConcurrentLinkedQueue<String>> coreConcurrencyStagesEntry : coreConcurrencyStageTracker.entrySet()) {
            String coreName = coreConcurrencyStagesEntry.getKey();
            List<String> coreConcurrencyStages = new ArrayList<>(coreConcurrencyStagesEntry.getValue());
            // Log line is truncated beyond certain length, therefore, printing them in the batches of 200
            List<List<String>> batches = Lists.partition(coreConcurrencyStages, 200);
            if (batches.isEmpty()) {
              batches = new ArrayList<>(1);
              batches.add(new ArrayList<>(0));
            }
            for (int j = 0; j < batches.size(); j++) {
              log.info("batch " + (j + 1) + "/" + batches.size()
                  + " of core " + coreName
                  + " of life " + (i + 1) + "/" + lifeCountForNode
                  + " of node " + nodeName
                  + "\n" + batches.get(j).toString());
            }
            analyzeCoreConcurrencyStagesForBreaches(coreName, coreConcurrencyStages);
          }
        }
      }
    }
  }

  /**
   * Analyze core's concurrency stages to make sure no critical section was breached.
   */
  private void analyzeCoreConcurrencyStagesForBreaches(String coreName, List<String> coreConcurrencyStages) {
    SharedCoreStage currentStage = null;
    int activePullers = 0; // number of threads that have started pulling and not finished
    int activeIndexers = 0; // number of threads that have started indexing and not finished 
    int activePushers = 0; // number of threads that are actively pushing at any given time
    for (String s : coreConcurrencyStages) {
      String[] parts = s.split("\\.");
      currentStage = SharedCoreStage.valueOf(parts[1]);

      if (currentStage == SharedCoreStage.BLOB_PULL_STARTED) {
        activePullers++;
      } else if (currentStage == SharedCoreStage.BLOB_PULL_FINISHED) {
        activePullers--;
      } else if (currentStage == SharedCoreStage.LOCAL_INDEXING_STARTED) {
        activeIndexers++;
      } else if (currentStage == SharedCoreStage.BLOB_PUSH_STARTED) {
        activePushers++;
      } else if (currentStage == SharedCoreStage.BLOB_PUSH_FINISHED) {
        activePushers--;
      } else if (currentStage == SharedCoreStage.INDEXING_BATCH_FINISHED) {
        activeIndexers--;
      }

      // making sure no other activity (including another pull) takes place during pull
      assertFalse("Pull and indexing are interleaved, coreName=" + coreName + " currentStage=" + s, activePullers > 1 || (activePullers > 0 && (activeIndexers > 0 || activePushers > 0)));

      // making sure push to blob are not disrupted by another push to blob
      assertFalse("Blob push breached by another blob push, coreName=" + coreName + " currentStage=" + s, activePushers > 1);
    }
  }

  /**
   * Adds a new replica.
   */
  private Replica addReplica() throws Exception {
    List<String> existingReplicas = getCollection().getSlice(SHARD_NAME).getReplicas().stream().map(r -> r.getName()).collect(Collectors.toList());
    // add another replica
    assertTrue(CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, SHARD_NAME, Replica.Type.SHARED)
        .process(cluster.getSolrClient()).isSuccess());
    // Verify that new replica is created
    waitForState("Timed-out waiting for new replica to be created", COLLECTION_NAME, clusterShape(1, existingReplicas.size() + 1));

    Replica newReplica = null;

    for (Replica r : getCollection().getSlice(SHARD_NAME).getReplicas()) {
      if (!existingReplicas.contains(r.getName())) {
        newReplica = r;
        break;
      }
    }

    assertNotNull("Could not find new replica", newReplica);

    return newReplica;
  }

  /**
   * Directly query the {@code replica} and verify that all the docs indexed(after accounting for deletions) are found.
   */
  private void queryNewReplicaAndVerifyAllDocsFound(Replica replica) throws Exception {
    try (SolrClient replicaDirectClient = getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName())) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("q", "*:*").set("distrib", "false").set("rows", testState.numDocsIndexed.get());
      CountDownLatch latch = new CountDownLatch(1);
      Map<String, CountDownLatch> corePullTracker = testState.getCorePullTracker(replica.getNodeName());
      corePullTracker.put(replica.getCoreName(), latch);
      QueryResponse resp = replicaDirectClient.query(params);
      assertEquals("new replica did not return empty results", 0, resp.getResults().getNumFound());

      assertTrue(latch.await(120, TimeUnit.SECONDS));

      resp = replicaDirectClient.query(params);
      List<String> docs = resp.getResults().stream().map(r -> (String) r.getFieldValue("id")).collect(Collectors.toList());
      assertEquals("we did not ask for all the docs found", resp.getResults().getNumFound(), docs.size());
      Collections.sort(docs, new Comparator<String>() {
        public int compare(String id1, String id2) {
          return Integer.parseInt(id1) - Integer.parseInt(id2);
        }
      });
      List<String> docsExpected = new ArrayList<>();
      for (int i = 1; i <= testState.numDocsIndexed.get(); i++) {
        String docId = Integer.toString(i);
        if (!testState.idsDeleted.contains(docId)) {
          docsExpected.add(docId);
        }
      }
      log.info("numDocsFound=" + docs.size() + " docsFound= " + docs.toString());
      assertEquals("wrong docs", docsExpected.size() + docsExpected.toString(), docs.size() + docs.toString());
    }
  }

  /**
   * Setup all the nodes for test.
   */
  private void setupSolrNodesForTest() throws Exception {
    for (JettySolrRunner solrProcess : cluster.getJettySolrRunners()) {
      setupSolrProcess(solrProcess);
    }
  }

  /**
   * Setup solr process for test(process is one life of a node, restarts starts a new life).
   */
  private void setupSolrProcess(JettySolrRunner solrProcess) throws Exception {
    CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), solrProcess);
    Map<String, CountDownLatch> corePullTracker = configureTestBlobProcessForNode(solrProcess);
    ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStagesTracker = new ConcurrentHashMap<>();
    configureTestSharedConcurrencyControllerForProcess(solrProcess, coreConcurrencyStagesTracker);

    SolrProcessTracker processTracker = new SolrProcessTracker(corePullTracker, coreConcurrencyStagesTracker);
    List<SolrProcessTracker> nodeTracker = testState.solrNodesTracker.computeIfAbsent(solrProcess.getNodeName(), k -> new ArrayList<>());
    nodeTracker.add(processTracker);
  }

  private DocCollection getCollection() {
    return cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
  }

  /**
   * Setup {@link SharedCoreConcurrencyController} for the solr process so we can accumulate concurrency stages a core
   * goes through during test.
   */
  private void configureTestSharedConcurrencyControllerForProcess(
      JettySolrRunner solrProcess, ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStagesMap) {
    SharedCoreConcurrencyController concurrencyController = new SharedCoreConcurrencyController(solrProcess.getCoreContainer()) {
      @Override
      public void recordState(String collectionName, String shardName, String coreName, SharedCoreStage stage) {
        super.recordState(collectionName, shardName, coreName, stage);
        ConcurrentLinkedQueue<String> coreConcurrencyStages = coreConcurrencyStagesMap.computeIfAbsent(coreName, k -> new ConcurrentLinkedQueue<>());
        coreConcurrencyStages.add(Thread.currentThread().getId() + "." + stage.name());
      }
    };
    setupTestSharedConcurrencyControllerForNode(concurrencyController, solrProcess);
  }

  /**
   * Manages state for each test from start to end.
   */
  private static class TestState {
    /**
     * Threads included in the test (indexing, queries and failovers).
     */
    private final List<Thread> includedThreads = new ArrayList<>();
    /**
     * Indicator when to stop. It is set to true when either indexing is done or interrupted.
     */
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);
    /**
     * Used to provide unique id to each indexing doc.
     */
    private final AtomicInteger docIdGenerator = new AtomicInteger(0);
    /**
     * At any given moment how many minimum number of docs that have been indexed (it does not account for deletion)
     */
    private final AtomicInteger numDocsIndexed = new AtomicInteger(0);
    /**
     * Set of ids from indexed docs that can be deleted.
     */
    private final ConcurrentLinkedQueue<List<String>> idBatchesToDelete = new ConcurrentLinkedQueue<>();
    /**
     * Ids that have been deleted.
     */
    private final ConcurrentLinkedQueue<String> idsDeleted = new ConcurrentLinkedQueue<>();
    /**
     * Error setting up indexing or those encountered by indexing on the
     * last attempt {@link #MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST} of each batch.
     */
    private final ConcurrentLinkedQueue<String> indexingErrors = new ConcurrentLinkedQueue<>();
    /**
     * Error setting up queries or those encountered by queries.
     */
    private final ConcurrentLinkedQueue<String> queryErrors = new ConcurrentLinkedQueue<>();
    /**
     * Error encountered when failing over to a new leader.
     */
    private String failoverError = null;
    /**
     * Tracks the cores' pull and concurrency stage information for each life of a node (node gets a new life on restart).
     * Key is the node name.
     */
    private final Map<String, List<SolrProcessTracker>> solrNodesTracker = new HashMap<>();

    /**
     * Gets the core pull tracker for current life of the node.
     */
    private Map<String, CountDownLatch> getCorePullTracker(String nodeName) {
      List<SolrProcessTracker> allLives = solrNodesTracker.get(nodeName);
      return allLives.get(allLives.size() - 1).corePullTracker;
    }

    /**
     * Includes a thread into test.
     */
    private void includeThread(Thread t) {
      includedThreads.add(t);
    }

    /**
     * Starts all the included threads.
     */
    private void startIncludedThreads() throws Exception {
      for (Thread t : includedThreads) {
        t.start();
      }
    }

    /**
     * Wait for all the included threads to stop.
     */
    private void waitForThreadsToStop() throws Exception {
      for (Thread t : includedThreads) {
        t.join();
      }
      log.info("docIdGenerator=" + docIdGenerator.get() + " numDocsIndexed=" + numDocsIndexed.get() + " numDocsDeleted=" + idsDeleted.size());
    }

    /**
     * Check if any error was encountered during the test.
     */
    private void checkErrors() {
      assertTrue("indexingErrors=\n" + indexingErrors.toString() + "\n"
              + "queryErrors=\n" + queryErrors.toString() + "\n"
              + "failoverError=\n" + failoverError + "\n",
          indexingErrors.isEmpty() && queryErrors.isEmpty() && failoverError == null);
    }
  }

  /**
   * Track cores' pull and concurrency stage information for one life of a node
   */
  private static class SolrProcessTracker {
    /**
     * Per core pull tracker.
     * Key is the core name.
     */
    private final Map<String, CountDownLatch> corePullTracker;
    /**
     * Per core concurrency stage tracker.
     * Key is the core name.
     * 
     * For now we are only using single replica per node therefore it will only be single core
     * but it should be able to handle multiple replicas per node, if test chooses to setup that way.
     */
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStageTracker;

    private SolrProcessTracker(Map<String, CountDownLatch> corePullTracker,
                               ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStageTracker) {
      this.corePullTracker = corePullTracker;
      this.coreConcurrencyStageTracker = coreConcurrencyStageTracker;
    }
  }
}
