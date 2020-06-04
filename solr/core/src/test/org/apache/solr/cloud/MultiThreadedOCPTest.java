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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Random;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.SplitShard;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.OverseerTaskProcessor.MAX_PARALLEL_TASKS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_COLL_TASK;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

/**
 * Tests the Multi threaded Collections API.
 */
public class MultiThreadedOCPTest extends AbstractFullDistribZkTestBase {

  private static final int REQUEST_STATUS_TIMEOUT = 5 * 60;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NUM_COLLECTIONS = 3;

  public MultiThreadedOCPTest() {
    sliceCount = 2;
    
    fixShardCount(3);
  }

  @Test
  public void test() throws Exception {
    testParallelCollectionAPICalls();
    testTaskExclusivity();
    testDeduplicationOfSubmittedTasks();
    testLongAndShortRunningParallelApiCalls();
    testFillWorkQueue();
  }

  private void testFillWorkQueue() throws Exception {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      DistributedQueue distributedQueue = new ZkDistributedQueue(cloudClient.getZkStateReader().getZkClient(),
          "/overseer/collection-queue-work", new Stats());
      //fill the work queue with blocked tasks by adding more than the no:of parallel tasks
      for (int i = 0; i < MAX_PARALLEL_TASKS + 15; i++) {
        distributedQueue.offer(Utils.toJSON(Utils.makeMap(
            "collection", "A_COLL",
            QUEUE_OPERATION, MOCK_COLL_TASK.toLower(),
            ASYNC, Integer.toString(i),

            // third task waits for a long time, and thus blocks the queue for all other tasks for A_COLL.
            // Subsequent tasks as well as the first two only wait for 1ms
            "sleep", (i == 2 ? "10000" : "1")
        )));
        log.info("MOCK task added {}", i);
      }

      // Wait until we see the second A_COLL task getting processed (assuming the first got processed as well)
      Long task1CollA = waitForTaskToCompleted(client, 1);

      assertNotNull("Queue did not process first two tasks on A_COLL, can't run test", task1CollA);

      // Make sure the long running task did not finish, otherwise no way the B_COLL task can be tested to run in parallel with it
      assertNull("Long running task finished too early, can't test", checkTaskHasCompleted(client, 2));

      // Enqueue a task on another collection not competing with the lock on A_COLL and see that it can be executed right away
      distributedQueue.offer(Utils.toJSON(Utils.makeMap(
          "collection", "B_COLL",
          QUEUE_OPERATION, MOCK_COLL_TASK.toLower(),
          ASYNC, "200",
          "sleep", "1"
      )));

      // We now check that either the B_COLL task has completed before the third (long running) task on A_COLL,
      // Or if both have completed (if this check got significantly delayed for some reason), we verify B_COLL was first.
      Long taskCollB = waitForTaskToCompleted(client, 200);

      // We do not wait for the long running task to finish, that would be a waste of time.
      Long task2CollA = checkTaskHasCompleted(client, 2);

      // Given the wait delay (500 iterations of 100ms), the task has plenty of time to complete, so this is not expected.
      assertNotNull("Task on  B_COLL did not complete, can't test", taskCollB);
      // We didn't wait for the 3rd A_COLL task to complete (test can run quickly) but if it did, we expect the B_COLL to have finished first.
      assertTrue("task2CollA: " + task2CollA + " taskCollB: " + taskCollB, task2CollA  == null || task2CollA > taskCollB);
    }
  }

  /**
   * Verifies the status of an async task submitted to the Overseer Collection queue.
   * @return <code>null</code> if the task has not completed, the completion timestamp if the task has completed
   * (see mockOperation() in {@link org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler}).
   */
  private Long checkTaskHasCompleted(SolrClient client, int requestId) throws IOException, SolrServerException {
    return (Long) getStatusResponse(Integer.toString(requestId), client).getResponse().get("MOCK_FINISHED");
  }

  /**
   * Waits until the specified async task has completed or time ran out.
   * @return <code>null</code> if the task has not completed, the completion timestamp if the task has completed
   */
  private Long waitForTaskToCompleted(SolrClient client, int requestId) throws Exception {
    for (int i = 0; i < 500; i++) {
      Long task = checkTaskHasCompleted(client, requestId);
      if (task != null) {
        return task;
      }
      Thread.sleep(100);
    }

    return null;
  }

  private void testParallelCollectionAPICalls() throws IOException, SolrServerException {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      for(int i = 1 ; i <= NUM_COLLECTIONS ; i++) {
        CollectionAdminRequest.createCollection("ocptest" + i,"conf1",3,1).processAsync(String.valueOf(i), client);
      }
  
      boolean pass = false;
      int counter = 0;
      while(true) {
        int numRunningTasks = 0;
        for (int i = 1; i <= NUM_COLLECTIONS; i++)
          if (getRequestState(i + "", client) == RequestStatusState.RUNNING) {
            numRunningTasks++;
          }
        if (numRunningTasks > 1) {
          pass = true;
          break;
        } else if (counter++ > 100) {
          break;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      assertTrue("More than one tasks were supposed to be running in parallel but they weren't.", pass);
      for (int i = 1; i <= NUM_COLLECTIONS; i++) {
        final RequestStatusState state = getRequestStateAfterCompletion(i + "", REQUEST_STATUS_TIMEOUT, client);
        assertSame("Task " + i + " did not complete, final state: " + state, RequestStatusState.COMPLETED, state);
      }
    }
  }

  private void testTaskExclusivity() throws Exception, SolrServerException {

    DistributedQueue distributedQueue = new ZkDistributedQueue(cloudClient.getZkStateReader().getZkClient(),
        "/overseer/collection-queue-work", new Stats());
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {

      Create createCollectionRequest = CollectionAdminRequest.createCollection("ocptest_shardsplit","conf1",4,1);
      createCollectionRequest.processAsync("1000",client);

      distributedQueue.offer(Utils.toJSON(Utils.makeMap(
          "collection", "ocptest_shardsplit",
          QUEUE_OPERATION, MOCK_COLL_TASK.toLower(),
          ASYNC, "1001",
          "sleep", "100"
      )));
      distributedQueue.offer(Utils.toJSON(Utils.makeMap(
          "collection", "ocptest_shardsplit",
          QUEUE_OPERATION, MOCK_COLL_TASK.toLower(),
          ASYNC, "1002",
          "sleep", "100"
      )));

      int iterations = 0;
      while(true) {
        int runningTasks = 0;
        int completedTasks = 0;
        for (int i = 1001; i <= 1002; i++) {
          final RequestStatusState state = getRequestState(i, client);
          if (state == RequestStatusState.RUNNING) {
            runningTasks++;
          } else if (state == RequestStatusState.COMPLETED) {
            completedTasks++;
          }
          assertNotSame("We have a failed SPLITSHARD task", RequestStatusState.FAILED, state);
        }
        // TODO: REQUESTSTATUS might come back with more than 1 running tasks over multiple calls.
        // The only way to fix this is to support checking of multiple requestids in a single REQUESTSTATUS task.
        
        assertTrue("Mutual exclusion failed. Found more than one task running for the same collection", runningTasks < 2);
  
        if(completedTasks == 2 || iterations++ > REQUEST_STATUS_TIMEOUT)
          break;
  
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
      for (int i = 1001; i <= 1002; i++) {
        final RequestStatusState state = getRequestStateAfterCompletion(i + "", REQUEST_STATUS_TIMEOUT, client);
        assertSame("Task " + i + " did not complete, final state: " + state, RequestStatusState.COMPLETED, state);
      }
    }
  }

  private void testDeduplicationOfSubmittedTasks() throws IOException, SolrServerException {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminRequest.createCollection("ocptest_shardsplit2","conf1",3,1).processAsync("3000",client);
  
      SplitShard splitShardRequest = CollectionAdminRequest.splitShard("ocptest_shardsplit2").setShardName(SHARD1);
      splitShardRequest.processAsync("3001",client);
      
      splitShardRequest = CollectionAdminRequest.splitShard("ocptest_shardsplit2").setShardName(SHARD2);
      splitShardRequest.processAsync("3002",client);
  
      // Now submit another task with the same id. At this time, hopefully the previous 3002 should still be in the queue.
      expectThrows(SolrServerException.class, () -> {
          CollectionAdminRequest.splitShard("ocptest_shardsplit2").setShardName(SHARD1).processAsync("3002",client);
          // more helpful assertion failure
          fail("Duplicate request was supposed to exist but wasn't found. De-duplication of submitted task failed.");
        });
      
      for (int i = 3001; i <= 3002; i++) {
        final RequestStatusState state = getRequestStateAfterCompletion(i + "", REQUEST_STATUS_TIMEOUT, client);
        assertSame("Task " + i + " did not complete, final state: " + state, RequestStatusState.COMPLETED, state);
      }
    }
  }

  private void testLongAndShortRunningParallelApiCalls() throws InterruptedException, IOException, SolrServerException {

    Thread indexThread = new Thread() {
      @Override
      public void run() {
        Random random = random();
        int max = atLeast(random, 200);
        for (int id = 101; id < max; id++) {
          try {
            doAddDoc(String.valueOf(id));
          } catch (Exception e) {
            log.error("Exception while adding docs", e);
          }
        }
      }
    };
    indexThread.start();
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {

      SplitShard splitShardRequest = CollectionAdminRequest.splitShard("collection1").setShardName(SHARD1);
      splitShardRequest.processAsync("2000",client);

      RequestStatusState state = getRequestState("2000", client);
      while (state ==  RequestStatusState.SUBMITTED) {
        state = getRequestState("2000", client);
        Thread.sleep(10);
      }
      assertSame("SplitShard task [2000] was supposed to be in [running] but isn't. It is [" + state + "]",
          RequestStatusState.RUNNING, state);

      // CLUSTERSTATE is always mutually exclusive, it should return with a response before the split completes

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", "collection1");
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      client.request(request);

      state = getRequestState("2000", client);

      assertSame("After invoking OVERSEERSTATUS, SplitShard task [2000] was still supposed to be in [running] but "
          + "isn't. It is [" + state + "]", RequestStatusState.RUNNING, state);

    } finally {
      try {
        indexThread.join();
      } catch (InterruptedException e) {
        log.warn("Indexing thread interrupted.");
      }
    }
  }

  void doAddDoc(String id) throws Exception {
    index("id", id);
    // todo - target diff servers and use cloud clients as well as non-cloud clients
  }
}



