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
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
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
            ASYNC, String.valueOf(i),

            "sleep", (i == 0 ? "1000" : "1") //first task waits for 1 second, and thus blocking
            // all other tasks. Subsequent tasks only wait for 1ms
        )));
        log.info("MOCK task added {}", i);

      }
      Thread.sleep(100);//wait and post the next message

      //this is not going to be blocked because it operates on another collection
      distributedQueue.offer(Utils.toJSON(Utils.makeMap(
          "collection", "B_COLL",
          QUEUE_OPERATION, MOCK_COLL_TASK.toLower(),
          ASYNC, "200",
          "sleep", "1"
      )));


      Long acoll = null, bcoll = null;
      for (int i = 0; i < 500; i++) {
        if (bcoll == null) {
          CollectionAdminResponse statusResponse = getStatusResponse("200", client);
          bcoll = (Long) statusResponse.getResponse().get("MOCK_FINISHED");
        }
        if (acoll == null) {
          CollectionAdminResponse statusResponse = getStatusResponse("2", client);
          acoll = (Long) statusResponse.getResponse().get("MOCK_FINISHED");
        }
        if (acoll != null && bcoll != null) break;
        Thread.sleep(100);
      }
      assertTrue(acoll != null && bcoll != null);
      assertTrue("acoll: " + acoll + " bcoll: " + bcoll, acoll > bcoll);
    }

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
      @SuppressWarnings({"rawtypes"})
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



