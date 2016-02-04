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
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.SplitShard;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Multi threaded Collections API.
 */
public class MultiThreadedOCPTest extends AbstractFullDistribZkTestBase {

  private static final int REQUEST_STATUS_TIMEOUT = 5 * 60;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NUM_COLLECTIONS = 4;

  public MultiThreadedOCPTest() {
    sliceCount = 2;
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {

    testParallelCollectionAPICalls();
    testTaskExclusivity();
    testDeduplicationOfSubmittedTasks();
    testLongAndShortRunningParallelApiCalls();
  }

  private void testParallelCollectionAPICalls() throws IOException, SolrServerException {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      for(int i = 1 ; i <= NUM_COLLECTIONS ; i++) {
        new Create()
                .setCollectionName("ocptest" + i)
                .setNumShards(4)
                .setConfigName("conf1")
                .setAsyncId(String.valueOf(i))
                .process(client);
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

  private void testTaskExclusivity() throws IOException, SolrServerException {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      Create createCollectionRequest = new Create()
              .setCollectionName("ocptest_shardsplit")
              .setNumShards(4)
              .setConfigName("conf1")
              .setAsyncId("1000");
      createCollectionRequest.process(client);
  
      SplitShard splitShardRequest = new SplitShard()
              .setCollectionName("ocptest_shardsplit")
              .setShardName(SHARD1)
              .setAsyncId("1001");
      splitShardRequest.process(client);
  
      splitShardRequest = new SplitShard()
              .setCollectionName("ocptest_shardsplit")
              .setShardName(SHARD2)
              .setAsyncId("1002");
      splitShardRequest.process(client);
  
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
      new Create()
              .setCollectionName("ocptest_shardsplit2")
              .setNumShards(4)
              .setConfigName("conf1")
              .setAsyncId("3000")
              .process(client);
  
      SplitShard splitShardRequest = new SplitShard()
              .setCollectionName("ocptest_shardsplit2")
              .setShardName(SHARD1)
              .setAsyncId("3001");
      splitShardRequest.process(client);
  
      splitShardRequest = new SplitShard()
              .setCollectionName("ocptest_shardsplit2")
              .setShardName(SHARD2)
              .setAsyncId("3002");
      splitShardRequest.process(client);
  
      // Now submit another task with the same id. At this time, hopefully the previous 3002 should still be in the queue.
      splitShardRequest = new SplitShard()
              .setCollectionName("ocptest_shardsplit2")
              .setShardName(SHARD1)
              .setAsyncId("3002");
      CollectionAdminResponse response = splitShardRequest.process(client);
  
      NamedList r = response.getResponse();
      assertEquals("Duplicate request was supposed to exist but wasn't found. De-duplication of submitted task failed.",
          "Task with the same requestid already exists.", r.get("error"));
  
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

      SplitShard splitShardRequest = new SplitShard()
              .setCollectionName("collection1")
              .setShardName(SHARD1)
              .setAsyncId("2000");
      splitShardRequest.process(client);

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



