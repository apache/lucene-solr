package org.apache.solr.cloud;

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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.RequestStatus;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.SplitShard;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests the Multi threaded Collections API.
 */
public class MultiThreadedOCPTest extends AbstractFullDistribZkTestBase {

  private static final int REQUEST_STATUS_TIMEOUT = 5 * 60;
  private static Logger log = LoggerFactory
      .getLogger(MultiThreadedOCPTest.class);

  private static final int NUM_COLLECTIONS = 4;

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();

    useJettyDataDir = false;

    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

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
        Create createCollectionRequest = new Create();
        createCollectionRequest.setCollectionName("ocptest" + i);
        createCollectionRequest.setNumShards(4);
        createCollectionRequest.setConfigName("conf1");
        createCollectionRequest.setAsyncId(String.valueOf(i));
        createCollectionRequest.process(client);
      }
  
      boolean pass = false;
      int counter = 0;
      while(true) {
        int numRunningTasks = 0;
        for (int i = 1; i <= NUM_COLLECTIONS; i++)
          if (getRequestState(i + "", client).equals("running"))
            numRunningTasks++;
        if(numRunningTasks > 1) {
          pass = true;
          break;
        } else if(counter++ > 100)
          break;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      assertTrue("More than one tasks were supposed to be running in parallel but they weren't.", pass);
      for(int i=1;i<=NUM_COLLECTIONS;i++) {
        String state = getRequestStateAfterCompletion(i + "", REQUEST_STATUS_TIMEOUT, client);
        assertTrue("Task " + i + " did not complete, final state: " + state,state.equals("completed"));
      }
    }
  }

  private void testTaskExclusivity() throws IOException, SolrServerException {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      Create createCollectionRequest = new Create();
      createCollectionRequest.setCollectionName("ocptest_shardsplit");
      createCollectionRequest.setNumShards(4);
      createCollectionRequest.setConfigName("conf1");
      createCollectionRequest.setAsyncId("1000");
      createCollectionRequest.process(client);
  
      SplitShard splitShardRequest = new SplitShard();
      splitShardRequest.setCollectionName("ocptest_shardsplit");
      splitShardRequest.setShardName(SHARD1);
      splitShardRequest.setAsyncId("1001");
      splitShardRequest.process(client);
  
      splitShardRequest = new SplitShard();
      splitShardRequest.setCollectionName("ocptest_shardsplit");
      splitShardRequest.setShardName(SHARD2);
      splitShardRequest.setAsyncId("1002");
      splitShardRequest.process(client);
  
      int iterations = 0;
      while(true) {
        int runningTasks = 0;
        int completedTasks = 0;
        for (int i=1001;i<=1002;i++) {
          String state = getRequestState(i, client);
          if (state.equals("running"))
            runningTasks++;
          if (state.equals("completed"))
            completedTasks++;
          assertTrue("We have a failed SPLITSHARD task", !state.equals("failed"));
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
      for (int i=1001;i<=1002;i++) {
        String state = getRequestStateAfterCompletion(i + "", REQUEST_STATUS_TIMEOUT, client);
        assertTrue("Task " + i + " did not complete, final state: " + state,state.equals("completed"));
      }
    }
  }

  private void testDeduplicationOfSubmittedTasks() throws IOException, SolrServerException {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      Create createCollectionRequest = new Create();
      createCollectionRequest.setCollectionName("ocptest_shardsplit2");
      createCollectionRequest.setNumShards(4);
      createCollectionRequest.setConfigName("conf1");
      createCollectionRequest.setAsyncId("3000");
      createCollectionRequest.process(client);
  
      SplitShard splitShardRequest = new SplitShard();
      splitShardRequest.setCollectionName("ocptest_shardsplit2");
      splitShardRequest.setShardName(SHARD1);
      splitShardRequest.setAsyncId("3001");
      splitShardRequest.process(client);
  
      splitShardRequest = new SplitShard();
      splitShardRequest.setCollectionName("ocptest_shardsplit2");
      splitShardRequest.setShardName(SHARD2);
      splitShardRequest.setAsyncId("3002");
      splitShardRequest.process(client);
  
      // Now submit another task with the same id. At this time, hopefully the previous 3002 should still be in the queue.
      splitShardRequest = new SplitShard();
      splitShardRequest.setCollectionName("ocptest_shardsplit2");
      splitShardRequest.setShardName(SHARD1);
      splitShardRequest.setAsyncId("3002");
      CollectionAdminResponse response = splitShardRequest.process(client);
  
      NamedList r = response.getResponse();
      assertEquals("Duplicate request was supposed to exist but wasn't found. De-duplication of submitted task failed.",
          "Task with the same requestid already exists.", r.get("error"));
  
      for (int i=3001;i<=3002;i++) {
        String state = getRequestStateAfterCompletion(i + "", REQUEST_STATUS_TIMEOUT, client);
        assertTrue("Task " + i + " did not complete, final state: " + state,state.equals("completed"));
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

      SplitShard splitShardRequest = new SplitShard();
      splitShardRequest.setCollectionName("collection1");
      splitShardRequest.setShardName(SHARD1);
      splitShardRequest.setAsyncId("2000");
      splitShardRequest.process(client);

      String state = getRequestState("2000", client);
      while (state.equals("submitted")) {
        state = getRequestState("2000", client);
        Thread.sleep(10);
      }
      assertTrue("SplitShard task [2000] was supposed to be in [running] but isn't. It is [" + state + "]", state.equals("running"));

      // CLUSTERSTATE is always mutually exclusive, it should return with a response before the split completes

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.CLUSTERSTATUS.toString());
      params.set("collection", "collection1");
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      client.request(request);

      state = getRequestState("2000", client);

      assertTrue("After invoking OVERSEERSTATUS, SplitShard task [2000] was still supposed to be in [running] but isn't." +
          "It is [" + state + "]", state.equals("running"));

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

  private String getRequestStateAfterCompletion(String requestId, int waitForSeconds, SolrClient client)
      throws IOException, SolrServerException {
    String state = null;
    long maxWait = System.nanoTime() + TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS);

    while (System.nanoTime() < maxWait)  {
      state = getRequestState(requestId, client);
      if(state.equals("completed") || state.equals("failed"))
        return state;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }

    return state;
  }

  private String getRequestState(int requestId, SolrClient client) throws IOException, SolrServerException {
    return getRequestState(String.valueOf(requestId), client);
  }

  private String getRequestState(String requestId, SolrClient client) throws IOException, SolrServerException {
    RequestStatus requestStatusRequest = new RequestStatus();
    requestStatusRequest.setRequestId(requestId);
    CollectionAdminResponse response = requestStatusRequest.process(client);

    NamedList innerResponse = (NamedList) response.getResponse().get("status");
    return (String) innerResponse.get("state");
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    System.clearProperty("numShards");
    System.clearProperty("solr.xml.persist");
    
    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }

}



