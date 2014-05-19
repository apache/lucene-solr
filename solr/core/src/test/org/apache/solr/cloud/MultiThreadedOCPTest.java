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

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Tests the Multi threaded Collections API.
 */
public class MultiThreadedOCPTest extends AbstractFullDistribZkTestBase {

  private static Logger log = LoggerFactory
      .getLogger(MultiThreadedOCPTest.class);

  private static int NUM_COLLECTIONS = 4;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    useJettyDataDir = false;

    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  public MultiThreadedOCPTest() {
    fixShardCount = true;
    sliceCount = 2;
    shardCount = 4;
  }

  @Override
  public void doTest() throws Exception {

    testParallelCollectionAPICalls();
    testTaskExclusivity();
    testLongAndShortRunningParallelApiCalls();
  }

  private void testParallelCollectionAPICalls() throws IOException, SolrServerException {
    SolrServer server = createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0)));

    for(int i = 1 ; i <= NUM_COLLECTIONS ; i++) {
      CollectionAdminRequest.createCollection("ocptest" + i, 4, "conf1", server, i + "");
    }

    boolean pass = false;
    int counter = 0;
    while(true) {
      int numRunningTasks = 0;
      for (int i = 1; i <= NUM_COLLECTIONS; i++)
        if (getRequestState(i + "", server).equals("running"))
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
      String state = getRequestStateAfterCompletion(i + "", 30, server);
      assertTrue("Task " + i + " did not complete, final state: " + state,state.equals("completed"));
    }
  }

  private void testTaskExclusivity() throws IOException, SolrServerException {
    SolrServer server = createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0)));
    CollectionAdminRequest.createCollection("ocptest_shardsplit", 4, "conf1", server, "1000");

    CollectionAdminRequest.splitShard("ocptest_shardsplit", SHARD1, server, "1001");
    CollectionAdminRequest.splitShard("ocptest_shardsplit", SHARD2, server, "1002");

    int iterations = 0;
    while(true) {
      int runningTasks = 0;
      int completedTasks = 0;
      for (int i=1001;i<=1002;i++) {
        String state = getRequestState(i, server);
        if (state.equals("running"))
          runningTasks++;
        if (state.equals("completed"))
          completedTasks++;
        assertTrue("We have a failed SPLITSHARD task", !state.equals("failed"));
      }
      // TODO: REQUESTSTATUS might come back with more than 1 running tasks over multiple calls.
      // The only way to fix this is to support checking of multiple requestids in a single REQUESTSTATUS task.
      
      assertTrue("Mutual exclusion failed. Found more than one task running for the same collection", runningTasks < 2);

      if(completedTasks == 2 || iterations++ > 90)
        break;

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    for (int i=1001;i<=1002;i++) {
      String state = getRequestStateAfterCompletion(i + "", 30, server);
      assertTrue("Task " + i + " did not complete, final state: " + state,state.equals("completed"));
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

    try {
      Thread.sleep(5000);

      SolrServer server = createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0)));
      CollectionAdminRequest.splitShard("collection1", SHARD1, server, "2000");

      String state = getRequestState("2000", server);
      while (!state.equals("running")) {
        state = getRequestState("2000", server);
        if (state.equals("completed") || state.equals("failed"))
          break;
        Thread.sleep(100);
      }
      assertTrue("SplitShard task [2000] was supposed to be in [running] but isn't. It is [" + state + "]", state.equals("running"));

      invokeCollectionApi("action", CollectionParams.CollectionAction.OVERSEERSTATUS.toLower());

      state = getRequestState("2000", server);

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

  private String getRequestStateAfterCompletion(String requestId, int waitForSeconds, SolrServer server)
      throws IOException, SolrServerException {
    String state = null;
    while(waitForSeconds-- > 0) {
      state = getRequestState(requestId, server);
      if(state.equals("completed") || state.equals("failed"))
        return state;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    return state;
  }

  private String getRequestState(int requestId, SolrServer server) throws IOException, SolrServerException {
    return getRequestState(String.valueOf(requestId), server);
  }

  private String getRequestState(String requestId, SolrServer server) throws IOException, SolrServerException {
    CollectionAdminResponse response = CollectionAdminRequest.requestStatus(requestId, server);
    NamedList innerResponse = (NamedList) response.getResponse().get("status");
    return (String) innerResponse.get("state");
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("numShards");
    System.clearProperty("zkHost");
    System.clearProperty("solr.xml.persist");
    
    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }

}



