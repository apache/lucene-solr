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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.Before;

import java.io.IOException;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIAsyncDistributedZkTest extends AbstractFullDistribZkTestBase {
  private static final int MAX_TIMEOUT_SECONDS = 60;
  private static final boolean DEBUG = false;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    useJettyDataDir = false;

    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  public CollectionsAPIAsyncDistributedZkTest() {
    fixShardCount = true;

    sliceCount = 1;
    shardCount = 1;
  }

  @Override
  public void doTest() throws Exception {

    testSolrJAPICalls();

    if (DEBUG) {
      super.printLayout();
    }
  }

  private void testSolrJAPICalls() throws Exception {
    SolrServer server = createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0)));
    CollectionAdminRequest.createCollection("testasynccollectioncreation", 1, "conf1", server, "1001");
    String state = null;

    state = getRequestStateAfterCompletion("1001", MAX_TIMEOUT_SECONDS, server);

    assertEquals("CreateCollection task did not complete!", "completed", state);

    CollectionAdminRequest.createCollection("testasynccollectioncreation", 1, "conf1", server, "1002");

    state = getRequestStateAfterCompletion("1002", MAX_TIMEOUT_SECONDS, server);

    assertEquals("Recreating a collection with the same name didn't fail, should have.", "failed", state);

    CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica();
    addReplica.setCollectionName("testasynccollectioncreation");
    addReplica.setShardName("shard1");
    addReplica.setAsyncId("1003");
    server.request(addReplica);
    state = getRequestStateAfterCompletion("1003", MAX_TIMEOUT_SECONDS, server);
    assertEquals("Add replica did not complete", "completed", state);

    CollectionAdminRequest.splitShard("testasynccollectioncreation", "shard1", server, "1004");

    state = getRequestStateAfterCompletion("1004", MAX_TIMEOUT_SECONDS * 2, server);

    assertEquals("Shard split did not complete. Last recorded state: " + state, "completed", state);
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
