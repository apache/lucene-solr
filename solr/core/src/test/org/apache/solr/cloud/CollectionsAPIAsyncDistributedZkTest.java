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

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.RequestStatus;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.SplitShard;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.Test;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIAsyncDistributedZkTest extends AbstractFullDistribZkTestBase {
  private static final int MAX_TIMEOUT_SECONDS = 60;

  public CollectionsAPIAsyncDistributedZkTest() {
    sliceCount = 1;
  }

  @Test
  @ShardsFixed(num = 1)
  public void testSolrJAPICalls() throws Exception {
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      Create createCollectionRequest = new Create()
              .setCollectionName("testasynccollectioncreation")
              .setNumShards(1)
              .setConfigName("conf1")
              .setAsyncId("1001");
      createCollectionRequest.process(client);
  
      String state = getRequestStateAfterCompletion("1001", MAX_TIMEOUT_SECONDS, client);
  
      assertEquals("CreateCollection task did not complete!", "completed", state);
  
  
      createCollectionRequest = new Create()
              .setCollectionName("testasynccollectioncreation")
              .setNumShards(1)
              .setConfigName("conf1")
              .setAsyncId("1002");
      createCollectionRequest.process(client);
  
      state = getRequestStateAfterCompletion("1002", MAX_TIMEOUT_SECONDS, client);
  
      assertEquals("Recreating a collection with the same name didn't fail, should have.", "failed", state);
  
      CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica()
              .setCollectionName("testasynccollectioncreation")
              .setShardName("shard1")
              .setAsyncId("1003");
      client.request(addReplica);
      state = getRequestStateAfterCompletion("1003", MAX_TIMEOUT_SECONDS, client);
      assertEquals("Add replica did not complete", "completed", state);
  
  
      SplitShard splitShardRequest = new SplitShard()
              .setCollectionName("testasynccollectioncreation")
              .setShardName("shard1")
              .setAsyncId("1004");
      splitShardRequest.process(client);
  
      state = getRequestStateAfterCompletion("1004", MAX_TIMEOUT_SECONDS * 2, client);
  
      assertEquals("Shard split did not complete. Last recorded state: " + state, "completed", state);
    }
  }

  private String getRequestStateAfterCompletion(String requestId, int waitForSeconds, SolrClient client)
      throws IOException, SolrServerException {
    String state = null;
    while(waitForSeconds-- > 0) {
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

  private String getRequestState(String requestId, SolrClient client) throws IOException, SolrServerException {
    RequestStatus request = new RequestStatus();
    request.setRequestId(requestId);
    CollectionAdminResponse response = request.process(client);
    NamedList innerResponse = (NamedList) response.getResponse().get("status");
    return (String) innerResponse.get("state");
  }
}
