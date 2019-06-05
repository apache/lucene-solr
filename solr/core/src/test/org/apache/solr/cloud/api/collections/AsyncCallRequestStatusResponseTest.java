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
package org.apache.solr.cloud.api.collections;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncCallRequestStatusResponseTest extends SolrCloudTestCase {

  private static boolean oldResponseEntries;

  @SuppressWarnings("deprecation")
  @BeforeClass
  public static void setupCluster() throws Exception {
    oldResponseEntries = OverseerCollectionMessageHandler.INCLUDE_TOP_LEVEL_RESPONSE;
    OverseerCollectionMessageHandler.INCLUDE_TOP_LEVEL_RESPONSE = random().nextBoolean();
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }
  
  @SuppressWarnings("deprecation")
  @AfterClass
  public static void restoreFlag() throws Exception {
    OverseerCollectionMessageHandler.INCLUDE_TOP_LEVEL_RESPONSE = oldResponseEntries; 
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testAsyncCallStatusResponse() throws Exception {
    int numShards = 4;
    int numReplicas = 1;
    Create createCollection = CollectionAdminRequest.createCollection("asynccall", "conf", numShards, numReplicas);
    createCollection.setMaxShardsPerNode(100);
    String asyncId =
        createCollection.processAsync(cluster.getSolrClient());

    waitForState("Expected collection 'asynccall' to have "+numShards+" shards and "+
        numShards*numReplicas+" replica", "asynccall", clusterShape(numShards, numShards*numReplicas));

    RequestStatusState state = AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(asyncId, 30, cluster.getSolrClient());
    assertEquals("Unexpected request status: " + state, "completed", state.getKey());

    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus(asyncId);
    CollectionAdminResponse rsp = requestStatus.process(cluster.getSolrClient());
    NamedList<?> r = rsp.getResponse();
    if (OverseerCollectionMessageHandler.INCLUDE_TOP_LEVEL_RESPONSE) {
      final int actualNumOfElems = 3+(numShards*numReplicas);
      // responseHeader, success, status, + old responses per every replica  
      assertEquals("Expected "+actualNumOfElems+" elements in the response" + r.jsonStr(),
               actualNumOfElems, r.size());
    } else {
      // responseHeader, success, status
      assertEquals("Expected 3 elements in the response" + r.jsonStr(), 3, r.size());
    }
    assertNotNull("Expected 'responseHeader' response" + r, r.get("responseHeader"));
    assertNotNull("Expected 'status' response" + r, r.get("status"));
    {
      final NamedList<?> success = (NamedList<?>)r.get("success");
      assertNotNull("Expected 'success' response" + r, success);
    
      final int actualSuccessElems = 2*(numShards*numReplicas);
      // every replica responds once on submit and once on complete
      assertEquals("Expected "+actualSuccessElems+
        " elements in the success element" + success.jsonStr(), 
          actualSuccessElems, success.size());
    }
  }
}
