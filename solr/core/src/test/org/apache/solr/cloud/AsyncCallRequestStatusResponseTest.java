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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncCallRequestStatusResponseTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testAsyncCallStatusResponse() throws Exception {

    String asyncId =
        CollectionAdminRequest.createCollection("asynccall", "conf", 2, 1).processAsync(cluster.getSolrClient());

    waitForState("Expected collection 'asynccall' to have 2 shards and 1 replica", "asynccall", clusterShape(2, 2));

    RequestStatusState state = AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(asyncId, 30, cluster.getSolrClient());
    assertEquals("Unexpected request status: " + state, "completed", state.getKey());

    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus(asyncId);
    CollectionAdminResponse rsp = requestStatus.process(cluster.getSolrClient());
    NamedList<?> r = rsp.getResponse();
    if (OverseerCollectionMessageHandler.INCLUDE_TOP_LEVEL_RESPONSE) {
      assertEquals("Expected 5 elements in the response" + r, 5, r.size());
    } else {
      assertEquals("Expected 3 elements in the response" + r, 3, r.size());
    }
    assertNotNull("Expected 'responseHeader' response" + r, r.get("responseHeader"));
    assertNotNull("Expected 'success' response" + r, r.get("success"));
    assertNotNull("Expected 'status' response" + r, r.get("status"));
    assertEquals("Expected 4 elements in the success element" + r.get("success"), 4, ((NamedList<?>)r.get("success")).size());
  }
}
