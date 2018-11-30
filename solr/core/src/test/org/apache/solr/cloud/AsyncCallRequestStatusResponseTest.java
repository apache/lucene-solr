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

import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
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

    int tries = 0;
    while (true) {
      final RequestStatusState state
          = CollectionAdminRequest.requestStatus(asyncId).process(cluster.getSolrClient()).getRequestStatus();
      if (state == RequestStatusState.COMPLETED)
        break;
      if (tries++ > 10)
        fail("Expected to see RequestStatusState.COMPLETED but was " + state.toString());
      TimeUnit.SECONDS.sleep(1);
    }

    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus(asyncId);
    CollectionAdminResponse rsp = requestStatus.process(cluster.getSolrClient());
    NamedList<?> r = rsp.getResponse();
    // Check that there's more response than the hardcoded status and states
    assertEquals("Assertion Failure" + r.toString(), 5, r.size());
  }
}
