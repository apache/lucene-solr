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
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class AsyncCallRequestStatusResponseTest extends AbstractFullDistribZkTestBase {

  @ShardsFixed(num = 2)
  @Test
  public void testAsyncCallStatusResponse() throws Exception {
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    create.setCollectionName("asynccall")
        .setNumShards(2)
        .setAsyncId("1000")
        .setConfigName("conf1")
        .process(cloudClient);
    waitForCollection(cloudClient.getZkStateReader(), "asynccall", 2);
    final RequestStatusState state = getRequestStateAfterCompletion("1000", 30, cloudClient);
    assertSame(RequestStatusState.COMPLETED, state);
    CollectionAdminRequest.RequestStatus requestStatus = new CollectionAdminRequest.RequestStatus();
    requestStatus.setRequestId("1000");
    CollectionAdminResponse rsp = requestStatus.process(cloudClient);
    NamedList<?> r = rsp.getResponse();
    // Check that there's more response than the hardcoded status and states
    assertEquals("Assertion Failure" + r.toString(), 5, r.size());
  }
}
