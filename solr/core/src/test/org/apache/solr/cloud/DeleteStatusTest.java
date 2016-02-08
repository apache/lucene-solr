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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.junit.Test;

public class DeleteStatusTest extends AbstractFullDistribZkTestBase {

  @Test
  public void testDeleteStatus() throws IOException, SolrServerException {
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    create.setCollectionName("requeststatus")
        .setConfigName("conf1")
        .setReplicationFactor(1)
        .setNumShards(1)
        .setAsyncId("collectioncreate")
        .process(cloudClient);

    RequestStatusState state = getRequestStateAfterCompletion("collectioncreate", 30, cloudClient);
    assertSame(RequestStatusState.COMPLETED, state);

    // Let's delete the stored response now
    CollectionAdminRequest.DeleteStatus deleteStatus = new CollectionAdminRequest.DeleteStatus();
    CollectionAdminResponse rsp = deleteStatus
        .setRequestId("collectioncreate")
        .process(cloudClient);
    assertEquals("successfully removed stored response for [collectioncreate]", rsp.getResponse().get("status"));

    // Make sure that the response was deleted from zk
    state = getRequestState("collectioncreate", cloudClient);
    assertSame(RequestStatusState.NOT_FOUND, state);

    // Try deleting the same requestid again
    deleteStatus = new CollectionAdminRequest.DeleteStatus();
    rsp = deleteStatus
        .setRequestId("collectioncreate")
        .process(cloudClient);
    assertEquals("[collectioncreate] not found in stored responses", rsp.getResponse().get("status"));

    // Let's try deleting a non-existent status
    deleteStatus = new CollectionAdminRequest.DeleteStatus();
    rsp = deleteStatus
        .setRequestId("foo")
        .process(cloudClient);
    assertEquals("[foo] not found in stored responses", rsp.getResponse().get("status"));
  }

  @Test
  public void testDeleteStatusFlush() throws Exception {
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    create.setConfigName("conf1")
        .setCollectionName("foo")
        .setAsyncId("foo")
        .setNumShards(1)
        .setReplicationFactor(1)
        .process(cloudClient);

    create = new CollectionAdminRequest.Create();
    create.setConfigName("conf1")
        .setCollectionName("bar")
        .setAsyncId("bar")
        .setNumShards(1)
        .setReplicationFactor(1)
        .process(cloudClient);

    RequestStatusState state = getRequestStateAfterCompletion("foo", 30, cloudClient);
    assertEquals(RequestStatusState.COMPLETED, state);

    state = getRequestStateAfterCompletion("bar", 30, cloudClient);
    assertEquals(RequestStatusState.COMPLETED, state);

    CollectionAdminRequest.DeleteStatus deleteStatus = new CollectionAdminRequest.DeleteStatus();
    deleteStatus.setFlush(true)
        .process(cloudClient);

    assertEquals(RequestStatusState.NOT_FOUND, getRequestState("foo", cloudClient));
    assertEquals(RequestStatusState.NOT_FOUND, getRequestState("bar", cloudClient));

    deleteStatus = new CollectionAdminRequest.DeleteStatus();
    try {
      deleteStatus.process(cloudClient);
      fail("delete status should have failed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      assertTrue(e.getMessage().contains("Either requestid or flush parameter must be specified."));
    }

    deleteStatus = new CollectionAdminRequest.DeleteStatus();
    try {
      deleteStatus.setFlush(true)
          .setRequestId("foo")
          .process(cloudClient);
      fail("delete status should have failed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      assertTrue(e.getMessage().contains("Both requestid and flush parameters can not be specified together."));
    }
  }
}
