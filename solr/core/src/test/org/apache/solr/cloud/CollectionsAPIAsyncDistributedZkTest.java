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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.SplitShard;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Slice;
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
  
      RequestStatusState state = getRequestStateAfterCompletion("1001", MAX_TIMEOUT_SECONDS, client);
  
      assertSame("CreateCollection task did not complete!", RequestStatusState.COMPLETED, state);
  
      createCollectionRequest = new Create()
              .setCollectionName("testasynccollectioncreation")
              .setNumShards(1)
              .setConfigName("conf1")
              .setAsyncId("1002");
      createCollectionRequest.process(client);
  
      state = getRequestStateAfterCompletion("1002", MAX_TIMEOUT_SECONDS, client);
  
      assertSame("Recreating a collection with the same should have failed.", RequestStatusState.FAILED, state);
  
      CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica()
              .setCollectionName("testasynccollectioncreation")
              .setShardName("shard1")
              .setAsyncId("1003");
      client.request(addReplica);
      state = getRequestStateAfterCompletion("1003", MAX_TIMEOUT_SECONDS, client);
      assertSame("Add replica did not complete", RequestStatusState.COMPLETED, state);
  
      SplitShard splitShardRequest = new SplitShard()
              .setCollectionName("testasynccollectioncreation")
              .setShardName("shard1")
              .setAsyncId("1004");
      splitShardRequest.process(client);
  
      state = getRequestStateAfterCompletion("1004", MAX_TIMEOUT_SECONDS * 2, client);
  
      assertEquals("Shard split did not complete. Last recorded state: " + state, RequestStatusState.COMPLETED, state);
    }
  }

  @Test
  public void testAsyncRequests() throws Exception {
    String collection = "testAsyncOperations";

    Create createCollectionRequest = new Create()
        .setCollectionName(collection)
        .setNumShards(1)
        .setRouterName("implicit")
        .setShards("shard1")
        .setConfigName("conf1")
        .setAsyncId("42");
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);
    assertEquals("42", response.getResponse().get("requestid"));
    RequestStatusState state = getRequestStateAfterCompletion("42", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("CreateCollection task did not complete!", RequestStatusState.COMPLETED, state);

    //Add a few documents to shard1
    int numDocs = TestUtil.nextInt(random(), 10, 100);
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i=0; i<numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("_route_", "shard1");
      docs.add(doc);
    }
    cloudClient.add(collection, docs);
    cloudClient.commit(collection);

    SolrQuery query = new SolrQuery("*:*");
    query.set("shards", "shard1");
    assertEquals(numDocs, cloudClient.query(collection, query).getResults().getNumFound());

    CollectionAdminRequest.Reload reloadCollection = new CollectionAdminRequest.Reload();
    reloadCollection.setCollectionName(collection).setAsyncId("43");
    response = reloadCollection.process(cloudClient);
    assertEquals("43", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("43", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("ReloadCollection did not complete", RequestStatusState.COMPLETED, state);

    CollectionAdminRequest.CreateShard createShard = new CollectionAdminRequest.CreateShard()
        .setCollectionName(collection)
        .setShardName("shard2")
        .setAsyncId("44");
    response = createShard.process(cloudClient);
    assertEquals("44", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("44", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("CreateShard did not complete", RequestStatusState.COMPLETED, state);

    //Add a doc to shard2 to make sure shard2 was created properly
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", numDocs + 1);
    doc.addField("_route_", "shard2");
    cloudClient.add(collection, doc);
    cloudClient.commit(collection);
    query = new SolrQuery("*:*");
    query.set("shards", "shard2");
    assertEquals(1, cloudClient.query(collection, query).getResults().getNumFound());

    CollectionAdminRequest.DeleteShard deleteShard = new CollectionAdminRequest.DeleteShard()
        .setCollectionName(collection)
        .setShardName("shard2")
        .setAsyncId("45");
    response = deleteShard.process(cloudClient);
    assertEquals("45", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("45", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("DeleteShard did not complete", RequestStatusState.COMPLETED, state);

    CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica()
        .setCollectionName(collection)
        .setShardName("shard1")
        .setAsyncId("46");
    response = addReplica.process(cloudClient);
    assertEquals("46", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("46", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("AddReplica did not complete", RequestStatusState.COMPLETED, state);

    //cloudClient watch might take a couple of seconds to reflect it
    Slice shard1 = cloudClient.getZkStateReader().getClusterState().getSlice(collection, "shard1");
    int count = 0;
    while (shard1.getReplicas().size() != 2) {
      if (count++ > 1000) {
        fail("2nd Replica not reflecting in the cluster state");
      }
      Thread.sleep(100);
    }

    CollectionAdminRequest.CreateAlias createAlias = new CollectionAdminRequest.CreateAlias()
        .setAliasName("myalias")
        .setAliasedCollections(collection)
        .setAsyncId("47");
    response = createAlias.process(cloudClient);
    assertEquals("47", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("47", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("CreateAlias did not complete", RequestStatusState.COMPLETED, state);

    query = new SolrQuery("*:*");
    query.set("shards", "shard1");
    assertEquals(numDocs, cloudClient.query("myalias", query).getResults().getNumFound());

    CollectionAdminRequest.DeleteAlias deleteAlias = new CollectionAdminRequest.DeleteAlias()
        .setAliasName("myalias")
        .setAsyncId("48");
    response = deleteAlias.process(cloudClient);
    assertEquals("48", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("48", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("DeleteAlias did not complete", RequestStatusState.COMPLETED, state);

    try {
      cloudClient.query("myalias", query);
      fail("Alias should not exist");
    } catch (SolrException e) {
      //expected
    }

    String replica = shard1.getReplicas().iterator().next().getName();
    CollectionAdminRequest.DeleteReplica deleteReplica = new CollectionAdminRequest.DeleteReplica()
        .setCollectionName(collection)
        .setShardName("shard1")
        .setReplica(replica)
        .setAsyncId("47");
    response = deleteReplica.process(cloudClient);
    assertEquals("47", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("47", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("DeleteReplica did not complete", RequestStatusState.COMPLETED, state);

    CollectionAdminRequest.Delete deleteCollection = new CollectionAdminRequest.Delete()
        .setCollectionName(collection)
        .setAsyncId("48");
    response = deleteCollection.process(cloudClient);
    assertEquals("48", response.getResponse().get("requestid"));
    state = getRequestStateAfterCompletion("48", MAX_TIMEOUT_SECONDS, cloudClient);
    assertSame("DeleteCollection did not complete", RequestStatusState.COMPLETED, state);
  }
}
