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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIAsyncDistributedZkTest extends SolrCloudTestCase {

  private static final int MAX_TIMEOUT_SECONDS = 60;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testSolrJAPICalls() throws Exception {

    final CloudSolrClient client = cluster.getSolrClient();

    RequestStatusState state = CollectionAdminRequest.createCollection("testasynccollectioncreation","conf1",1,1)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("CreateCollection task did not complete!", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.createCollection("testasynccollectioncreation","conf1",1,1)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("Recreating a collection with the same should have failed.", RequestStatusState.FAILED, state);

    state = CollectionAdminRequest.addReplicaToShard("testasynccollectioncreation", "shard1")
      .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("Add replica did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.splitShard("testasynccollectioncreation")
        .setShardName("shard1")
        .processAndWait(client, MAX_TIMEOUT_SECONDS * 2);
    assertEquals("Shard split did not complete. Last recorded state: " + state, RequestStatusState.COMPLETED, state);

  }

  @Test
  public void testAsyncRequests() throws Exception {

    final String collection = "testAsyncOperations";
    final CloudSolrClient client = cluster.getSolrClient();

    RequestStatusState state = CollectionAdminRequest.createCollection(collection,"conf1",1,1)
        .setRouterName("implicit")
        .setShards("shard1")
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
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
    client.add(collection, docs);
    client.commit(collection);

    SolrQuery query = new SolrQuery("*:*");
    query.set("shards", "shard1");
    assertEquals(numDocs, client.query(collection, query).getResults().getNumFound());

    state = CollectionAdminRequest.reloadCollection(collection)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("ReloadCollection did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.createShard(collection,"shard2")
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("CreateShard did not complete", RequestStatusState.COMPLETED, state);

    //Add a doc to shard2 to make sure shard2 was created properly
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", numDocs + 1);
    doc.addField("_route_", "shard2");
    client.add(collection, doc);
    client.commit(collection);
    query = new SolrQuery("*:*");
    query.set("shards", "shard2");
    assertEquals(1, client.query(collection, query).getResults().getNumFound());

    state = CollectionAdminRequest.deleteShard(collection,"shard2").processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("DeleteShard did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.addReplicaToShard(collection, "shard1")
      .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("AddReplica did not complete", RequestStatusState.COMPLETED, state);

    //cloudClient watch might take a couple of seconds to reflect it
    Slice shard1 = client.getZkStateReader().getClusterState().getCollection(collection).getSlice("shard1");
    int count = 0;
    while (shard1.getReplicas().size() != 2) {
      if (count++ > 1000) {
        fail("2nd Replica not reflecting in the cluster state");
      }
      Thread.sleep(100);
    }

    state = CollectionAdminRequest.createAlias("myalias",collection)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("CreateAlias did not complete", RequestStatusState.COMPLETED, state);

    query = new SolrQuery("*:*");
    query.set("shards", "shard1");
    assertEquals(numDocs, client.query("myalias", query).getResults().getNumFound());

    state = CollectionAdminRequest.deleteAlias("myalias")
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("DeleteAlias did not complete", RequestStatusState.COMPLETED, state);

    try {
      client.query("myalias", query);
      fail("Alias should not exist");
    } catch (SolrException e) {
      //expected
    }

    Replica replica = shard1.getReplicas().iterator().next();
    for (String liveNode : client.getZkStateReader().getClusterState().getLiveNodes()) {
      if (!replica.getNodeName().equals(liveNode)) {
        state = new CollectionAdminRequest.MoveReplica(collection, replica.getName(), liveNode)
            .processAndWait(client, MAX_TIMEOUT_SECONDS);
        assertSame("MoveReplica did not complete", RequestStatusState.COMPLETED, state);
        break;
      }
    }

    shard1 = client.getZkStateReader().getClusterState().getCollection(collection).getSlice("shard1");
    String replicaName = shard1.getReplicas().iterator().next().getName();
    state = CollectionAdminRequest.deleteReplica(collection, "shard1", replicaName)
      .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("DeleteReplica did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.deleteCollection(collection)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("DeleteCollection did not complete", RequestStatusState.COMPLETED, state);
  }

}
