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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Cloud Collections API.
 */
@Slow
public class CollectionsAPIAsyncDistributedZkTest extends SolrCloudTestCase {

  private static final int MAX_TIMEOUT_SECONDS = 90;
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Before
  public void setupCluster() throws Exception {
    // we recreate per test - they need to be isolated to be solid
    useFactory(null);
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    shutdownCluster();
  }

  @Test
  public void testSolrJAPICalls() throws Exception {

    final CloudHttp2SolrClient client = cluster.getSolrClient();

    CollectionAdminRequest.createCollection("testasynccollectioncreation", "conf1", 1, 1).setMaxShardsPerNode(200).process(client);

    RequestStatusState state = CollectionAdminRequest.createCollection("testasynccollectioncreation", "conf1", 1, 1).processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("Recreating a collection with the same should have failed.", RequestStatusState.FAILED, state);

    state = CollectionAdminRequest.addReplicaToShard("testasynccollectioncreation", "s1").processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("Add replica did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.splitShard("testasynccollectioncreation").setShardName("s1").processAndWait(client, MAX_TIMEOUT_SECONDS * 2);

    assertEquals("Shard split did not complete. Last recorded state: " + state, RequestStatusState.COMPLETED, state);
  }

  @Test
  @Nightly // slow, processAndWait still polls ...
  public void testAsyncRequests() throws Exception {
    final String collection = "testAsyncOperations";
    final CloudHttp2SolrClient client = cluster.getSolrClient();

    RequestStatusState state = CollectionAdminRequest.createCollection(collection,"conf1",1,1)
        .setRouterName("implicit")
        .setShards("s1")
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("CreateCollection task did not complete!", RequestStatusState.COMPLETED, state);

    
    cluster.waitForActiveCollection(collection, 1, 1);
    
    //Add a few documents to shard1
    int numDocs = TestUtil.nextInt(random(), 10, 100);
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i=0; i<numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("_route_", "s1");
      docs.add(doc);
    }
    client.add(collection, docs);
    client.commit(collection);

    SolrQuery query = new SolrQuery("*:*");
    query.set("shards", "s1");
    assertEquals(numDocs, client.query(collection, query).getResults().getNumFound());

    state = CollectionAdminRequest.reloadCollection(collection)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("ReloadCollection did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.createShard(collection,"s2")
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("CreateShard did not complete", RequestStatusState.COMPLETED, state);

    //Add a doc to shard2 to make sure shard2 was created properly
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", numDocs + 1);
    doc.addField("_route_", "s2");
    client.add(collection, doc);
    client.commit(collection);
    query = new SolrQuery("*:*");
    query.set("shards", "s2");
    assertEquals(1, client.query(collection, query).getResults().getNumFound());

    state = CollectionAdminRequest.deleteShard(collection,"s2").processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("DeleteShard did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.addReplicaToShard(collection, "s1")
      .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("AddReplica did not complete", RequestStatusState.COMPLETED, state);

    //cloudClient watch might take a couple of seconds to reflect it
    client.getZkStateReader().waitForState(collection, 20, TimeUnit.SECONDS, (n, c) -> {
      if (c == null)
        return false;
      Slice slice = c.getSlice("s1");
      if (slice == null) {
        return false;
      }

      if (slice.getReplicas().size() == 2) {
        return true;
      }

      return false;
    });

    state = CollectionAdminRequest.createAlias("myalias",collection)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("CreateAlias did not complete", RequestStatusState.COMPLETED, state);

    query = new SolrQuery("*:*");
    query.set("shards", "s1");
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
    
    Slice shard1 = client.getZkStateReader().getClusterState().getCollection(collection).getSlice("s1");
    Replica replica = shard1.getReplicas().iterator().next();
    for (String liveNode : client.getZkStateReader().getClusterState().getLiveNodes()) {
      if (!replica.getNodeName().equals(liveNode)) {
        state = new CollectionAdminRequest.MoveReplica(collection, replica.getName(), liveNode)
            .processAndWait(client, MAX_TIMEOUT_SECONDS);
        assertSame("MoveReplica did not complete", RequestStatusState.COMPLETED, state);
        break;
      }
    }

    shard1 = client.getZkStateReader().getClusterState().getCollection(collection).getSlice("s1");
    String replicaName = shard1.getReplicas().iterator().next().getName();
    state = CollectionAdminRequest.deleteReplica(collection, "s1", replicaName)
      .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("DeleteReplica did not complete", RequestStatusState.COMPLETED, state);

    state = CollectionAdminRequest.deleteCollection(collection)
        .processAndWait(client, MAX_TIMEOUT_SECONDS);
    assertSame("DeleteCollection did not complete", RequestStatusState.COMPLETED, state);
  }

  public void testAsyncIdRaceCondition() throws Exception {

    SolrClient[] clients = new SolrClient[cluster.getJettySolrRunners().size()];
    int j = 0;
    for (JettySolrRunner r:cluster.getJettySolrRunners()) {
      clients[j++] = new HttpSolrClient.Builder(r.getBaseUrl().toString()).build();
    }
    RequestStatusState state = CollectionAdminRequest.createCollection("testAsyncIdRaceCondition","conf1",1,1)
        .setRouterName("implicit")
        .setShards("s1")
        .processAndWait(cluster.getSolrClient(), MAX_TIMEOUT_SECONDS);
    assertSame("CreateCollection task did not complete!", RequestStatusState.COMPLETED, state);

    final AtomicInteger numSuccess = new AtomicInteger(0);
    final AtomicInteger numFailure = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch((TEST_NIGHTLY ? 10 : 3));
    
    ExecutorService es = testExecutor;
    List<Future> futures = new ArrayList<>();
    try {
      for (int i = 0; i < (TEST_NIGHTLY ? 10 : 3); i++) {
        futures.add(es.submit(new Runnable() {
          
          @Override
          public void run() {
            CollectionAdminRequest.Reload reloadCollectionRequest = CollectionAdminRequest.reloadCollection("testAsyncIdRaceCondition");
            latch.countDown();
            try {
              latch.await();
            } catch (InterruptedException e) {
              throw new RuntimeException();
            }
            
            try {
              if (log.isInfoEnabled()) {
                log.info("{} - Reloading Collection.", Thread.currentThread().getName());
              }
              reloadCollectionRequest.processAsync("repeatedId", clients[LuceneTestCase.random().nextInt(clients.length)]);
              numSuccess.incrementAndGet();
            } catch (SolrServerException e) {
              if (log.isInfoEnabled()) {
                log.info(e.getMessage());
              }
              assertEquals("Task with the same requestid already exists.", e.getMessage());
              numFailure.incrementAndGet();
            } catch (IOException e) {
              throw new RuntimeException();
            }
          }
        }));
      }

      for (Future future : futures) {
        future.get();
      }

      cluster.waitForActiveCollection("testAsyncIdRaceCondition", 1, 1);

      assertEquals(1, numSuccess.get());
      assertEquals((TEST_NIGHTLY ? 10 : 3) - 1, numFailure.get());
    } finally {
      for (int i = 0; i < clients.length; i++) {
        clients[i].close();
      }
    }
  }
}
