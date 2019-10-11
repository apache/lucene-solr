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
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitShardTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String COLLECTION_NAME = "splitshardtest-collection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.deleteAllCollections();
  }

  @Test
  public void doTest() throws IOException, SolrServerException {
    CollectionAdminRequest
        .createCollection(COLLECTION_NAME, "conf", 2, 1)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
    
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setNumSubShards(5)
        .setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState("Timed out waiting for sub shards to be active. Number of active shards=" +
            cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getActiveSlices().size(),
        COLLECTION_NAME, activeClusterShape(6, 7));

    try {
      splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME).setShardName("shard2").setNumSubShards(10);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards > 8");
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains("A shard can only be split into 2 to 8 subshards in one split request."));
    }

    try {
      splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME).setShardName("shard2").setNumSubShards(1);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards < 2");
    } catch (HttpSolrClient.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains("A shard can only be split into 2 to 8 subshards in one split request. Provided numSubShards=1"));
    }
  }

  @Test
  public void multipleOptionsSplitTest() throws IOException, SolrServerException {
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setNumSubShards(5)
        .setRanges("0-c,d-7fffffff")
        .setShardName("shard1");
    boolean expectedException = false;
    try {
      splitShard.process(cluster.getSolrClient());
      fail("An exception should have been thrown");
    } catch (SolrException ex) {
      expectedException = true;
    }
    assertTrue("Expected SolrException but it didn't happen", expectedException);
  }

  @Test
  public void testSplitFuzz() throws Exception {
    String collectionName = "splitFuzzCollection";
    CollectionAdminRequest
        .createCollection(collectionName, "conf", 2, 1)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 2, 2);

    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
        .setSplitFuzz(0.5f)
        .setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState("Timed out waiting for sub shards to be active. Number of active shards=" +
            cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName).getActiveSlices().size(),
        collectionName, activeClusterShape(3, 4));
    DocCollection coll = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName);
    Slice s1_0 = coll.getSlice("shard1_0");
    Slice s1_1 = coll.getSlice("shard1_1");
    long fuzz = ((long)Integer.MAX_VALUE >> 3) + 1L;
    long delta0 = s1_0.getRange().max - s1_0.getRange().min;
    long delta1 = s1_1.getRange().max - s1_1.getRange().min;
    long expected0 = (Integer.MAX_VALUE >> 1) + fuzz;
    long expected1 = (Integer.MAX_VALUE >> 1) - fuzz;
    assertEquals("wrong range in s1_0", expected0, delta0);
    assertEquals("wrong range in s1_1", expected1, delta1);
  }


  CloudSolrClient createCollection(String collectionName, int repFactor) throws Exception {

      CollectionAdminRequest
          .createCollection(collectionName, "conf", 1, repFactor)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, repFactor);

    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(collectionName);
    return client;
  }


  long getNumDocs(CloudSolrClient client) throws Exception {
    String collectionName = client.getDefaultCollection();
    DocCollection collection = client.getZkStateReader().getClusterState().getCollection(collectionName);
    Collection<Slice> slices = collection.getSlices();

    long totCount = 0;
    for (Slice slice : slices) {
      if (!slice.getState().equals(Slice.State.ACTIVE)) continue;
      long lastReplicaCount = -1;
      for (Replica replica : slice.getReplicas()) {
        SolrClient replicaClient = getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName());
        long numFound = 0;
        try {
          numFound = replicaClient.query(params("q", "*:*", "distrib", "false")).getResults().getNumFound();
          log.info("Replica count=" + numFound + " for " + replica);
        } finally {
          replicaClient.close();
        }
        if (lastReplicaCount >= 0) {
          assertEquals("Replica doc count for " + replica, lastReplicaCount, numFound);
        }
        lastReplicaCount = numFound;
      }
      totCount += lastReplicaCount;
    }


    long cloudClientDocs = client.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals("Sum of shard count should equal distrib query doc count", totCount, cloudClientDocs);
    return totCount;
  }


  void doLiveSplitShard(String collectionName, int repFactor) throws Exception {
    final CloudSolrClient client = createCollection(collectionName, repFactor);

    final AtomicBoolean doIndex = new AtomicBoolean(true);
    final AtomicInteger docsIndexed = new AtomicInteger();
    Thread indexThread = null;
    try {
      // start indexing client before we initiate a shard split
      indexThread = new Thread(() -> {
        while (doIndex.get()) {
          try {
            // Thread.sleep(10);  // uncomment this to cap indexing rate at 100 docs per second...
            int currDoc = docsIndexed.get();

            // Try all docs in the same update request
            UpdateRequest updateReq = new UpdateRequest();
            updateReq.add(sdoc("id", "doc_" + currDoc));
            UpdateResponse ursp = updateReq.commit(client, collectionName);
            assertEquals(0, ursp.getStatus());  // for now, don't accept any failures
            if (ursp.getStatus() == 0) {
              docsIndexed.incrementAndGet();
            }
          } catch (Exception e) {
            fail(e.getMessage());
            break;
          }
        }
      });
      indexThread.start();

      Thread.sleep(100);  // wait for a few docs to be indexed before invoking split
      int docCount = docsIndexed.get();

      CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
          .setShardName("shard1");
      splitShard.process(client);
      waitForState("Timed out waiting for sub shards to be active.",
          collectionName, activeClusterShape(2, 3*repFactor));  // 2 repFactor for the new split shards, 1 repFactor for old replicas

      // make sure that docs were able to be indexed during the split
      assertTrue(docsIndexed.get() > docCount);

      Thread.sleep(100);  // wait for a few more docs to be indexed after split

    } finally {
      // shut down the indexer
      doIndex.set(false);
      if (indexThread != null) {
        indexThread.join();
      }
    }

    assertTrue(docsIndexed.get() > 0);

    long numDocs = getNumDocs(client);
    if (numDocs != docsIndexed.get()) {
      // Find out what docs are missing.
      for (int i = 0; i < docsIndexed.get(); i++) {
        String id = "doc_" + i;
        long cloudClientDocs = client.query(new SolrQuery("id:" + id)).getResults().getNumFound();
        if (cloudClientDocs != 1) {
          log.error("MISSING DOCUMENT " + id);
        }
      }
    }

    assertEquals("Documents are missing!", docsIndexed.get(), numDocs);
    log.info("Number of documents indexed and queried : " + numDocs);
  }

  @Test
  public void testLiveSplit() throws Exception {
    doLiveSplitShard("livesplit1", 1);
  }


}
