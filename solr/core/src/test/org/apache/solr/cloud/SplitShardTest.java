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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
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
    configureCluster(5)
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
        // screen out inactive replicas
        if (!replica.getState().equals(Replica.State.ACTIVE)) continue;
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

  void doLiveSplitShard(String collectionName, int repFactor, int nThreads) throws Exception {
    final CloudSolrClient client = createCollection(collectionName, repFactor);

    final ConcurrentHashMap<String,Long> model = new ConcurrentHashMap<>();  // what the index should contain
    final AtomicBoolean doIndex = new AtomicBoolean(true);
    final AtomicInteger docsIndexed = new AtomicInteger();
    Thread[] indexThreads = new Thread[nThreads];
    try {

      for (int i=0; i<nThreads; i++) {
        indexThreads[i] = new Thread(() -> {
          while (doIndex.get()) {
            try {
              // Thread.sleep(10);  // cap indexing rate at 100 docs per second per thread
              int currDoc = docsIndexed.incrementAndGet();
              String docId = "doc_" + currDoc;

              // Try all docs in the same update request
              UpdateRequest updateReq = new UpdateRequest();
              updateReq.add(sdoc("id", docId));
              // UpdateResponse ursp = updateReq.commit(client, collectionName);  // uncomment this if you want a commit each time
              UpdateResponse ursp = updateReq.process(client, collectionName);
              assertEquals(0, ursp.getStatus());  // for now, don't accept any failures
              if (ursp.getStatus() == 0) {
                model.put(docId, 1L);  // in the future, keep track of a version per document and reuse ids to keep index from growing too large
              }
            } catch (Exception e) {
              fail(e.getMessage());
              break;
            }
          }
        });
      }

      for (Thread thread : indexThreads) {
        thread.start();
      }

      Thread.sleep(100);  // wait for a few docs to be indexed before invoking split
      int docCount = model.size();

      CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
          .setShardName("shard1");
      splitShard.process(client);
      waitForState("Timed out waiting for sub shards to be active.",
          collectionName, activeClusterShape(2, 3*repFactor));  // 2 repFactor for the new split shards, 1 repFactor for old replicas

      // make sure that docs were able to be indexed during the split
      assertTrue(model.size() > docCount);

      Thread.sleep(100);  // wait for a few more docs to be indexed after split

    } finally {
      // shut down the indexers
      doIndex.set(false);
      for (Thread thread : indexThreads) {
        thread.join();
      }
    }

    client.commit();  // final commit is needed for visibility

    long numDocs = getNumDocs(client);
    if (numDocs != model.size()) {
      SolrDocumentList results = client.query(new SolrQuery("q","*:*", "fl","id", "rows", Integer.toString(model.size()) )).getResults();
      Map<String,Long> leftover = new HashMap<>(model);
      for (SolrDocument doc : results) {
        String id = (String) doc.get("id");
        leftover.remove(id);
      }
      log.error("MISSING DOCUMENTS: " + leftover);
    }

    assertEquals("Documents are missing!", docsIndexed.get(), numDocs);
    log.info("Number of documents indexed and queried : " + numDocs);
  }



  @Test
  public void testLiveSplit() throws Exception {
    // Debugging tips: if this fails, it may be easier to debug by lowering the number fo threads to 1 and looping the test
    // until you get another failure.
    // You may need to further instrument things like DistributedZkUpdateProcessor to display the cluster state for the collection, etc.
    // Using more threads increases the chance to hit a concurrency bug, but too many threads can overwhelm single-threaded buffering
    // replay after the low level index split and result in subShard leaders that can't catch up and
    // become active (a known issue that still needs to be resolved.)
    doLiveSplitShard("livesplit1", 1, 4);
  }


  void doLiveSplitShardFail(String collectionName, int repFactor, int nThreads) throws Exception {
    final boolean doSplit = true;  // test debugging aid: set to false if you want to check that the test passes if we don't do a split
    final boolean updateFailureOK = true;  // we will get failures to client when killing a node
    final CloudSolrClient client = createCollection(collectionName, repFactor);

    final ConcurrentHashMap<String,Long> model = new ConcurrentHashMap<>();  // what the index should contain
    final AtomicBoolean doIndex = new AtomicBoolean(true);
    final AtomicInteger docsIndexed = new AtomicInteger();
    final AtomicInteger failures = new AtomicInteger();
    // allows waiting for a given number of update requests, regardless of if they succeed for fail
    final AtomicReference<CountDownLatch> updateLatch = new AtomicReference<>(new CountDownLatch(random().nextInt(4)));
    final AtomicReference<CountDownLatch> updateSuccessLatch = new AtomicReference<>(new CountDownLatch(0));
    Thread[] indexThreads = new Thread[nThreads];
    try {

      for (int i=0; i<nThreads; i++) {
        indexThreads[i] = new Thread(() -> {
          while (doIndex.get()) {
            try {
              // Thread.sleep(10);  // cap indexing rate at 100 docs per second per thread
              int currDoc = docsIndexed.incrementAndGet();
              String docId = "doc_" + currDoc;

              // Try all docs in the same update request
              UpdateRequest updateReq = new UpdateRequest();
              updateReq.add(sdoc("id", docId));

              UpdateResponse ursp;
              if (false && random().nextInt(4)==0) { // add commit 25% of the time  // nocommit... turned off for now
                ursp = updateReq.commit(client, collectionName);
              } else {
                ursp = updateReq.process(client, collectionName);
              }

              updateLatch.get().countDown();
              if (ursp.getStatus() == 0) {
                model.put(docId, 1L);  // in the future, keep track of a version per document and reuse ids to keep index from growing too large
                updateSuccessLatch.get().countDown();
              } else {
                failures.incrementAndGet();
                if (!updateFailureOK) {
                  assertEquals(0, ursp.getStatus());
                }
              }
            } catch (Exception e) {
              if (!updateFailureOK) {
                fail(e.getMessage());
                break;
              }
              updateLatch.get().countDown();  // do this on exception as well so we don't get stuck
              failures.incrementAndGet();
            }
          }
        });
      }

      for (Thread thread : indexThreads) {
        thread.start();
      }

      updateLatch.get().await(); // wait for some documents to be indexed

      if (doSplit) {
        CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
            .setShardName("shard1");
        splitShard.process(client);

        Replica leader = client.getZkStateReader().getLeader(collectionName, "shard1");
        JettySolrRunner leaderJetty = cluster.getReplicaJetty(leader);
        log.warn("STOPPING " + leaderJetty);
        leaderJetty.stop();
      }

      // wait for some update successes
      updateSuccessLatch.set(new CountDownLatch(10));
      if (!updateSuccessLatch.get().await(1, TimeUnit.MINUTES)) {
        log.error("CLUSTER STATE AFTER WAITING:\n" + client.getZkStateReader().getClusterState().getCollection(collectionName));
        fail("FAILED to index more documents");
      }

    } finally {
      // shut down the indexers
      doIndex.set(false);
      for (Thread thread : indexThreads) {
        thread.join();
      }
    }

    client.commit();  // final commit is needed for visibility

    log.info("END CLUSTER STATE:\n" + client.getZkStateReader().getClusterState().getCollection(collectionName));

    // Index can legitimately have more documents than our model because of failed requests that actually succeeded.
    // For this reason, don't limit rows by the model size, but by the actual number of docs.  It would be really nice if "-1" worked for "give me all"...
    long numDocs = getNumDocs(client);
    SolrDocumentList results = client.query(new SolrQuery("DBG","FINAL_QUERY", "q","*:*", "fl","id", "rows", Long.toString(numDocs*2))).getResults();
    Map<String,Long> leftover = new HashMap<>(model);
    for (SolrDocument doc : results) {
      String id = (String) doc.get("id");
      leftover.remove(id);
    }
    if (leftover.size() > 0) {
      log.error("MISSING DOCUMENTS: " + leftover);
    }

    log.info("Number of documents attempted to be indexed : " + numDocs + " update request failures=" + failures.get());

    assertTrue("Documents are missing! " + leftover, leftover.size() == 0);
  }

  public void testLiveSplitFail() throws Exception {
    // Debugging tips: if this fails, it may be easier to debug by lowering the number fo threads to 1 and looping the test
    // until you get another failure.
    // You may need to further instrument things like DistributedZkUpdateProcessor to display the cluster state for the collection, etc.
    // Using more threads increases the chance to hit a concurrency bug, but too many threads can overwhelm single-threaded buffering
    // replay after the low level index split and result in subShard leaders that can't catch up and
    // become active (a known issue that still needs to be resolved.)
    doLiveSplitShardFail("c99", 2, 8);
  }

}
