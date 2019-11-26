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
package org.apache.solr.store.blob;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for shard splitting in conjunction with shared storage
 */
public class SharedStorageSplitTest extends SolrCloudSharedStoreTestCase  {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Map<String, Map<String, CountDownLatch>> solrProcessesTaskTracker = new HashMap<>();
  
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(2)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
    
    // we don't use this in testing
    Path sharedStoreRootPath = createTempDir("tempDir");
    CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    // configure same client for each runner, this isn't a concurrency test so this is fine
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), runner);
      solrProcessesTaskTracker.put(runner.getNodeName(), configureTestBlobProcessForNode(runner));
    }
  }
  
  
  @AfterClass
  public static void teardownTest() throws Exception {
    shutdownCluster();
  }

  CloudSolrClient createCollection(String collectionName, boolean sharedStorage, int repFactor) throws Exception {
    if (sharedStorage) {
      CollectionAdminRequest
          .createCollection(collectionName, "conf", 1, 0, 0, 0)
          .setMaxShardsPerNode(100)
          .setSharedIndex(true)
          .setSharedReplicas(repFactor)
          .process(cluster.getSolrClient());
    } else {
      CollectionAdminRequest
          .createCollection(collectionName, "conf", 1, repFactor)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());
    }

    cluster.waitForActiveCollection(collectionName, 1, repFactor);

    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(collectionName);
    return client;
  }

  private void indexPrefixDocs(CloudSolrClient client, String collectionName, int nPrefixes, int nDocsPerPrefix, int docOffset) throws Exception {
    if (random().nextBoolean()) {
      // index docs separately
      for (int i = 0; i < nPrefixes; i++) {
        String prefix = "a" + i;
        for (int j = 0; j < nDocsPerPrefix; j++) {
          client.add(sdoc("id", prefix + "!doc" + (j+docOffset)));
        }
      }
      if (random().nextBoolean()) {
        client.commit(collectionName, true, true, false);
      }
    } else {
      // Try all docs in the same update request
      UpdateRequest updateReq = new UpdateRequest();
      for (int i = 0; i < nPrefixes; i++) {
        String prefix = "a" + i;
        for (int j = 0; j < nDocsPerPrefix; j++) {
          updateReq.add(sdoc("id", prefix + "!doc" + (j+docOffset)));
        }
      }
      UpdateResponse ursp;
      if (random().nextBoolean()) {
        ursp = updateReq.commit(client, collectionName);
      } else {
        ursp = updateReq.process(client, collectionName);
      }
      assertEquals(0, ursp.getStatus());
    }
  }

  void doSplitShard(String collectionName, boolean sharedStorage, int repFactor, int nPrefixes, int nDocsPerPrefix) throws Exception {
    CloudSolrClient client = createCollection(collectionName, sharedStorage, repFactor);

    /*** TODO: this currently causes a failure due to a NPE.  Uncomment when fixed.
    if (random().nextBoolean()) {
      // start off with a commit
      client.commit(collectionName, true, true, false);
    }
     ***/

    indexPrefixDocs(client, collectionName, nPrefixes, nDocsPerPrefix, 0);

    assertEquals(nPrefixes * nDocsPerPrefix, getNumDocs(client,sharedStorage,repFactor));

    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
        .setSplitByPrefix(true)
        .setShardName("shard1");
    splitShard.process(client);
    waitForState("Timed out waiting for sub shards to be active.",
        collectionName, activeClusterShape(2, 3*repFactor));  // 2 repFactor for the new split shards, 1 repFactor for old replicas

    // now index another batch of docs into the new shards
    indexPrefixDocs(client, collectionName, nPrefixes, nDocsPerPrefix, nDocsPerPrefix);
    assertEquals(nPrefixes * nDocsPerPrefix * 2, getNumDocs(client,sharedStorage,repFactor));
  }

  long getNumDocs(CloudSolrClient client, boolean sharedStorage, int repFactor) throws Exception {
    String collectionName = client.getDefaultCollection();
    DocCollection collection = client.getZkStateReader().getClusterState().getCollection(collectionName);
    Collection<Slice> slices = collection.getSlices();

    if (sharedStorage && repFactor > 1) {
      // set up count down latches to wait for pulls to complete
      List<CountDownLatch> latches = new LinkedList<>();

      for (Slice slice : slices) {
        CountDownLatch cdl = new CountDownLatch(slice.getReplicas().size());
        latches.add(cdl);
        for (Replica replica : slice.getReplicas()) {
          // ensure we count down for all replicas per slice.
          String sharedShardName = (String) slice.getProperties().get(ZkStateReader.SHARED_SHARD_NAME);
          solrProcessesTaskTracker.get(replica.getNodeName())
              .put(replica.getCoreName(), cdl);
          SolrClient replicaClient = getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName());
          try {
            replicaClient.query(params("q", "*:* priming pull", "distrib", "false"));
          } finally {
            replicaClient.close();
          }
        }
      }

      for (CountDownLatch latch : latches) {
        assertTrue(latch.await(60, TimeUnit.SECONDS));
      }

    }

    long totCount = 0;
    for (Slice slice : slices) {
      if (!slice.getState().equals(Slice.State.ACTIVE)) continue;
      long lastReplicaCount = -1;
      for (Replica replica : slice.getReplicas()) {
        SolrClient replicaClient = getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName());
        long numFound = 0;
        try {
          numFound = replicaClient.query(params("q", "*:*", "distrib", "false")).getResults().getNumFound();
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

  @Test
  public void testSplit() throws Exception {
    doSplitShard("c1", true, 1, 2, 2);
    doSplitShard("c2", true, 2, 2, 2);
  }

  void doLiveSplitShard(String collectionName, boolean sharedStorage, int repFactor, int nThreads) throws Exception {
    final boolean doSplit = true;  // test debugging aid: set to false if you want to check that the test passes if we don't do a split
    final boolean updateFailureOK = true;  // TODO: this should be changed to false after the NPE bug is fixed
    final CloudSolrClient client = createCollection(collectionName, sharedStorage, repFactor);

    final ConcurrentHashMap<String,Long> model = new ConcurrentHashMap<>();  // what the index should contain
    final AtomicBoolean doIndex = new AtomicBoolean(true);
    final AtomicInteger docsIndexed = new AtomicInteger();
    final AtomicInteger failures = new AtomicInteger();
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
              UpdateResponse ursp = updateReq.commit(client, collectionName);  // uncomment this if you want a commit each time
              // UpdateResponse ursp = updateReq.process(client, collectionName);

              if (ursp.getStatus() == 0) {
                model.put(docId, 1L);  // in the future, keep track of a version per document and reuse ids to keep index from growing too large
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
              failures.incrementAndGet();
            }
          }
        });
      }

      for (Thread thread : indexThreads) {
        thread.start();
      }

      Thread.sleep(100);  // wait for a few docs to be indexed before invoking split
      int docCount = model.size();

      if (doSplit) {
        CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
            .setShardName("shard1");
        splitShard.process(client);
        waitForState("Timed out waiting for sub shards to be active.",
            collectionName, activeClusterShape(2, 3 * repFactor));  // 2 repFactor for the new split shards, 1 repFactor for old replicas
      } else {
        Thread.sleep(10 * 1000);
      }

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

    long numDocs = getNumDocs(client, true, repFactor);
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
    log.info("Number of documents indexed and queried : " + numDocs + " failures during splitting=" + failures.get());
  }


  // TODO: this test is adapted from SplitShardTest.testLiveSplit and could perhaps
  // be unified.
  @Test
  public void testLiveSplit() throws Exception {
    // Debugging tips: if this fails, it may be easier to debug by lowering the number fo threads to 1 and looping the test
    // until you get another failure.
    // You may need to further instrument things like DistributedZkUpdateProcessor to display the cluster state for the collection, etc.
    // Using more threads increases the chance to hit a concurrency bug, but too many threads can overwhelm single-threaded buffering
    // replay after the low level index split and result in subShard leaders that can't catch up and
    // become active (a known issue that still needs to be resolved.)
    doLiveSplitShard("livesplit1", true, 1, 8);
  }
}
