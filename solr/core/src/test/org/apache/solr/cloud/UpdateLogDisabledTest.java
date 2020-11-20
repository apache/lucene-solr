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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.SolrIndexSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that updates are not lost silently when UpdateLog is disabled.
 */
public class UpdateLogDisabledTest extends SolrCloudTestCase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @BeforeClass
    public static void setupCluster() throws Exception {
        System.setProperty("metricsEnabled", "true");
        configureCluster(1)
                .addConfig("ulogDisabled", configset("cloud-minimal-update-log-disabled"))
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

    /**
     * Create a collection using the test configset with updateLog disabled.
     * @return the CloudSolrClient for the test cluster
     * @throws Exception
     */
    CloudSolrClient createCollection(String collectionName, int repFactor) throws Exception {
        CollectionAdminRequest
                .createCollection(collectionName, "ulogDisabled", 1, repFactor)
                .process(cluster.getSolrClient());

        cluster.waitForActiveCollection(collectionName, 1, repFactor);

        CloudSolrClient client = cluster.getSolrClient();
        client.setDefaultCollection(collectionName);
        return client;
    }

    /**
     * Return the number of docs indexed to the default collection, created by createCollection method.
     * @param client for the test cluster
     * @return number of documents indexed
     * @throws Exception
     */
    long getNumDocs(CloudSolrClient client) throws Exception {
        String collectionName = client.getDefaultCollection();
        DocCollection collection = client.getZkStateReader().getClusterState().getCollection(collectionName);
        Collection<Slice> slices = collection.getSlices();

        long totCount = 0;
        final HashSet<String> actual = new HashSet<>();
        for (Slice slice : slices) {
            if (!slice.getState().equals(Slice.State.ACTIVE)) continue;
            long lastReplicaCount = -1;
            for (Replica replica : slice.getReplicas()) {
                SolrClient replicaClient = getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName());
                long numFound = 0;
                try {
                    SolrDocumentList docsFound = replicaClient.query(params("q", "*:*", "distrib", "false")).getResults();
                    for (SolrDocument document : docsFound) {
                        actual.add(document.getFieldValue("id").toString());
                    }
                    numFound = docsFound.getNumFound();
                    log.info("Replica count={} for {}", numFound, replica);
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

    /**
     * Test that a collection created with updateLog disabled rejects updates during split.
     * @throws Exception
     */
    @Test
    public void testLiveSplit() throws Exception {
        String collectionName = "testLiveSplit";
        int repFactor = 1;
        int nThreads = 8;
        final CloudSolrClient client = createCollection(collectionName, repFactor);

        // what the index should and should not contain
        final ConcurrentHashMap<String,Long> model = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String,Long> attempted = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String,Long> failed = new ConcurrentHashMap<>();

        // variables for managing concurrent updates
        final AtomicBoolean doIndex = new AtomicBoolean(true);
        final AtomicInteger docsIndexed = new AtomicInteger();
        final AtomicInteger docsIndexAttempted = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();

        long numDocsBefore = getNumDocs(client);
        log.info("Number of docs indexed before threads are kicked off: " + numDocsBefore);

        // allows waiting for a given number of updates
        final AtomicReference<CountDownLatch> updateLatch = new AtomicReference<>(new CountDownLatch(random().nextInt(4)));
        Thread[] indexThreads = new Thread[nThreads];
        try {
            for (int i=0; i<nThreads; i++) {
                indexThreads[i] = new Thread(() -> {
                    while (doIndex.get()) {
                        String docId = "doc_x";
                        try {
                            int currDoc = docsIndexAttempted.incrementAndGet();
                            docId = "doc_" + currDoc;
                            attempted.put(docId, 1L);

                            UpdateRequest updateReq = new UpdateRequest();
                            updateReq.add(sdoc("id", docId));
                            UpdateResponse ursp = updateReq.process(client, collectionName);

                            if (ursp.getStatus() == 0) {
                                model.put(docId, 1L);  // in the future, keep track of a version per document and reuse ids to keep index from growing too large
                                docsIndexed.incrementAndGet();
                            } else {

                            }
                        } catch (Exception e) {
                            assertTrue(e.getMessage().contains("The core associated with this request is currently rejecting requests."));
                            failures.incrementAndGet();
                            failed.put(docId, 1L);
                        }
                        updateLatch.get().countDown();
                    }
                });
            }

            // kick off indexing threads
            for (Thread thread : indexThreads) {
                thread.start();
            }

            // wait for some documents to be indexed before splitting
            updateLatch.get().await();
            CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
                    .setShardName("shard1").setSplitMethod(SolrIndexSplitter.SplitMethod.LINK.name());
            splitShard.process(client);
            waitForState("Timed out waiting for sub shards to be active.",
                    collectionName, activeClusterShape(2, 3 * repFactor));  // 2 repFactor for the new split shards, 1 repFactor for old replicas

            // wait for a few more docs to be indexed after split
            updateLatch.set(new CountDownLatch(random().nextInt(4)));
            updateLatch.get().await();
        } finally {
            // shut down the indexers
            doIndex.set(false);
            for (Thread thread : indexThreads) {
                thread.join();
            }
        }

        // final commit is needed for visibility
        client.commit();

        // ensure that documents either failed or succeeded
        assertFalse(model.keySet().removeAll(failed.keySet()));

        // verify that number of docs accessible to client matches those successfully indexed
        long numDocs = getNumDocs(client);
        HashSet<String> indexedNotInModel = new HashSet<>();
        HashSet<String> indexedAndInFailed = new HashSet<>();
        HashSet<String> indexedAndAttempted = new HashSet<>();
        if (numDocs != model.size()) {
            long numIndexed = docsIndexed.get();
            SolrDocumentList results = client.query(new SolrQuery("q","*:*", "fl","id", "rows",
                    Long.toString(Long.max(numDocs,numIndexed)))).getResults();
            for (SolrDocument doc : results) {
                String id = (String) doc.get("id");
                if (!model.containsKey(id)) {
                    indexedNotInModel.add(id);
                }
                if (failed.containsKey(id)) {
                    indexedAndInFailed.add(id);
                }
                if (!attempted.containsKey(id)) {
                    indexedAndAttempted.add(id);
                }
            }
            log.error("Outersection with failed: " + indexedAndInFailed + ", outersection with model: " + indexedNotInModel);
        }

        assertEquals("Documents are missing!", docsIndexed.get(), numDocs);
        log.info("Number of documents indexed and queried : " + numDocs + " failures during splitting=" + failures.get());
    }

    /**
     * Create a core, start rejecting updates, and ensure that indexing requests fail.
     * @throws Exception
     */
    @Test
    public void testRequestFailsWhenRejecting() throws Exception {
        SolrCore core = null;
        try {
            String collectionName = "testRequestFailedWhenRejecting";
            createCollection(collectionName, 1);

            // wait for collection to be created
            ClusterState clusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
            DocCollection collection = clusterState.getCollection(collectionName);
            Slice shard = collection.getSlice("shard1");
            CoreContainer cc = null;
            for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
                if (solrRunner.getNodeName().equals(shard.getLeader().getNodeName())) {
                    cc = solrRunner.getCoreContainer();
                }
            }

            assertNotNull("Core container not found.", cc);

            // attempt to index a document - should succeed
            SolrInputDocument doc1 = new SolrInputDocument();
            doc1.setField("id", "1");
            UpdateRequest req = new UpdateRequest();
            req.add(doc1);
            UpdateResponse resp = req.commit(cluster.getSolrClient(), collectionName);

            assertEquals(resp.getStatus(), 0);

            // start rejecting updates for this core
            core = cc.getCore(shard.getLeader().getCoreName());
            core.getUpdateHandler().startRejectingUpdates();

            // attempt to index a document - should fail
            SolrInputDocument doc2 = new SolrInputDocument();
            doc2.setField("id", "2");
            req = new UpdateRequest();
            req.add(doc2);
            req.commit(cluster.getSolrClient(), collectionName);

            fail("Update request should have failed with exception");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("The core associated with this request is currently rejecting requests."));
        } finally {
            if (core != null) {
                core.close();
            }
        }
    }

    /**
     * Create a core, start rejecting updates; then stop rejecting updates, and ensure that indexing requests succeed.
     * @throws Exception
     */
    @Test
    public void testRequestSucceedsWhenNotRejecting() throws Exception {
        SolrCore core = null;
        try {
            String collectionName = "testRequestFailedWhenRejecting";
            createCollection(collectionName, 1);

            // get core container for node with leader replica
            ClusterState clusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
            DocCollection collection = clusterState.getCollection(collectionName);
            Slice shard = collection.getSlice("shard1");
            CoreContainer cc = null;
            for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
                if (solrRunner.getNodeName().equals(shard.getLeader().getNodeName())) {
                    cc = solrRunner.getCoreContainer();
                }
            }
            assertNotNull("Core container not found.", cc);

            // attempt to index a document - should succeed
            SolrInputDocument doc1 = new SolrInputDocument();
            doc1.setField("id", "1");
            UpdateRequest req = new UpdateRequest();
            req.add(doc1);
            UpdateResponse resp = req.commit(cluster.getSolrClient(), collectionName);
            assertEquals(resp.getStatus(), 0);

            // start rejecting updates for this core
            core = cc.getCore(shard.getLeader().getCoreName());
            core.getUpdateHandler().startRejectingUpdates();

            // stop rejecting updates for this core
            core.getUpdateHandler().stopRejectingUpdates();

            // attempt to index another document - should succeed
            SolrInputDocument doc2 = new SolrInputDocument();
            doc2.setField("id", "2");
            req = new UpdateRequest();
            req.add(doc2);
            resp = req.commit(cluster.getSolrClient(), collectionName);

            // assert that both docs were successfully indexed
            assertEquals(resp.getStatus(), 0);
            assertEquals(2, getNumDocs(cluster.getSolrClient()));
        } catch (Exception e) {
            fail("Encountered unexpected exception: " + e.getMessage());
        } finally {
            if (core != null) {
                core.close();
            }
        }
    }
}
