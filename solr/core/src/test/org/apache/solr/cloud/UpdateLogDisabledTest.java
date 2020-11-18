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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateLogDisabledTest extends SolrCloudTestCase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String COLLECTION_NAME = "ulog-disabled-test-collection";

    @BeforeClass
    public static void setupCluster() throws Exception {
        System.setProperty("metricsEnabled", "true");
        configureCluster(1)
                .addConfig("conf", configset("cloud-minimal-update-log-disabled"))
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

    CloudSolrClient createCollection(String collectionName, int repFactor) throws Exception {

        CollectionAdminRequest
                .createCollection(collectionName, "conf", 1, repFactor)
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

    void doLiveSplitShard(String collectionName, int repFactor, int nThreads) throws Exception {
        final CloudSolrClient client = createCollection(collectionName, repFactor);

        final ConcurrentHashMap<String,Long> model = new ConcurrentHashMap<>();  // what the index should contain
        final AtomicBoolean doIndex = new AtomicBoolean(true);
        final AtomicInteger docsAttemptedIndex = new AtomicInteger();
        final AtomicInteger docsFailedIndex = new AtomicInteger();

        final AtomicInteger docsIndexed = new AtomicInteger();
        int docCountBeforeSplit;
        Thread[] indexThreads = new Thread[nThreads];
        try {

            for (int i=0; i<nThreads; i++) {
                indexThreads[i] = new Thread(() -> {
                    while (doIndex.get()) {
                        try {
                            // Thread.sleep(10);  // cap indexing rate at 100 docs per second per thread
                            int currDoc = docsAttemptedIndex.incrementAndGet();
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
                            docsIndexed.incrementAndGet();
                        } catch (Exception e) {
                            assertTrue(e.getMessage().contains("The core associated with this request is currently rejecting requests."));
                            docsFailedIndex.incrementAndGet();
                            break;
                        }
                    }
                });
            }

            for (Thread thread : indexThreads) {
                thread.start();
            }

            Thread.sleep(100);  // wait for a few docs to be indexed before invoking split
            docCountBeforeSplit = model.size();

            CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
                    .setShardName("shard1");
            splitShard.process(client);
            waitForState("Timed out waiting for sub shards to be active.",
                    collectionName, activeClusterShape(2, 3*repFactor));  // 2 repFactor for the new split shards, 1 repFactor for old replicas

            // make sure that docs were able to be indexed during the split
            assertTrue(model.size() > docCountBeforeSplit);
            assertTrue(docsAttemptedIndex.get() > docsIndexed.get());
            assertTrue(docsFailedIndex.get() > 0);
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
            log.error("MISSING DOCUMENTS: {}", leftover);
        }

        assertEquals("Documents are missing!", docsIndexed.get(), numDocs);
        log.info("Number of documents indexed and queried : {}", numDocs);
    }



    @Test
    public void testLiveSplit() throws Exception {
        // Debugging tips: if this fails, it may be easier to debug by lowering the number fo threads to 1 and looping the test
        // until you get another failure.
        // You may need to further instrument things like DistributedZkUpdateProcessor to display the cluster state for the collection, etc.
        // Using more threads increases the chance to hit a concurrency bug, but too many threads can overwhelm single-threaded buffering
        // replay after the low level index split and result in subShard leaders that can't catch up and
        // become active (a known issue that still needs to be resolved.)
        doLiveSplitShard(COLLECTION_NAME, 1, 4);
    }


}
