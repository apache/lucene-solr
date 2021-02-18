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

package org.apache.solr.handler;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.UpdateParams;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.solr.handler.TestStressThreadBackup.makeDoc;

//@LuceneTestCase.Nightly
@LuceneTestCase.SuppressCodecs({"SimpleText"})
public class TestStressIncrementalBackup extends SolrCloudTestCase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Path backupPath;
    private SolrClient adminClient;
    private SolrClient coreClient;
    
    @Before
    public void beforeTest() throws Exception {
        backupPath = createTempDir(getTestClass().getSimpleName() + "_backups");
        System.setProperty("solr.allowPaths", backupPath.toString());

        // NOTE: we don't actually care about using SolrCloud, but we want to use SolrClient and I can't
        // bring myself to deal with the nonsense that is SolrJettyTestBase.

        // We do however explicitly want a fresh "cluster" every time a test is run
        configureCluster(1)
                .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
                .configure();

        assertEquals(0, (CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME, "conf1", 1, 1)
                .process(cluster.getSolrClient()).getStatus()));
        adminClient = getHttpSolrClient(cluster.getJettySolrRunners().get(0).getBaseUrl().toString());
        initCoreNameAndSolrCoreClient();
    }

    private void initCoreNameAndSolrCoreClient() {
        // Sigh.
        Replica r = cluster.getSolrClient().getZkStateReader().getClusterState()
                .getCollection(DEFAULT_TEST_COLLECTION_NAME).getActiveSlices().iterator().next()
                .getReplicas().iterator().next();
        coreName = r.getCoreName();
        coreClient = getHttpSolrClient(r.getCoreUrl());
    }

    @After
    public void afterTest() throws Exception {
        // we use a clean cluster instance for every test, so we need to clean it up
        shutdownCluster();

        if (null != adminClient) {
            adminClient.close();
        }
        if (null != coreClient) {
            coreClient.close();
        }

        System.clearProperty("solr.allowPaths");
    }

    public void testCoreAdminHandler() throws Exception {
        final int numBackupIters = 20; // don't use 'atLeast', we don't want to blow up on nightly

        final AtomicReference<Throwable> heavyCommitFailure = new AtomicReference<>();
        final AtomicBoolean keepGoing = new AtomicBoolean(true);

        // this thread will do nothing but add/commit new 'dummy' docs over and over again as fast as possible
        // to create a lot of index churn w/ segment merging
        final Thread heavyCommitting = new Thread() {
            public void run() {
                try {
                    int docIdCounter = 0;
                    while (keepGoing.get()) {
                        docIdCounter++;

                        final UpdateRequest req = new UpdateRequest().add(makeDoc("dummy_" + docIdCounter, "dummy"));
                        // always commit to force lots of new segments
                        req.setParam(UpdateParams.COMMIT,"true");
                        req.setParam(UpdateParams.OPEN_SEARCHER,"false");           // we don't care about searching

                        // frequently forceMerge to ensure segments are frequently deleted
                        if (0 == (docIdCounter % 13)) {                             // arbitrary
                            req.setParam(UpdateParams.OPTIMIZE, "true");
                            req.setParam(UpdateParams.MAX_OPTIMIZE_SEGMENTS, "5");    // arbitrary
                        }

                        log.info("Heavy Committing #{}: {}", docIdCounter, req);
                        final UpdateResponse rsp = req.process(coreClient);
                        assertEquals("Dummy Doc#" + docIdCounter + " add status: " + rsp.toString(), 0, rsp.getStatus());

                    }
                } catch (Throwable t) {
                    heavyCommitFailure.set(t);
                }
            }
        };

        heavyCommitting.start();
        try {
            // now have the "main" test thread try to take a serious of backups/snapshots
            // while adding other "real" docs

            // NOTE #1: start at i=1 for 'id' & doc counting purposes...
            // NOTE #2: abort quickly if the oher thread reports a heavyCommitFailure...
            for (int i = 1; (i <= numBackupIters && null == heavyCommitFailure.get()); i++) {

                // in each iteration '#i', the commit we create should have exactly 'i' documents in
                // it with the term 'type_s:real' (regardless of what the other thread does with dummy docs)

                // add & commit a doc #i
                final UpdateRequest req = new UpdateRequest().add(makeDoc("doc_" + i, "real"));
                req.setParam(UpdateParams.COMMIT,"true"); // make immediately available for backup
                req.setParam(UpdateParams.OPEN_SEARCHER,"false"); // we don't care about searching

                final UpdateResponse rsp = req.process(coreClient);
                assertEquals("Real Doc#" + i + " add status: " + rsp.toString(), 0, rsp.getStatus());

                makeBackup();
            }

        } finally {
            keepGoing.set(false);
            heavyCommitting.join();
        }
        assertNull(heavyCommitFailure.get());
    }

    public void makeBackup() throws Exception {
        CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(DEFAULT_TEST_COLLECTION_NAME, "stressBackup")
                .setLocation(backupPath.toString())
                .setIncremental(true)
                .setMaxNumberBackupPoints(5);
        if (random().nextBoolean()) {
            try {
                RequestStatusState state = backup.processAndWait(cluster.getSolrClient(), 1000);
                assertEquals(RequestStatusState.COMPLETED, state);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            CollectionAdminResponse rsp = backup.process(cluster.getSolrClient());
            assertEquals(0, rsp.getStatus());
        }
    }

}
