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
package org.apache.solr.search;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.junit.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class TestTaskManagement extends SolrCloudTestCase {
    private static final String COLLECTION_NAME = "collection1";

    private ExecutorService executorService;

    @BeforeClass
    public static void setupCluster() throws Exception {
        initCore("solrconfig.xml", "schema11.xml");

        configureCluster(4)
                .addConfig("conf", configset("sql"))
                .configure();
    }

    @AfterClass
    public static void tearDownCluster() throws Exception {
        shutdownCluster();
    }

    @Before
    public void setup() throws Exception {
        super.setUp();

        CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 1)
                .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
                .process(cluster.getSolrClient());
        cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
        cluster.getSolrClient().setDefaultCollection(COLLECTION_NAME);

        cluster.getSolrClient().setDefaultCollection("collection1");

        executorService = ExecutorUtil.newMDCAwareCachedThreadPool("TestTaskManagement");

        List<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i);
            doc.addField("foo1_s", Integer.toString(i));
            doc.addField("foo2_s", Boolean.toString(i % 2 == 0));
            doc.addField("foo4_s", new BytesRef(Boolean.toString(i % 2 == 0)));

            docs.add(doc);
        }

        cluster.getSolrClient().add(docs);
        cluster.getSolrClient().commit();
    }

    @After
    public void tearDown() throws Exception {
        CollectionAdminRequest.deleteCollection(COLLECTION_NAME).process(cluster.getSolrClient());
        executorService.shutdown();
        super.tearDown();
    }

    @Test
    public void testNonExistentQuery() throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();

        params.set("cancelUUID", "foobar");
        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);
        request.setPath("/tasks/cancel");

        NamedList<Object> queryResponse = null;

        queryResponse = cluster.getSolrClient().request(request);

        assertEquals("Query with queryID foobar not found", queryResponse.get("status"));
    }

    @Test
    public void testCancellationQuery() throws Exception {
        ConcurrentHashSet<Integer> queryIdsSet = new ConcurrentHashSet<>();
        ConcurrentHashSet<Integer> notFoundIdsSet = new ConcurrentHashSet<>();

        List<CompletableFuture<Void>> queryFutures = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            CompletableFuture<Void> future = executeQueryAsync(Integer.toString(i));

            queryFutures.add(future);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < 90; i++) {
            CompletableFuture<Void> future = cancelQuery(Integer.toString(i), 4000, queryIdsSet, notFoundIdsSet);

            futures.add(future);
        }

        futures.forEach(CompletableFuture::join);

        queryFutures.forEach(CompletableFuture::join);

        assertTrue(queryIdsSet.size() + notFoundIdsSet.size() == 90);
    }

    @Test
    public void testListCancellableQueries() throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();

        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);
        request.setPath("/tasks/list");

        for (int i = 0; i < 50; i++) {
            executeQueryAsync(Integer.toString(i));
        }

        NamedList<Object> queryResponse = null;

        queryResponse = cluster.getSolrClient().request(request);

        @SuppressWarnings({"unchecked"})
        NamedList<String> result = (NamedList<String>) queryResponse.get("taskList");

        Iterator<Map.Entry<String, String>> iterator = result.iterator();

        Set<Integer> presentQueryIDs = new HashSet<>();

        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();

            presentQueryIDs.add(Integer.parseInt(entry.getKey()));
        }

        assertTrue(presentQueryIDs.size() > 0 && presentQueryIDs.size() <= 50);

        Iterator<Integer> integerIterator = presentQueryIDs.iterator();

        while (integerIterator.hasNext()) {
            int value = integerIterator.next();

            assertTrue (value >= 0 && value < 50);
        }
    }

    @Test
    public void testCheckSpecificQueryStatus() throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();

        params.set("taskUUID", "25");

        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);

        request.setPath("/tasks/list");

        NamedList<Object> queryResponse = cluster.getSolrClient().request(request);

        @SuppressWarnings({"unchecked"})
        String result = (String) queryResponse.get("taskStatus");

        assertFalse(result.contains("true"));
    }

    private CompletableFuture<Void> cancelQuery(final String queryID, final int sleepTime, Set<Integer> cancelledQueryIdsSet,
                                          Set<Integer> notFoundQueryIdSet) {
        return CompletableFuture.runAsync(() -> {
            ModifiableSolrParams params = new ModifiableSolrParams();

            params.set("cancelUUID", queryID);
            @SuppressWarnings({"rawtypes"})
            SolrRequest request = new QueryRequest(params);
            request.setPath("/tasks/cancel");

            // Wait for some time to let the query start
            try {
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }

                try {
                    NamedList<Object> queryResponse = null;

                    queryResponse = cluster.getSolrClient().request(request);

                    String cancellationResult = (String) queryResponse.get("status");
                    if (cancellationResult.contains("cancelled successfully")) {
                        cancelledQueryIdsSet.add(Integer.parseInt(queryID));
                    } else if (cancellationResult.contains("not found")) {
                        notFoundQueryIdSet.add(Integer.parseInt(queryID));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
        }, executorService);
    }

    public void executeQuery(String queryId) throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();

        params.set("q", "*:*");
        params.set("canCancel", "true");

        if (queryId != null) {
            params.set("queryUUID", queryId);
        }

        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);

        cluster.getSolrClient().request(request);
    }

    public CompletableFuture<Void> executeQueryAsync(String queryId) {
        return CompletableFuture.runAsync(() -> {
            try {
                executeQuery(queryId);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        });
    }
}
