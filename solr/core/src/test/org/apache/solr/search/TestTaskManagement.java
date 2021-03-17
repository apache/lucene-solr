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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
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

        params.set("queryUUID", "foobar");
        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);
        request.setPath("/tasks/cancel");

        NamedList<Object> queryResponse;

        queryResponse = cluster.getSolrClient().request(request);

        assertEquals("Query with queryID foobar not found", queryResponse.get("status"));
        assertEquals(404, queryResponse.get("responseCode"));
    }

    @Test
    public void testCancellationQuery() {
        Set<Integer> queryIdsSet = ConcurrentHashMap.newKeySet();
        Set<Integer> notFoundIdsSet = ConcurrentHashMap.newKeySet();

        List<CompletableFuture<Void>> queryFutures = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            CompletableFuture<Void> future = executeQueryAsync(Integer.toString(i));

            queryFutures.add(future);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            CompletableFuture<Void> future = cancelQuery(Integer.toString(i), 4000, queryIdsSet, notFoundIdsSet);

            futures.add(future);
        }

        futures.forEach(CompletableFuture::join);

        queryFutures.forEach(CompletableFuture::join);

        assertEquals("Total query count did not match the expected value",
                queryIdsSet.size() + notFoundIdsSet.size(), 100);
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

        NamedList<Object> queryResponse;

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

        for (int value : presentQueryIDs) {
            assertTrue(value >= 0 && value < 50);
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

        assertTrue(result.contains("inactive"));
    }

    private CompletableFuture<Void> cancelQuery(final String queryID, final int sleepTime, Set<Integer> cancelledQueryIdsSet,
                                          Set<Integer> notFoundQueryIdSet) {
        return CompletableFuture.runAsync(() -> {
            ModifiableSolrParams params = new ModifiableSolrParams();

            params.set("queryUUID", queryID);
            @SuppressWarnings({"rawtypes"})
            SolrRequest request = new QueryRequest(params);
            request.setPath("/tasks/cancel");

            // Wait for some time to let the query start
            try {
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }

                try {
                    NamedList<Object> queryResponse;

                    queryResponse = cluster.getSolrClient().request(request);

                    int responseCode = (int) queryResponse.get("responseCode");

                    if (responseCode == 200 /* HTTP OK */) {
                        cancelledQueryIdsSet.add(Integer.parseInt(queryID));
                    } else if (responseCode == 404 /* HTTP NOT FOUND */) {
                        notFoundQueryIdSet.add(Integer.parseInt(queryID));
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
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
                throw new CompletionException(e);
            }
        });
    }
}
