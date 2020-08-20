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

package org.apache.solr.servlet;

import javax.servlet.FilterConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.servlet.RateLimitManager.DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS;
import static org.hamcrest.CoreMatchers.containsString;

public class TestRequestRateLimiter extends SolrCloudTestCase {
  private final static String FIRST_COLLECTION = "c1";
  private final static String SECOND_COLLECTION = "c2";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(FIRST_COLLECTION, configset("cloud-minimal")).configure();
  }

  @Test
  public void testConcurrentQueries() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(FIRST_COLLECTION);

    CollectionAdminRequest.createCollection(FIRST_COLLECTION, 1, 1).process(client);
    cluster.waitForActiveCollection(FIRST_COLLECTION, 1, 1);

    SolrDispatchFilter solrDispatchFilter = cluster.getJettySolrRunner(0).getSolrDispatchFilter();

    RequestRateLimiter.RateLimiterConfig rateLimiterConfig = new RequestRateLimiter.RateLimiterConfig(SolrRequest.SolrRequestType.QUERY,
        true, 1, DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS, 5 /* allowedRequests */, true /* isSlotBorrowing */);
    // We are fine with a null FilterConfig here since we ensure that MockBuilder never invokes its parent here
    RateLimitManager.Builder builder = new MockBuilder(null /* dummy FilterConfig */, new MockRequestRateLimiter(rateLimiterConfig, 5));
    RateLimitManager rateLimitManager = builder.build();

    solrDispatchFilter.replaceRateLimitManager(rateLimitManager);

    int numDocs = TEST_NIGHTLY ? 10000 : 100;

    processTest(client, numDocs, 350 /* number of queries */);

    MockRequestRateLimiter mockQueryRateLimiter = (MockRequestRateLimiter) rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY);

    assertEquals(350, mockQueryRateLimiter.incomingRequestCount.get());

    assertTrue(mockQueryRateLimiter.acceptedNewRequestCount.get() > 0);
    assertTrue((mockQueryRateLimiter.acceptedNewRequestCount.get() == mockQueryRateLimiter.incomingRequestCount.get()
        || mockQueryRateLimiter.rejectedRequestCount.get() > 0));
    assertEquals(mockQueryRateLimiter.incomingRequestCount.get(),
        mockQueryRateLimiter.acceptedNewRequestCount.get() + mockQueryRateLimiter.rejectedRequestCount.get());
  }

  @Nightly
  public void testSlotBorrowing() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(SECOND_COLLECTION);

    CollectionAdminRequest.createCollection(SECOND_COLLECTION, 1, 1).process(client);
    cluster.waitForActiveCollection(SECOND_COLLECTION, 1, 1);

    SolrDispatchFilter solrDispatchFilter = cluster.getJettySolrRunner(0).getSolrDispatchFilter();

    RequestRateLimiter.RateLimiterConfig queryRateLimiterConfig = new RequestRateLimiter.RateLimiterConfig(SolrRequest.SolrRequestType.QUERY,
        true, 1, DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS, 5 /* allowedRequests */, true /* isSlotBorrowing */);
    RequestRateLimiter.RateLimiterConfig indexRateLimiterConfig = new RequestRateLimiter.RateLimiterConfig(SolrRequest.SolrRequestType.UPDATE,
        true, 1, DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS, 5 /* allowedRequests */, true /* isSlotBorrowing */);
    // We are fine with a null FilterConfig here since we ensure that MockBuilder never invokes its parent
    RateLimitManager.Builder builder = new MockBuilder(null /*dummy FilterConfig */, new MockRequestRateLimiter(queryRateLimiterConfig, 5), new MockRequestRateLimiter(indexRateLimiterConfig, 5));
    RateLimitManager rateLimitManager = builder.build();

    solrDispatchFilter.replaceRateLimitManager(rateLimitManager);

    int numDocs = 10000;

    processTest(client, numDocs, 400 /* Number of queries */);

    MockRequestRateLimiter mockIndexRateLimiter = (MockRequestRateLimiter) rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.UPDATE);

    assertTrue("Incoming slots borrowed count did not match. Expected > 0  incoming " + mockIndexRateLimiter.borrowedSlotCount.get(),
        mockIndexRateLimiter.borrowedSlotCount.get() > 0);
  }

  private void processTest(CloudSolrClient client, int numDocuments, int numQueries) throws Exception {

    for (int i = 0; i < numDocuments; i++) {
      SolrInputDocument doc = new SolrInputDocument();

      doc.setField("id", i);
      doc.setField("text", "foo");
      client.add(doc);
    }

    client.commit();

    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("threadpool");
    List<Callable<Boolean>> callableList = new ArrayList<>();
    List<Future<Boolean>> futures;

    try {
      for (int i = 0; i < numQueries; i++) {
        callableList.add(() -> {
          try {
            QueryResponse response = client.query(new SolrQuery("*:*"));

            assertEquals(numDocuments, response.getResults().getNumFound());
          } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
          }

          return true;
        });
      }

      futures = executor.invokeAll(callableList);

      for (Future<?> future : futures) {
        try {
          assertTrue(future.get() != null);
        } catch (Exception e) {
          assertThat(e.getMessage(), containsString("non ok status: 429, message:Too Many Requests"));
        }
      }
    } finally {
      executor.shutdown();
    }
  }

  private static class MockRequestRateLimiter extends RequestRateLimiter {
    final AtomicInteger incomingRequestCount;
    final AtomicInteger acceptedNewRequestCount;
    final AtomicInteger rejectedRequestCount;
    final AtomicInteger borrowedSlotCount;

    private final int maxCount;

    public MockRequestRateLimiter(RateLimiterConfig config, final int maxCount) {
      super(config);

      this.incomingRequestCount = new AtomicInteger(0);
      this.acceptedNewRequestCount = new AtomicInteger(0);
      this.rejectedRequestCount = new AtomicInteger(0);
      this.borrowedSlotCount = new AtomicInteger(0);
      this.maxCount = maxCount;
    }

    @Override
    public SlotMetadata handleRequest() throws InterruptedException {
      incomingRequestCount.getAndIncrement();

      SlotMetadata response = super.handleRequest();

      if (response != null) {
        acceptedNewRequestCount.getAndIncrement();
      } else {
        rejectedRequestCount.getAndIncrement();
      }

      return response;
    }

    @Override
    public SlotMetadata allowSlotBorrowing() throws InterruptedException {
      SlotMetadata result = super.allowSlotBorrowing();

      if (result.isReleasable()) {
        borrowedSlotCount.incrementAndGet();
      }

      return result;
    }
  }

  private static class MockBuilder extends RateLimitManager.Builder {
    private final RequestRateLimiter queryRequestRateLimiter;
    private final RequestRateLimiter indexRequestRateLimiter;

    public MockBuilder(FilterConfig config, RequestRateLimiter queryRequestRateLimiter) {
      super(config);

      this.queryRequestRateLimiter = queryRequestRateLimiter;
      this.indexRequestRateLimiter = null;
    }

    public MockBuilder(FilterConfig config, RequestRateLimiter queryRequestRateLimiter, RequestRateLimiter indexRequestRateLimiter) {
      super(config);

      this.queryRequestRateLimiter = queryRequestRateLimiter;
      this.indexRequestRateLimiter = indexRequestRateLimiter;
    }

    @Override
    public RateLimitManager build() {
      RateLimitManager rateLimitManager = new RateLimitManager();

      rateLimitManager.registerRequestRateLimiter(queryRequestRateLimiter, SolrRequest.SolrRequestType.QUERY);

      if (indexRequestRateLimiter != null) {
        rateLimitManager.registerRequestRateLimiter(indexRequestRateLimiter, SolrRequest.SolrRequestType.UPDATE);
      }

      return rateLimitManager;
    }
  }
}
