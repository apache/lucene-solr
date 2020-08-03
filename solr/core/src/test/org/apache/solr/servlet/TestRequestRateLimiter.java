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

public class TestRequestRateLimiter extends SolrCloudTestCase {
  private final static String COLLECTION = "c1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(COLLECTION, configset("cloud-minimal")).configure();
  }

  @Test
  public void testConcurrentQueries() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(COLLECTION);

    CollectionAdminRequest.createCollection(COLLECTION, 1, 1).process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    SolrDispatchFilter solrDispatchFilter = cluster.getJettySolrRunner(0).getSolrDispatchFilter();

    RequestRateLimiter.RateLimiterConfig rateLimiterConfig = new RequestRateLimiter.RateLimiterConfig(SolrRequest.SolrRequestType.QUERY,
        true, 1, DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS, 5 /* allowedRequests */, true /* isWorkStealingEnabled */);
    RateLimitManager.Builder builder = new MockBuilder(new MockRequestRateLimiter(rateLimiterConfig, 5));
    RateLimitManager rateLimitManager = builder.build();

    solrDispatchFilter.replaceRateLimitManager(rateLimitManager);

    for (int i = 0; i < 100; i++) {
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
      for (int i = 0; i < 25; i++) {
        callableList.add(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            try {
              QueryResponse response = client.query(new SolrQuery("*:*"));

              if (response.getResults().getNumFound() > 0) {
                assertEquals(100, response.getResults().getNumFound());
              }
            } catch (Exception e) {
              throw new RuntimeException(e.getMessage());
            }

            return true;
          }
        });
      }

      futures = executor.invokeAll(callableList);

      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          assertTrue("Not true " + e.getMessage(), e.getMessage().contains("non ok status: 429, message:Too Many Requests"));
        }
      }

      MockRequestRateLimiter mockQueryRateLimiter = (MockRequestRateLimiter) rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY);

      assertTrue("Incoming request count did not match. Expected == 25  incoming " + mockQueryRateLimiter.incomingRequestCount.get(),
          mockQueryRateLimiter.incomingRequestCount.get() == 25);
      assertTrue("Incoming accepted new request count did not match. Expected 5 incoming " + mockQueryRateLimiter.acceptedNewRequestCount.get(),
          mockQueryRateLimiter.acceptedNewRequestCount.get() == 5);
      assertTrue("Incoming rejected new request count did not match. Expected 20 incoming " + mockQueryRateLimiter.rejectedRequestCount.get(),
          mockQueryRateLimiter.rejectedRequestCount.get() == 20);
      assertTrue("Incoming total processed requests count did not match. Expected " + mockQueryRateLimiter.incomingRequestCount.get() + " incoming "
              + (mockQueryRateLimiter.acceptedNewRequestCount.get() + mockQueryRateLimiter.rejectedRequestCount.get()),
          (mockQueryRateLimiter.acceptedNewRequestCount.get() + mockQueryRateLimiter.rejectedRequestCount.get()) == mockQueryRateLimiter.incomingRequestCount.get());
    } finally {
      executor.shutdown();
    }
  }

  private static class MockRequestRateLimiter extends RequestRateLimiter {
    AtomicInteger incomingRequestCount;
    AtomicInteger acceptedNewRequestCount;
    AtomicInteger rejectedRequestCount;
    AtomicInteger activeRequestCount;

    private final int maxCount;

    public MockRequestRateLimiter(RateLimiterConfig config, final int maxCount) {
      super(config);

      this.incomingRequestCount = new AtomicInteger(0);
      this.acceptedNewRequestCount = new AtomicInteger(0);
      this.rejectedRequestCount = new AtomicInteger(0);
      this.activeRequestCount = new AtomicInteger(0);
      this.maxCount = maxCount;
    }

    @Override
    public boolean handleRequest() throws InterruptedException {
      incomingRequestCount.getAndIncrement();

      boolean response = super.handleRequest();

      if (response) {
          acceptedNewRequestCount.getAndIncrement();
        if (activeRequestCount.incrementAndGet() > maxCount) {
          throw new IllegalStateException("Active request count exceeds the count. Incoming " +
              activeRequestCount.get() + " maxCount " + maxCount);
        }
      } else {
          rejectedRequestCount.getAndIncrement();
      }

      return response;
    }

    @Override
    public void decrementConcurrentRequests() {
      activeRequestCount.decrementAndGet();
      super.decrementConcurrentRequests();
    }
  }

  private static class MockBuilder extends RateLimitManager.Builder {
    private final RequestRateLimiter queryRequestRateLimiter;

    public MockBuilder(RequestRateLimiter queryRequestRateLimiter) {
      this.queryRequestRateLimiter = queryRequestRateLimiter;
    }

    @Override
    public RateLimitManager build() {
      RateLimitManager rateLimitManager = new RateLimitManager();

      rateLimitManager.registerRequestRateLimiter(queryRequestRateLimiter, SolrRequest.SolrRequestType.QUERY);

      return rateLimitManager;
    }
  }
}
