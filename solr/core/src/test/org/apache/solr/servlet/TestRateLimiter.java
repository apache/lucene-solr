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

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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

import static org.apache.solr.servlet.RateLimitManager.DEFAULT_SUSPEND_TIME_INMS;
import static org.apache.solr.servlet.RateLimitManager.DEFAULT_TIMEOUT_MS;

public class TestRateLimiter extends SolrCloudTestCase {
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

    RequestRateLimiter.RateLimiterConfig rateLimiterConfig = new RequestRateLimiter.RateLimiterConfig(DEFAULT_SUSPEND_TIME_INMS,
        DEFAULT_TIMEOUT_MS, 5 /* allowedRequests */);
    RateLimitManager.Builder builder = new MockBuilder(new MockRequestRateLimiter(rateLimiterConfig, 5),
        new MockRequestRateLimiter(rateLimiterConfig, 5));
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
              assertEquals(100, response.getResults().getNumFound());
            } catch (Exception e) {
              throw new RuntimeException(e.getMessage());
            }

            return true;
          }
        });
      }

      futures = executor.invokeAll(callableList);

      for (Future<?> future : futures) {
        future.get();
      }

      MockRequestRateLimiter mockQueryRateLimiter = (MockRequestRateLimiter) rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY);

      assertTrue("Incoming request count did not match. Expected >200  incoming " + mockQueryRateLimiter.incomingRequestCount.get(),
          mockQueryRateLimiter.incomingRequestCount.get() > 25);
      assertTrue("Incoming accepted new request count did not match. Expected 200 incoming " + mockQueryRateLimiter.acceptedNewRequestCount.get(),
          mockQueryRateLimiter.acceptedNewRequestCount.get() == 25);
      assertTrue("Incoming rejected new request count did not match. Expected >0 incoming " + mockQueryRateLimiter.rejectedRequestCount.get(),
          mockQueryRateLimiter.rejectedRequestCount.get() > 0);
      assertTrue("Incoming total processed requests count did not match. Expected " + mockQueryRateLimiter.incomingRequestCount.get() + " incoming "
              + (mockQueryRateLimiter.acceptedNewRequestCount.get() + mockQueryRateLimiter.rejectedRequestCount.get()),
          (mockQueryRateLimiter.acceptedNewRequestCount.get() + mockQueryRateLimiter.rejectedRequestCount.get()) == mockQueryRateLimiter.incomingRequestCount.get());
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testConcurrentUpdateAndQuery() throws Exception {
    SolrDispatchFilter solrDispatchFilter = cluster.getJettySolrRunner(0).getSolrDispatchFilter();
    String INDEX_COLLECTION = "c3";

    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(INDEX_COLLECTION);

    CollectionAdminRequest.createCollection(INDEX_COLLECTION, 1, 1).process(client);
    cluster.waitForActiveCollection(INDEX_COLLECTION, 1, 1);

    RequestRateLimiter.RateLimiterConfig rateLimiterConfig = new RequestRateLimiter.RateLimiterConfig(DEFAULT_SUSPEND_TIME_INMS,
        DEFAULT_TIMEOUT_MS, 3 /* allowedRequests */);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    RateLimitManager.Builder builder = new MockBuilder(new MockBlockingRequestRateLimiter(rateLimiterConfig, 3, countDownLatch),
        new MockRequestRateLimiter(rateLimiterConfig, 3));
    RateLimitManager rateLimitManager = builder.build();

    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("threadpool");
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();

      doc.setField("id", i);
      doc.setField("text", "foo");
      client.add(doc);
    }

    client.commit();

    solrDispatchFilter.replaceRateLimitManager(rateLimitManager);

    // Submit a blocking indexing task
    try {
      executor.submit(() -> {
          try {
              SolrInputDocument doc = new SolrInputDocument();

              doc.setField("id", 5);
              doc.setField("text", "foo");
              client.add(INDEX_COLLECTION, doc);

              client.commit();
          } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
          }
        });

      for (int i = 0; i < 10; i++) {
        futures.add(executor.submit(() -> {
          try {
            QueryResponse response = client.query(new SolrQuery("*:*"));
            assertEquals(100, response.getResults().getNumFound());
          } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
          }
        }));
      }

      for (Future<?> future : futures) {
        future.get();
      }

      countDownLatch.countDown();

      MockRequestRateLimiter mockIndexRateLimiter = (MockRequestRateLimiter) rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.UPDATE);

      assertTrue("Incoming request count did not match. Expected > 4 incoming " +  mockIndexRateLimiter.incomingRequestCount.get(),
           mockIndexRateLimiter.incomingRequestCount.get() == 1);

      MockRequestRateLimiter mockQueryRateLimiter = (MockRequestRateLimiter) rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY);

      assertTrue("Incoming request count did not match. Expected > 10 incoming " +  mockQueryRateLimiter.incomingRequestCount.get(),
          mockQueryRateLimiter.incomingRequestCount.get() > 10);
      assertTrue("Incoming accepted new request count did not match. Expected 10 incoming " +  mockQueryRateLimiter.acceptedNewRequestCount.get(),
          mockQueryRateLimiter.acceptedNewRequestCount.get() == 10);
      assertTrue("Incoming rejected new request count did not match. Expected >0 incoming " +  mockQueryRateLimiter.rejectedRequestCount.get(),
          mockQueryRateLimiter.rejectedRequestCount.get() > 0);
      assertTrue("Incoming total processed requests count did not match. Expected " +  mockQueryRateLimiter.incomingRequestCount.get() + " incoming "
              + ( mockQueryRateLimiter.acceptedNewRequestCount.get() +  mockQueryRateLimiter.rejectedRequestCount.get()),
          ( mockQueryRateLimiter.acceptedNewRequestCount.get() +  mockQueryRateLimiter.rejectedRequestCount.get()) ==  mockQueryRateLimiter.incomingRequestCount.get());
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
    public boolean handleRequest(HttpServletRequest request) throws InterruptedException {
      incomingRequestCount.getAndIncrement();

      boolean response = super.handleRequest(request);

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

  private static class MockBlockingRequestRateLimiter extends MockRequestRateLimiter {
    private final CountDownLatch countDownLatch;

    public MockBlockingRequestRateLimiter(RateLimiterConfig config, int maxCount, final CountDownLatch countDownLatch) {
      super(config, maxCount);

      this.countDownLatch = countDownLatch;
    }

    @Override
    public void decrementConcurrentRequests() {
      try {
        super.decrementConcurrentRequests();
        countDownLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  private static class MockBuilder extends RateLimitManager.Builder {
    private final RequestRateLimiter indexRequestRateLimiter;
    private final RequestRateLimiter queryRequestRateLimiter;

    public MockBuilder(RequestRateLimiter indexRequestRateLimiter, RequestRateLimiter queryRequestRateLimiter) {
      this.indexRequestRateLimiter = indexRequestRateLimiter;
      this.queryRequestRateLimiter = queryRequestRateLimiter;
    }

    @Override
    public RateLimitManager build() {
      RateLimitManager rateLimitManager = new RateLimitManager();

      rateLimitManager.registerRequestRateLimiter(indexRequestRateLimiter, SolrRequest.SolrRequestType.UPDATE);
      rateLimitManager.registerRequestRateLimiter(queryRequestRateLimiter, SolrRequest.SolrRequestType.QUERY);

      return rateLimitManager;
    }
  }
}
