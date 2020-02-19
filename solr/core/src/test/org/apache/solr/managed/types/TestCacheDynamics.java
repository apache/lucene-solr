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
package org.apache.solr.managed.types;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.managed.ChangeListener;
import org.apache.solr.managed.DefaultResourceManager;
import org.apache.solr.managed.ResourceManager;
import org.apache.solr.managed.ResourceManagerPool;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
// suppress chatty indexing and query logs
@LogLevel("org.apache.solr.core=WARN")
public class TestCacheDynamics extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NUM_TOKENS = 1000;
  private static final int NUM_DOCS = 1000;
  private static final int NUM_THREADS = 5;
  private static final ZipfDistribution zipf = new ZipfDistribution(NUM_TOKENS, 0.5);

  private static ResourceManager resourceManager;
  private static CloudSolrClient solrClient;
  private static SolrCloudManager cloudManager;

  private static String getRandomZipfText() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10 + random().nextInt(NUM_TOKENS - 10); i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(zipf.sample());
    }
    return sb.toString();
  }

  private static String getRandomQuery(String field, int maxLength, boolean zipfian) {
    StringBuilder sb = new StringBuilder();
    int len = maxLength > 1 ? 1 + random().nextInt(maxLength - 1) : maxLength;
    for (int i = 0; i < len; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(field).append(':');
      if (zipfian) {
        sb.append(zipf.sample());
      } else {
        sb.append(random().nextInt(NUM_TOKENS));
      }
    }
    return sb.toString();
  }

  private static final String COLLECTION = TestCacheDynamics.class.getName() + "_collection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-cache"))
        .configure();
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .setMaxShardsPerNode(5)
        .process(cluster.getSolrClient());
    CloudUtil.waitForState(cloudManager, "failed to create collection", COLLECTION, CloudUtil.clusterShape(2, 2));
    resourceManager = cluster.getJettySolrRunner(0).getCoreContainer().getResourceManagerApi().getResourceManager();
    solrClient = cluster.getSolrClient();
    solrClient.setDefaultCollection(COLLECTION);
    // add some docs
    UpdateRequest ureq = new UpdateRequest();
    for (int i = 0; i < NUM_DOCS; i++) {
      ureq.add(new SolrInputDocument("id", "id-" + i, "text_ws", getRandomZipfText()));
    }
    ureq.process(solrClient);
    solrClient.commit();
  }

  private CountDownLatch warmupLatch;

  @Test
  public void testDynamics() throws Exception {
    // speed-up the management for this test
    resourceManager.setPoolParams(DefaultResourceManager.QUERY_RESULT_CACHE_POOL, Collections.singletonMap(ResourceManagerPool.SCHEDULE_DELAY_SECONDS_PARAM, 1));
    ResourceManagerPool queryResultPool = resourceManager.getPool(DefaultResourceManager.QUERY_RESULT_CACHE_POOL);
    CacheChangeListener listener = new CacheChangeListener(8);
    queryResultPool.addChangeListener(listener);
    queryResultPool.setPoolLimits(Collections.singletonMap("maxSize", 100));
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicBoolean zipfian = new AtomicBoolean(true);
    final AtomicInteger numTerms = new AtomicInteger(3);
    final AtomicInteger numRows = new AtomicInteger(10);

    // wait at least this number of queries
    warmupLatch = new CountDownLatch(NUM_DOCS);

    // simulate query traffic
    List<Thread> queryThreads = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread t = new Thread(() -> {
        while (!stop.get()) {
          try {
            solrClient.query(params("q", getRandomQuery("text_ws", numTerms.get(), zipfian.get()), "fl", "*", "rows", "" + numRows.get()));
            warmupLatch.countDown();
            Thread.sleep(10);
          } catch (Exception e) {
            fail(e.toString());
            break;
          }
        }
      });
      queryThreads.add(t);
    }
    try {
      queryThreads.forEach(t -> t.start());

      // ========== HARD TRIM UNDER RESOURCE SHORTAGE ===========
      boolean await = warmupLatch.await(60, TimeUnit.SECONDS);
      if (!await) {
        fail("did not execute queries in time");
      }
      // initial size in the _default configset is 512
      // wait for at least one limit adjustment
      if (!listener.atLeastEvents.await(60, TimeUnit.SECONDS)) {
        fail("did not produce change events in time");
      }
      listener.changedValues.values().forEach(lst -> lst.forEach(map -> assertEquals(ChangeListener.Reason.ABOVE_TOTAL_LIMIT, map.get("reason"))));
      assertTrue("there were errors: " + listener.errors, listener.errors.isEmpty());
      Map<String, Object> metrics = queryResultPool.getSolrMetricsContext().getMetricsSnapshot(false);
      assertNotNull("'changes' missing: " + metrics.toString(), metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.changes"));
      long numChanges = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.changes")).get("ABOVE_TOTAL_LIMIT")).longValue();
      assertTrue("at least 8 changes should have happened: " + metrics, numChanges >= 8);
      assertNotNull("manageTimes missing: " + metrics.toString(), metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.manageTimes"));
      long numRuns = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.manageTimes")).get("count")).longValue();
      assertTrue("at least 3 runs should have happened: " + metrics, numRuns >= 3);
      long totalSize = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.totalValues")).get("size")).longValue();
      long maxSize = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.poolLimits")).get("maxSize")).longValue();
      assertEquals("total size " + totalSize + " should be within deadband of pool limit maxSize " + maxSize, (double)totalSize, (double)maxSize, 0.1 * maxSize);

      // ============ SOFT OPTIMIZATION WHEN RESOURCES AVAILABLE ==========
      listener.clear();
      // Increase available resources.
      // this value takes into account:
      // * 4 cores (4 cache instances in the pool)
      // *
      queryResultPool.setPoolLimits(Collections.singletonMap("maxSize", 2500));
      // modify the search pattern to generate unique results and fill up the caches
      // while thrashing the hitratio to trigger optimization
      numTerms.set(6);
      // reset the warmup latch
      warmupLatch = new CountDownLatch(NUM_DOCS);
      await = warmupLatch.await(60, TimeUnit.SECONDS);
      if (!await) {
        fail("did not execute queries in time");
      }
      // wait for at least one limit adjustment
      if (!listener.atLeastEvents.await(60, TimeUnit.SECONDS)) {
        fail("did not produce change events in time");
      }
      listener.changedValues.values().forEach(lst -> lst.forEach(map -> assertEquals(ChangeListener.Reason.OPTIMIZATION, map.get("reason"))));
      assertTrue("there were errors: " + listener.errors, listener.errors.isEmpty());

      // let it run quite some more to fill up the caches and generate more adjustments
      warmupLatch = new CountDownLatch(NUM_DOCS * 6);
      await = warmupLatch.await(90, TimeUnit.SECONDS);
      if (!await) {
        fail("did not execute queries in time");
      }
      listener.changedValues.values().forEach(lst -> {
        // first event is an optimization
        // second event is an optimization that eventually overshoots the target
        // third event is a hard control because the total limit is exceeded
        assertTrue("should be at least 3 events: " + lst.toString(), lst.size() >= 3);
        Map<String, Object> ev = lst.get(0);
        assertEquals(ChangeListener.Reason.OPTIMIZATION, ev.get("reason"));
        ev = lst.get(1);
        assertEquals(ChangeListener.Reason.OPTIMIZATION, ev.get("reason"));
        ev = lst.get(2);
        assertEquals(ChangeListener.Reason.ABOVE_TOTAL_LIMIT, ev.get("reason"));
      });
      metrics = queryResultPool.getSolrMetricsContext().getMetricsSnapshot(false);
      assertNotNull("'changes' missing: " + metrics.toString(), metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.changes"));
      numChanges = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.changes")).get("ABOVE_TOTAL_LIMIT")).longValue();
      assertTrue("at least 8 ABOVE_TOTAL_LIMIT changes should have happened: " + metrics, numChanges >= 8);
      numChanges = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.changes")).get("OPTIMIZATION")).longValue();
      assertTrue("at least 8 OPTIMIZATION changes should have happened: " + metrics, numChanges >= 8);
      assertNotNull("manageTimes missing: " + metrics.toString(), metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.manageTimes"));
      numRuns = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.manageTimes")).get("count")).longValue();
      assertTrue("at least 3 runs should have happened: " + metrics, numRuns >= 10);
      totalSize = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.totalValues")).get("size")).longValue();
      maxSize = ((Number) ((Map<String, Object>) metrics.get("RESOURCE.pool.cache.searcherQueryResultCache.poolLimits")).get("maxSize")).longValue();
      assertEquals("total size " + totalSize + " should be within deadband of pool limit maxSize " + maxSize, (double)totalSize, (double)maxSize, 0.1 * maxSize);
    } finally {
      stop.set(true);
      queryThreads.clear();
    }
  }

  @AfterClass
  public static void teardownTest() throws Exception {
    cloudManager = null;
    solrClient.close();
    solrClient = null;
    resourceManager = null;
  }
}
