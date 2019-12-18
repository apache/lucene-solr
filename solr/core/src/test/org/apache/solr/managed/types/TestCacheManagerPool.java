package org.apache.solr.managed.types;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.lucene.util.Accountable;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.managed.ChangeListener;
import org.apache.solr.managed.DefaultResourceManager;
import org.apache.solr.managed.ManagedComponent;
import org.apache.solr.managed.ResourceManager;
import org.apache.solr.managed.ResourceManagerPool;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.CaffeineCache;
import org.apache.solr.search.NoOpRegenerator;
import org.apache.solr.search.SolrCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestCacheManagerPool extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  ResourceManager resourceManager;

  @Before
  public void setupTest() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    // disable automatic scheduling of pool runs
    resourceManager = new DefaultResourceManager(h.getCore().getResourceLoader(), null);
    resourceManager.init(null);
  }

  private static class ChangeTestListener implements ChangeListener {
    Map<String, Map<String, Object>> changedValues = new ConcurrentHashMap<>();

    @Override
    public void changedLimit(String poolName, ManagedComponent component, String limitName, Object newRequestedVal, Object newActualVal, Reason reason) {
      Map<String, Object> perComponent = changedValues.computeIfAbsent(component.getManagedComponentId().toString(), id -> new ConcurrentHashMap<>());
      perComponent.put(limitName, newActualVal);
      perComponent.put("reason", reason);
    }

    public void clear() {
      changedValues.clear();
    }
  }

  @Test
  public void testPoolLimits() throws Exception {
    ResourceManagerPool pool = resourceManager.createPool("testPoolLimits", CacheManagerPool.TYPE, Collections.singletonMap("maxRamMB", 200), Collections.emptyMap());
    SolrMetricManager metricManager = new SolrMetricManager();
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, "fooRegistry", "barScope", "bazTag");
    List<SolrCache> caches = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      SolrCache<String, Accountable> cache = new CaffeineCache<>();
      Map<String, String> params = new HashMap<>();
      params.put("maxRamMB", "50");
      cache.init(params, null, new NoOpRegenerator());
      cache.initializeMetrics(solrMetricsContext, "child-" + i);
      cache.initializeManagedComponent(resourceManager, "testPoolLimits");
      caches.add(cache);
    }
    ChangeTestListener listener = new ChangeTestListener();
    pool.addChangeListener(listener);
    // fill up all caches just below the global limit, evenly with small values
    for (int i = 0; i < 202; i++) {
      for (SolrCache<String, Accountable> cache : caches) {
        cache.put("id-" + i, new Accountable() {
          @Override
          public long ramBytesUsed() {
            return 100 * SolrCache.KB;
          }
        });
      }
    }
    // generate lookups to trigger the optimization
    for (SolrCache<String, Accountable> cache : caches) {
      for (int i = 0; i < CacheManagerPool.DEFAULT_LOOKUP_DELTA * 2; i++) {
        cache.get("id-" + i);
      }
    }
    pool.manage();
    Map<String, Object> totalValues = pool.aggregateTotalValues(pool.getCurrentValues());

    assertEquals("should not adjust (within deadband): " + listener.changedValues.toString(), 0, listener.changedValues.size());
    // add a few large values to exceed the total limit
    // but without exceeding local (cache) limit
    for (int i = 0; i < 10; i++) {
      caches.get(0).put("large-" + i, new Accountable() {
        @Override
        public long ramBytesUsed() {
          return 2560 * SolrCache.KB;
        }
      });
    }
    pool.manage();
    totalValues = pool.aggregateTotalValues(pool.getCurrentValues());
    // OPTIMIZATION should have handled this, due to abnormally high hit ratios
    assertEquals("should adjust all: " + listener.changedValues.toString(), 10, listener.changedValues.size());
    listener.changedValues.values().forEach(map -> assertEquals(ChangeListener.Reason.OPTIMIZATION, map.get("reason")));
    // add more large values
    for (int i = 0; i < 10; i++) {
      caches.get(i).put("large1-" + i, new Accountable() {
        @Override
        public long ramBytesUsed() {
          return 2560 * SolrCache.KB;
        }
      });
    }
    // don't generate any new lookups - prevents OPTIMIZATION

    //

    // NOTE: this takes a few rounds to adjust because we modify the original maxRamMB which
    // may have been very different for each cache.
    int cnt = 0;
    do {
      cnt++;
      listener.clear();
      pool.manage();
      if (listener.changedValues.isEmpty()) {
        break;
      }
      totalValues = pool.aggregateTotalValues(pool.getCurrentValues());
      assertEquals("should adjust all again: " + listener.changedValues.toString(), 10, listener.changedValues.size());
      listener.changedValues.values().forEach(map -> assertEquals(ChangeListener.Reason.ABOVE_TOTAL_LIMIT, map.get("reason")));
      log.info(" - step " + cnt + ": " + listener.changedValues);
    } while (cnt < 10);
    if (cnt == 0) {
      fail("failed to adjust to fit in 10 steps: " + listener.changedValues);
    }
  }

  private static final Accountable LARGE_ITEM = new Accountable() {
    @Override
    public long ramBytesUsed() {
      return SolrCache.MB;
    }
  };

  private static final Accountable SMALL_ITEM = new Accountable() {
    @Override
    public long ramBytesUsed() {
      return SolrCache.KB;
    }
  };

  @Test
  public void testHitRatioOptimization() throws Exception {
    ResourceManagerPool pool = resourceManager.createPool("testHitRatio", CacheManagerPool.TYPE, Collections.singletonMap("maxSize", 200), Collections.emptyMap());
    SolrMetricManager metricManager = new SolrMetricManager();
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, "fooRegistry", "barScope", "bazTag");
    SolrCache<Integer, Accountable> cache = new CaffeineCache<>();
    Map<String, String> params = new HashMap<>();
    int initialSize = 100;
    params.put("size", "" + initialSize);
    cache.init(params, null, new NoOpRegenerator());
    cache.initializeMetrics(solrMetricsContext, "testHitRatio");
    cache.initializeManagedComponent(resourceManager, "testHitRatio");

    ChangeTestListener listener = new ChangeTestListener();
    pool.addChangeListener(listener);

    // ===== test shrinking =====
    // populate / lookup with a small set of items -> high hit ratio.
    // Optimization should kick in and shrink the cache
    ZipfDistribution zipf = new ZipfDistribution(100, 0.99);
    int NUM_LOOKUPS = 3000;
    for (int i = 0; i < NUM_LOOKUPS; i++) {
      cache.computeIfAbsent(zipf.sample(), k -> SMALL_ITEM);
    }
    pool.manage();
    assertTrue(cache.getMaxSize() < initialSize);
    assertEquals(listener.changedValues.toString(), 1, listener.changedValues.size());
    listener.changedValues.values().forEach(map -> assertEquals(ChangeListener.Reason.OPTIMIZATION, map.get("reason")));
    // iterate until it's small enough to affect the hit ratio
    int cnt = 0;
    do {
      listener.clear();
      cnt++;
      for (int i = 0; i < NUM_LOOKUPS; i++) {
        cache.computeIfAbsent(zipf.sample(), k -> SMALL_ITEM);
      }
      pool.manage();
      if (listener.changedValues.isEmpty()) {
        break;
      }
      log.info(" - step " + cnt + ": " + listener.changedValues);
    } while (cnt < 10);
    if (cnt == 10) {
      fail("failed to reach the balance: " + listener.changedValues);
    }
    assertTrue("maxSize adjusted more than allowed: " + cache.getMaxSize(), cache.getMaxSize() >= initialSize / CacheManagerPool.DEFAULT_MAX_ADJUST_RATIO);

    // ========= test expansion ===========
    listener.clear();
    zipf = new ZipfDistribution(100000, 0.5);
    for (int i = 0; i < NUM_LOOKUPS * 2; i++) {
      cache.computeIfAbsent(zipf.sample(), k -> SMALL_ITEM);
    }
    pool.manage();
    assertEquals(listener.changedValues.toString(), 1, listener.changedValues.size());
    listener.changedValues.values().forEach(map -> assertEquals(ChangeListener.Reason.OPTIMIZATION, map.get("reason")));
    assertTrue(cache.getMaxSize() > initialSize);

    cnt = 0;
    do {
      listener.clear();
      cnt++;
      for (int i = 0; i < NUM_LOOKUPS * 2; i++) {
        cache.computeIfAbsent(zipf.sample(), k -> SMALL_ITEM);
      }
      pool.manage();
      if (listener.changedValues.isEmpty()) {
        break;
      }
      log.info(" - step " + cnt + ": " + listener.changedValues);
    } while (cnt < 10);
    if (cnt == 10) {
      fail("failed to reach the balance: " + listener.changedValues);
    }
    assertTrue("maxSize adjusted more than allowed: " + cache.getMaxSize(), cache.getMaxSize() <= initialSize * CacheManagerPool.DEFAULT_MAX_ADJUST_RATIO);
  }

  @After
  public void teardownTest() throws Exception {
    resourceManager.close();
  }
}
