package org.apache.solr.managed.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.util.Accountable;
import org.apache.solr.SolrTestCaseJ4;
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

/**
 *
 */
public class TestCacheManagerPool extends SolrTestCaseJ4 {

  ResourceManager resourceManager;

  @Before
  public void setupTest() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    // disable automatic scheduling of pool runs
    resourceManager = new DefaultResourceManager(h.getCore().getResourceLoader(), null);
    resourceManager.init(null);
  }

  private static final long KB = 1024;
  private static final long MB = 1024 * KB;

  private static class ChangeListener implements org.apache.solr.managed.ChangeListener {
    Map<String, Map<String, Object>> changedValues = new ConcurrentHashMap<>();

    @Override
    public void changedLimit(String poolName, ManagedComponent component, String limitName, Object newRequestedVal, Object newActualVal) {
      Map<String, Object> perComponent = changedValues.computeIfAbsent(component.getManagedComponentId().toString(), id -> new ConcurrentHashMap<>());
      perComponent.put(limitName, newActualVal);
    }

    public void clear() {
      changedValues.clear();
    }
  }

  @Test
  public void testPoolLimits() throws Exception {
    ResourceManagerPool pool = resourceManager.createPool("test", CacheManagerPool.TYPE, Collections.singletonMap("maxRamMB", 200), Collections.emptyMap());
    SolrMetricManager metricManager = new SolrMetricManager();
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, "fooRegistry", "barScope", "bazTag");
    List<SolrCache> caches = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      SolrCache<String, Accountable> cache = new CaffeineCache<>();
      Map<String, String> params = new HashMap<>();
      params.put("maxRamMB", "50");
      cache.init(params, null, new NoOpRegenerator());
      cache.initializeMetrics(solrMetricsContext, "child-" + i);
      cache.initializeManagedComponent(resourceManager, "test");
      caches.add(cache);
    }
    ChangeListener listener = new ChangeListener();
    pool.addChangeListener(listener);
    // fill up all caches just below the global limit, evenly with small values
    for (int i = 0; i < 202; i++) {
      for (SolrCache<String, Accountable> cache : caches) {
        cache.put("id-" + i, new Accountable() {
          @Override
          public long ramBytesUsed() {
            return 100 * KB;
          }
        });
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
          return 2560 * KB;
        }
      });
    }
    pool.manage();
    totalValues = pool.aggregateTotalValues(pool.getCurrentValues());

    assertEquals("should adjust all: " + listener.changedValues.toString(), 10, listener.changedValues.size());
    listener.clear();
    pool.manage();
    totalValues = pool.aggregateTotalValues(pool.getCurrentValues());
    assertEquals("should adjust all again: " + listener.changedValues.toString(), 10, listener.changedValues.size());
    listener.clear();
    pool.manage();
    totalValues = pool.aggregateTotalValues(pool.getCurrentValues());
    assertEquals("should not adjust (within deadband): " + listener.changedValues.toString(), 0, listener.changedValues.size());
  }

  @After
  public void teardownTest() throws Exception {
    resourceManager.close();
  }
}
