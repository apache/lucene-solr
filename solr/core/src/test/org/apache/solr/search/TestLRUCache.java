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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.SolrMetricManager;

/**
 * Test for <code>org.apache.solr.search.LRUCache</code>
 */
public class TestLRUCache extends LuceneTestCase {

  SolrMetricManager metricManager = new SolrMetricManager();
  String registry = TestUtil.randomSimpleString(random(), 2, 10);
  String scope = TestUtil.randomSimpleString(random(), 2, 10);

  public void testFullAutowarm() throws IOException {
    LRUCache<Object, Object> lruCache = new LRUCache<>();
    Map<String, String> params = new HashMap<>();
    params.put("size", "100");
    params.put("initialSize", "10");
    params.put("autowarmCount", "100%");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = lruCache.init(params, null, cr);
    lruCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      lruCache.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", lruCache.get(25));
    assertEquals(null, lruCache.get(110));
    assertEquals(null, lruCache.get(1));  // first item put in should be the first out
    LRUCache<Object, Object> lruCacheNew = new LRUCache<>();
    lruCacheNew.init(params, o, cr);
    lruCacheNew.warm(null, lruCache);
    lruCacheNew.setState(SolrCache.State.LIVE);
    lruCache.close();
    lruCacheNew.put(103, "103");
    assertEquals("90", lruCacheNew.get(90));
    assertEquals("50", lruCacheNew.get(50));
    lruCacheNew.close();
  }
  
  public void testPercentageAutowarm() throws IOException {
      doTestPercentageAutowarm(100, 50, new int[]{51, 55, 60, 70, 80, 99, 100}, new int[]{1, 2, 3, 5, 10, 20, 30, 40, 50});
      doTestPercentageAutowarm(100, 25, new int[]{76, 80, 99, 100}, new int[]{1, 2, 3, 5, 10, 20, 30, 40, 50, 51, 55, 60, 70});
      doTestPercentageAutowarm(1000, 10, new int[]{901, 930, 950, 999, 1000}, new int[]{1, 5, 100, 200, 300, 400, 800, 899, 900});
      doTestPercentageAutowarm(10, 10, new int[]{10}, new int[]{1, 5, 9, 100, 200, 300, 400, 800, 899, 900});
  }
  
  private void doTestPercentageAutowarm(int limit, int percentage, int[] hits, int[]misses) {
    LRUCache<Object, Object> lruCache = new LRUCache<>();
    Map<String, String> params = new HashMap<>();
    params.put("size", String.valueOf(limit));
    params.put("initialSize", "10");
    params.put("autowarmCount", percentage + "%");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = lruCache.init(params, null, cr);
    lruCache.setState(SolrCache.State.LIVE);
    for (int i = 1; i <= limit; i++) {
      lruCache.put(i, "" + i);//adds numbers from 1 to 100
    }

    LRUCache<Object, Object> lruCacheNew = new LRUCache<>();
    lruCacheNew.init(params, o, cr);
    lruCacheNew.warm(null, lruCache);
    lruCacheNew.setState(SolrCache.State.LIVE);
    lruCache.close();
      
    for(int hit:hits) {
      assertEquals("The value " + hit + " should be on new cache", String.valueOf(hit), lruCacheNew.get(hit));
    }
      
    for(int miss:misses) {
      assertEquals("The value " + miss + " should NOT be on new cache", null, lruCacheNew.get(miss));
    }
    lruCacheNew.close();
  }
  
  @SuppressWarnings("unchecked")
  public void testNoAutowarm() throws IOException {
    LRUCache<Object, Object> lruCache = new LRUCache<>();
    lruCache.initializeMetrics(metricManager, registry, "foo", scope);
    Map<String, String> params = new HashMap<>();
    params.put("size", "100");
    params.put("initialSize", "10");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = lruCache.init(params, null, cr);
    lruCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      lruCache.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", lruCache.get(25));
    assertEquals(null, lruCache.get(110));
    Map<String,Object> nl = lruCache.getMetricsMap().getValue();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(101L, nl.get("inserts"));
    assertEquals(null, lruCache.get(1));  // first item put in should be the first out
    LRUCache<Object, Object> lruCacheNew = new LRUCache<>();
    lruCacheNew.init(params, o, cr);
    lruCacheNew.warm(null, lruCache);
    lruCacheNew.setState(SolrCache.State.LIVE);
    lruCache.close();
    lruCacheNew.put(103, "103");
    assertEquals(null, lruCacheNew.get(90));
    assertEquals(null, lruCacheNew.get(50));
    lruCacheNew.close();
  }

  public void testMaxRamSize() throws Exception {
    LRUCache<String, Accountable> accountableLRUCache = new LRUCache<>();
    accountableLRUCache.initializeMetrics(metricManager, registry, "foo", scope);
    Map<String, String> params = new HashMap<>();
    params.put("size", "5");
    params.put("maxRamMB", "1");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = accountableLRUCache.init(params, null, cr);
    long baseSize = accountableLRUCache.ramBytesUsed();
    assertEquals(LRUCache.BASE_RAM_BYTES_USED, baseSize);
    accountableLRUCache.put("1", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 512 * 1024;
      }
    });
    assertEquals(1, accountableLRUCache.size());
    assertEquals(baseSize + 512 * 1024 + LRUCache.DEFAULT_RAM_BYTES_USED + LRUCache.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY, accountableLRUCache.ramBytesUsed());
    accountableLRUCache.put("2", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 512 * 1024;
      }
    });
    assertEquals(1, accountableLRUCache.size());
    assertEquals(baseSize + 512 * 1024 + LRUCache.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + LRUCache.DEFAULT_RAM_BYTES_USED, accountableLRUCache.ramBytesUsed());
    Map<String,Object> nl = accountableLRUCache.getMetricsMap().getValue();
    assertEquals(1L, nl.get("evictions"));
    assertEquals(1L, nl.get("evictionsRamUsage"));
    accountableLRUCache.put("3", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 1024;
      }
    });
    nl = accountableLRUCache.getMetricsMap().getValue();
    assertEquals(1L, nl.get("evictions"));
    assertEquals(1L, nl.get("evictionsRamUsage"));
    assertEquals(2L, accountableLRUCache.size());
    assertEquals(baseSize + 513 * 1024 + LRUCache.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY * 2 + LRUCache.DEFAULT_RAM_BYTES_USED * 2, accountableLRUCache.ramBytesUsed());

    accountableLRUCache.clear();
    assertEquals(RamUsageEstimator.shallowSizeOfInstance(LRUCache.class), accountableLRUCache.ramBytesUsed());
  }

  public void testNonAccountableValues() throws Exception {
    LRUCache<String, String> cache = new LRUCache<>();
    Map<String, String> params = new HashMap<>();
    params.put("size", "5");
    params.put("maxRamMB", "1");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = cache.init(params, null, cr);

    expectThrows(SolrException.class, "Adding a non-accountable value to a cache configured with maxRamBytes should have failed",
        () -> cache.put("1", "1")
    );
  }
}
