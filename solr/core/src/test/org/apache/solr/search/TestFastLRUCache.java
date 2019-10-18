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

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.ConcurrentLRUCache;
import org.apache.solr.util.RTimer;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Test for FastLRUCache
 *
 *
 * @see org.apache.solr.search.FastLRUCache
 * @since solr 1.4
 */
public class TestFastLRUCache extends SolrTestCase {
  SolrMetricManager metricManager = new SolrMetricManager();
  String registry = TestUtil.randomSimpleString(random(), 2, 10);
  String scope = TestUtil.randomSimpleString(random(), 2, 10);

  public void testPercentageAutowarm() throws Exception {
    FastLRUCache<Object, Object> fastCache = new FastLRUCache<>();
    Map<String, String> params = new HashMap<>();
    params.put("size", "100");
    params.put("initialSize", "10");
    params.put("autowarmCount", "100%");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = fastCache.init(params, null, cr);
    fastCache.initializeMetrics(metricManager, registry, "foo", scope);
    MetricsMap metrics = fastCache.getMetricsMap();
    fastCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      fastCache.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", fastCache.get(25));
    assertEquals(null, fastCache.get(110));
    Map<String,Object> nl = metrics.getValue();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(101L, nl.get("inserts"));
    assertEquals(null, fastCache.get(1));  // first item put in should be the first out
    FastLRUCache<Object, Object> fastCacheNew = new FastLRUCache<>();
    fastCacheNew.init(params, o, cr);
    fastCacheNew.initializeMetrics(metricManager, registry, "foo", scope);
    metrics = fastCacheNew.getMetricsMap();
    fastCacheNew.warm(null, fastCache);
    fastCacheNew.setState(SolrCache.State.LIVE);
    fastCache.close();
    fastCacheNew.put(103, "103");
    assertEquals("90", fastCacheNew.get(90));
    assertEquals("50", fastCacheNew.get(50));
    nl = metrics.getValue();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(2L, nl.get("hits"));
    assertEquals(1L, nl.get("inserts"));
    assertEquals(0L, nl.get("evictions"));
    assertEquals(5L, nl.get("cumulative_lookups"));
    assertEquals(3L, nl.get("cumulative_hits"));
    assertEquals(102L, nl.get("cumulative_inserts"));
    fastCacheNew.close();
  }
  
  public void testPercentageAutowarmMultiple() throws Exception {
    doTestPercentageAutowarm(100, 50, new int[]{51, 55, 60, 70, 80, 99, 100}, new int[]{1, 2, 3, 5, 10, 20, 30, 40, 50});
    doTestPercentageAutowarm(100, 25, new int[]{76, 80, 99, 100}, new int[]{1, 2, 3, 5, 10, 20, 30, 40, 50, 51, 55, 60, 70});
    doTestPercentageAutowarm(1000, 10, new int[]{901, 930, 950, 999, 1000}, new int[]{1, 5, 100, 200, 300, 400, 800, 899, 900});
    doTestPercentageAutowarm(100, 200, new int[]{1, 10, 25, 51, 55, 60, 70, 80, 99, 100}, new int[]{200, 300});
    doTestPercentageAutowarm(100, 0, new int[]{}, new int[]{1, 10, 25, 51, 55, 60, 70, 80, 99, 100, 200, 300});
  }
  
  private void doTestPercentageAutowarm(int limit, int percentage, int[] hits, int[]misses) throws Exception {
    FastLRUCache<Object, Object> fastCache = new FastLRUCache<>();
    Map<String, String> params = new HashMap<>();
    params.put("size", String.valueOf(limit));
    params.put("initialSize", "10");
    params.put("autowarmCount", percentage + "%");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = fastCache.init(params, null, cr);
    fastCache.initializeMetrics(metricManager, registry, "foo", scope);
    fastCache.setState(SolrCache.State.LIVE);
    for (int i = 1; i <= limit; i++) {
      fastCache.put(i, "" + i);//adds numbers from 1 to 100
    }

    FastLRUCache<Object, Object> fastCacheNew = new FastLRUCache<>();
    fastCacheNew.init(params, o, cr);
    fastCacheNew.initializeMetrics(metricManager, registry, "foo", scope);
    fastCacheNew.warm(null, fastCache);
    fastCacheNew.setState(SolrCache.State.LIVE);
    fastCache.close();
      
    for(int hit:hits) {
      assertEquals("The value " + hit + " should be on new cache", String.valueOf(hit), fastCacheNew.get(hit));
    }
      
    for(int miss:misses) {
      assertEquals("The value " + miss + " should NOT be on new cache", null, fastCacheNew.get(miss));
    }
    Map<String,Object> nl = fastCacheNew.getMetricsMap().getValue();
    assertEquals(Long.valueOf(hits.length + misses.length), nl.get("lookups"));
    assertEquals(Long.valueOf(hits.length), nl.get("hits"));
    fastCacheNew.close();
  }
  
  public void testNoAutowarm() throws Exception {
    FastLRUCache<Object, Object> fastCache = new FastLRUCache<>();
    Map<String, String> params = new HashMap<>();
    params.put("size", "100");
    params.put("initialSize", "10");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = fastCache.init(params, null, cr);
    fastCache.initializeMetrics(metricManager, registry, "foo", scope);
    fastCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      fastCache.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", fastCache.get(25));
    assertEquals(null, fastCache.get(110));
    Map<String,Object> nl = fastCache.getMetricsMap().getValue();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(101L, nl.get("inserts"));
    assertEquals(null, fastCache.get(1));  // first item put in should be the first out
    FastLRUCache<Object, Object> fastCacheNew = new FastLRUCache<>();
    fastCacheNew.init(params, o, cr);
    fastCacheNew.warm(null, fastCache);
    fastCacheNew.setState(SolrCache.State.LIVE);
    fastCache.close();
    fastCacheNew.put(103, "103");
    assertEquals(null, fastCacheNew.get(90));
    assertEquals(null, fastCacheNew.get(50));
    fastCacheNew.close();
  }
  
  public void testFullAutowarm() throws Exception {
    FastLRUCache<Object, Object> cache = new FastLRUCache<>();
    Map<Object, Object> params = new HashMap<>();
    params.put("size", "100");
    params.put("initialSize", "10");
    params.put("autowarmCount", "-1");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = cache.init(params, null, cr);
    cache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      cache.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", cache.get(25));
    assertEquals(null, cache.get(110));

    assertEquals(null, cache.get(1));  // first item put in should be the first out


    FastLRUCache<Object, Object> cacheNew = new FastLRUCache<>();
    cacheNew.init(params, o, cr);
    cacheNew.warm(null, cache);
    cacheNew.setState(SolrCache.State.LIVE);
    cache.close();
    cacheNew.put(103, "103");
    assertEquals("90", cacheNew.get(90));
    assertEquals("50", cacheNew.get(50));
    assertEquals("103", cacheNew.get(103));
    cacheNew.close();
  }
  
  public void testSimple() throws Exception {
    FastLRUCache sc = new FastLRUCache();
    Map l = new HashMap();
    l.put("size", "100");
    l.put("initialSize", "10");
    l.put("autowarmCount", "25");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = sc.init(l, null, cr);
    sc.initializeMetrics(metricManager, registry, "foo", scope);
    sc.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      sc.put(i + 1, "" + (i + 1));
    }
    assertEquals("25", sc.get(25));
    assertEquals(null, sc.get(110));
    MetricsMap metrics = sc.getMetricsMap();
    Map<String,Object> nl = metrics.getValue();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(101L, nl.get("inserts"));

    assertEquals(null, sc.get(1));  // first item put in should be the first out


    FastLRUCache scNew = new FastLRUCache();
    scNew.init(l, o, cr);
    scNew.initializeMetrics(metricManager, registry, "foo", scope);
    scNew.warm(null, sc);
    scNew.setState(SolrCache.State.LIVE);
    sc.close();
    scNew.put(103, "103");
    assertEquals("90", scNew.get(90));
    assertEquals(null, scNew.get(50));
    nl = scNew.getMetricsMap().getValue();
    assertEquals(2L, nl.get("lookups"));
    assertEquals(1L, nl.get("hits"));
    assertEquals(1L, nl.get("inserts"));
    assertEquals(0L, nl.get("evictions"));

    assertEquals(5L, nl.get("cumulative_lookups"));
    assertEquals(2L, nl.get("cumulative_hits"));
    assertEquals(102L, nl.get("cumulative_inserts"));
    scNew.close();
  }

  public void testOldestItems() {
    ConcurrentLRUCache<Integer, String> cache = new ConcurrentLRUCache<>(100, 90);
    for (int i = 0; i < 50; i++) {
      cache.put(i + 1, "" + (i + 1));
    }
    cache.get(1);
    cache.get(3);
    Map<Integer, String> m = cache.getOldestAccessedItems(5);
    //7 6 5 4 2
    assertNotNull(m.get(7));
    assertNotNull(m.get(6));
    assertNotNull(m.get(5));
    assertNotNull(m.get(4));
    assertNotNull(m.get(2));

    m = cache.getOldestAccessedItems(0);
    assertTrue(m.isEmpty());

    //test this too
    m = cache.getLatestAccessedItems(0);
    assertTrue(m.isEmpty());

    cache.destroy();
  }

  // enough randomness to exercise all of the different cache purging phases
  public void testRandom() {
    int sz = random().nextInt(100)+5;
    int lowWaterMark = random().nextInt(sz-3)+1;
    int keyrange = random().nextInt(sz*3)+1;
    ConcurrentLRUCache<Integer, String> cache = new ConcurrentLRUCache<>(sz, lowWaterMark);
    for (int i=0; i<10000; i++) {
      cache.put(random().nextInt(keyrange), "");
      cache.get(random().nextInt(keyrange));
    }
  }

  void doPerfTest(int iter, int cacheSize, int maxKey) {
    final RTimer timer = new RTimer();

    int lowerWaterMark = cacheSize;
    int upperWaterMark = (int)(lowerWaterMark * 1.1);

    Random r = random();
    ConcurrentLRUCache cache = new ConcurrentLRUCache(upperWaterMark, lowerWaterMark, (upperWaterMark+lowerWaterMark)/2, upperWaterMark, false, false, null, -1);
    boolean getSize=false;
    int minSize=0,maxSize=0;
    for (int i=0; i<iter; i++) {
      cache.put(r.nextInt(maxKey),"TheValue");
      int sz = cache.size();
      if (!getSize && sz >= cacheSize) {
        getSize = true;
        minSize = sz;
      } else {
        if (sz < minSize) minSize=sz;
        else if (sz > maxSize) maxSize=sz;
      }
    }
    cache.destroy();

    System.out.println("time=" + timer.getTime() + ", minSize="+minSize+",maxSize="+maxSize);
  }

  public void testAccountable() throws Exception {
    FastLRUCache<Query, DocSet> sc = new FastLRUCache<>();
    try {
      Map l = new HashMap();
      l.put("size", "100");
      l.put("initialSize", "10");
      l.put("autowarmCount", "25");
      CacheRegenerator cr = new NoOpRegenerator();
      Object o = sc.init(l, null, cr);
      sc.initializeMetrics(metricManager, registry, "foo", scope);
      sc.setState(SolrCache.State.LIVE);
      long initialBytes = sc.ramBytesUsed();
      WildcardQuery q = new WildcardQuery(new Term("foo", "bar"));
      DocSet docSet = new BitDocSet();
      sc.put(q, docSet);
      long updatedBytes = sc.ramBytesUsed();
      assertTrue(updatedBytes > initialBytes);
      long estimated = initialBytes + q.ramBytesUsed() + docSet.ramBytesUsed() + ConcurrentLRUCache.CacheEntry.BASE_RAM_BYTES_USED
          + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
      assertEquals(estimated, updatedBytes);
      sc.clear();
      long clearedBytes = sc.ramBytesUsed();
      assertEquals(initialBytes, clearedBytes);
    } finally {
      sc.close();
    }
  }

  public void testSetLimits() throws Exception {
    FastLRUCache<String, Accountable> cache = new FastLRUCache<>();
    Map<String, String> params = new HashMap<>();
    params.put("size", "6");
    params.put("maxRamMB", "8");
    CacheRegenerator cr = new NoOpRegenerator();
    Object o = cache.init(params, null, cr);
    cache.initializeMetrics(metricManager, registry, "foo", scope);
    for (int i = 0; i < 6; i++) {
      cache.put("" + i, new Accountable() {
        @Override
        public long ramBytesUsed() {
          return 1024 * 1024;
        }
      });
    }
    // no evictions yet
    assertEquals(6, cache.size());
    // this also sets minLimit = 4
    cache.setMaxSize(5);
    // should not happen yet - evictions are triggered by put
    assertEquals(6, cache.size());
    cache.put("6", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 1024 * 1024;
      }
    });
    // should evict to minLimit
    assertEquals(4, cache.size());

    // modify ram limit
    cache.setMaxRamMB(3);
    // should not happen yet - evictions are triggered by put
    assertEquals(4, cache.size());
    // this evicts down to 3MB * 0.8, ie. ramLowerWaterMark
    cache.put("7", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 0;
      }
    });
    assertEquals(3, cache.size());
    assertNotNull("5", cache.get("5"));
    assertNotNull("6", cache.get("6"));
    assertNotNull("7", cache.get("7"));

    // scale up

    cache.setMaxRamMB(4);
    cache.put("8", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 1024 * 1024;
      }
    });
    assertEquals(4, cache.size());

    cache.setMaxSize(10);
    for (int i = 0; i < 6; i++) {
      cache.put("new" + i, new Accountable() {
        @Override
        public long ramBytesUsed() {
          return 0;
        }
      });
    }
    assertEquals(10, cache.size());
  }

  public void testMaxIdleTime() throws Exception {
    int IDLE_TIME_SEC = 600;
    long IDLE_TIME_NS = TimeUnit.NANOSECONDS.convert(IDLE_TIME_SEC, TimeUnit.SECONDS);
    CountDownLatch sweepFinished = new CountDownLatch(1);
    ConcurrentLRUCache<String, Accountable> cache = new ConcurrentLRUCache(6, 5, 5, 6, false, false, null, IDLE_TIME_SEC) {
      @Override
      public void markAndSweep() {
        super.markAndSweep();
        sweepFinished.countDown();
      }
    };
    long currentTime = TimeSource.NANO_TIME.getEpochTimeNs();
    for (int i = 0; i < 4; i++) {
      cache.putCacheEntry(new ConcurrentLRUCache.CacheEntry<>("" + i, new Accountable() {
        @Override
        public long ramBytesUsed() {
          return 1024 * 1024;
        }
      }, currentTime, 0));
    }
    // no evictions yet
    assertEquals(4, cache.size());
    assertEquals("markAndSweep spurious run", 1, sweepFinished.getCount());
    cache.putCacheEntry(new ConcurrentLRUCache.CacheEntry<>("4", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 0;
      }
    }, currentTime - IDLE_TIME_NS * 2, 0));
    boolean await = sweepFinished.await(10, TimeUnit.SECONDS);
    assertTrue("did not evict entries in time", await);
    assertEquals(4, cache.size());
    assertNull(cache.get("4"));
  }

  /***
      public void testPerf() {
      doPerfTest(1000000, 100000, 200000); // big cache, warmup
      doPerfTest(2000000, 100000, 200000); // big cache
      doPerfTest(2000000, 100000, 120000);  // smaller key space increases distance between oldest, newest and makes the first passes less effective.
      doPerfTest(6000000, 1000, 2000);    // small cache, smaller hit rate
      doPerfTest(6000000, 1000, 1200);    // small cache, bigger hit rate
      }
  ***/

  // returns number of puts
  int useCache(SolrCache sc, int numGets, int maxKey, int seed) {
    int ret = 0;
    Random r = new Random(seed);

    // use like a cache... gets and a put if not found
    for (int i=0; i<numGets; i++) {
      Integer k = r.nextInt(maxKey);
      Integer v = (Integer)sc.get(k);
      if (v == null) {
        sc.put(k, k);
        ret++;
      }
    }

    return ret;
  }

  void fillCache(SolrCache sc, int cacheSize, int maxKey) {
    for (int i=0; i<cacheSize; i++) {
      Integer kv = random().nextInt(maxKey);
      sc.put(kv,kv);
    }
  }


  double[] cachePerfTest(final SolrCache sc, final int nThreads, final int numGets, int cacheSize, final int maxKey) {
    Map l = new HashMap();
    l.put("size", ""+cacheSize);
    l.put("initialSize", ""+cacheSize);

    Object o = sc.init(l, null, null);
    sc.setState(SolrCache.State.LIVE);

    fillCache(sc, cacheSize, maxKey);

    final RTimer timer = new RTimer();

    Thread[] threads = new Thread[nThreads];
    final AtomicInteger puts = new AtomicInteger(0);
    for (int i=0; i<threads.length; i++) {
      final int seed=random().nextInt();
      threads[i] = new Thread() {
          @Override
          public void run() {
            int ret = useCache(sc, numGets/nThreads, maxKey, seed);
            puts.addAndGet(ret);
          }
        };
    }

    for (Thread thread : threads) {
      try {
        thread.start();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    double time = timer.getTime();
    double hitRatio = (1-(((double)puts.get())/numGets));
//    System.out.println("time=" + time + " impl=" +sc.getClass().getSimpleName()
//                       +" nThreads= " + nThreads + " size="+cacheSize+" maxKey="+maxKey+" gets="+numGets
//                       +" hitRatio="+(1-(((double)puts.get())/numGets)));
    return new double[]{time, hitRatio};
  }

  private int NUM_RUNS = 5;
  void perfTestBoth(int maxThreads, int numGets, int cacheSize, int maxKey,
                    Map<String, Map<String, SummaryStatistics>> timeStats,
                    Map<String, Map<String, SummaryStatistics>> hitStats) {
    for (int nThreads = 1 ; nThreads <= maxThreads; nThreads++) {
      String testKey = "threads=" + nThreads + ",gets=" + numGets + ",size=" + cacheSize + ",maxKey=" + maxKey;
      System.err.println(testKey);
      for (int i = 0; i < NUM_RUNS; i++) {
        double[] data = cachePerfTest(new LRUCache(), nThreads, numGets, cacheSize, maxKey);
        timeStats.computeIfAbsent(testKey, k -> new TreeMap<>())
            .computeIfAbsent("LRUCache", k -> new SummaryStatistics())
            .addValue(data[0]);
        hitStats.computeIfAbsent(testKey, k -> new TreeMap<>())
            .computeIfAbsent("LRUCache", k -> new SummaryStatistics())
            .addValue(data[1]);
        data = cachePerfTest(new CaffeineCache(), nThreads, numGets, cacheSize, maxKey);
        timeStats.computeIfAbsent(testKey, k -> new TreeMap<>())
            .computeIfAbsent("CaffeineCache", k -> new SummaryStatistics())
            .addValue(data[0]);
        hitStats.computeIfAbsent(testKey, k -> new TreeMap<>())
            .computeIfAbsent("CaffeineCache", k -> new SummaryStatistics())
            .addValue(data[1]);
        data = cachePerfTest(new FastLRUCache(), nThreads, numGets, cacheSize, maxKey);
        timeStats.computeIfAbsent(testKey, k -> new TreeMap<>())
            .computeIfAbsent("FastLRUCache", k -> new SummaryStatistics())
            .addValue(data[0]);
        hitStats.computeIfAbsent(testKey, k -> new TreeMap<>())
            .computeIfAbsent("FastLRUCache", k -> new SummaryStatistics())
            .addValue(data[1]);
      }
    }
  }

  int NUM_THREADS = 4;
  /***
      public void testCachePerf() {
        Map<String, Map<String, SummaryStatistics>> timeStats = new TreeMap<>();
        Map<String, Map<String, SummaryStatistics>> hitStats = new TreeMap<>();
      // warmup
      perfTestBoth(NUM_THREADS, 100000, 100000, 120000, new HashMap<>(), new HashMap());

      perfTestBoth(NUM_THREADS, 2000000, 100000, 100000, timeStats, hitStats); // big cache, 100% hit ratio
      perfTestBoth(NUM_THREADS, 2000000, 100000, 120000, timeStats, hitStats); // big cache, bigger hit ratio
      perfTestBoth(NUM_THREADS, 2000000, 100000, 200000, timeStats, hitStats); // big cache, ~50% hit ratio
      perfTestBoth(NUM_THREADS, 2000000, 100000, 1000000, timeStats, hitStats); // big cache, ~10% hit ratio

      perfTestBoth(NUM_THREADS, 2000000, 1000, 1000, timeStats, hitStats); // small cache, ~100% hit ratio
      perfTestBoth(NUM_THREADS, 2000000, 1000, 1200, timeStats, hitStats); // small cache, bigger hit ratio
      perfTestBoth(NUM_THREADS, 2000000, 1000, 2000, timeStats, hitStats); // small cache, ~50% hit ratio
      perfTestBoth(NUM_THREADS, 2000000, 1000, 10000, timeStats, hitStats); // small cache, ~10% hit ratio

        System.out.println("\n=====================\n");
        timeStats.forEach((testKey, map) -> {
          Map<String, SummaryStatistics> hits = hitStats.get(testKey);
          System.out.println("* " + testKey);
          map.forEach((type, summary) -> {
            System.out.println("\t" + String.format("%14s", type) + "\ttime " + summary.getMean() + "\thitRatio " + hits.get(type).getMean());
          });
        });
      }
  ***/


}
