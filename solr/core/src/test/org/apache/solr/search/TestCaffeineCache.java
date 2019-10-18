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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Test for {@link CaffeineCache}.
 */
public class TestCaffeineCache extends SolrTestCase {

  SolrMetricManager metricManager = new SolrMetricManager();
  String registry = TestUtil.randomSimpleString(random(), 2, 10);
  String scope = TestUtil.randomSimpleString(random(), 2, 10);

  @Test
  public void testSimple() throws IOException {
    CaffeineCache<Integer, String> lfuCache = new CaffeineCache<>();
    lfuCache.initializeMetrics(metricManager, registry, "foo", scope + "-1");

    CaffeineCache<Integer, String> newLFUCache = new CaffeineCache<>();
    newLFUCache.initializeMetrics(metricManager, registry, "foo2", scope + "-2");

    Map<String, String> params = new HashMap<>();
    params.put("size", "100");
    params.put("initialSize", "10");
    params.put("autowarmCount", "25");

    NoOpRegenerator regenerator = new NoOpRegenerator();
    Object initObj = lfuCache.init(params, null, regenerator);
    lfuCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      lfuCache.put(i + 1, Integer.toString(i + 1));
    }
    assertEquals("15", lfuCache.get(15));
    assertEquals("75", lfuCache.get(75));
    assertEquals(null, lfuCache.get(110));
    Map<String, Object> nl = lfuCache.getMetricsMap().getValue();
    assertEquals(3L, nl.get("lookups"));
    assertEquals(2L, nl.get("hits"));
    assertEquals(101L, nl.get("inserts"));

    assertEquals(null, lfuCache.get(1));  // first item put in should be the first out

    // Test autowarming
    newLFUCache.init(params, initObj, regenerator);
    newLFUCache.warm(null, lfuCache);
    newLFUCache.setState(SolrCache.State.LIVE);

    newLFUCache.put(103, "103");
    assertEquals("15", newLFUCache.get(15));
    assertEquals("75", newLFUCache.get(75));
    assertEquals(null, newLFUCache.get(50));
    nl = newLFUCache.getMetricsMap().getValue();
    assertEquals(3L, nl.get("lookups"));
    assertEquals(2L, nl.get("hits"));
    assertEquals(1L, nl.get("inserts"));
    assertEquals(0L, nl.get("evictions"));

    assertEquals(7L, nl.get("cumulative_lookups"));
    assertEquals(4L, nl.get("cumulative_hits"));
    assertEquals(102L, nl.get("cumulative_inserts"));
  }

  @Test
  public void testTimeDecay() {
    Cache<Integer, String> cacheDecay = Caffeine.newBuilder()
        .executor(Runnable::run)
        .maximumSize(20)
        .build();
    for (int i = 1; i < 21; i++) {
      cacheDecay.put(i, Integer.toString(i));
    }
    Map<Integer, String> itemsDecay;

    // Now increase the freq count for 5 items
    for (int i = 0; i < 5; ++i) {
      for (int j = 0; j < 10; ++j) {
        cacheDecay.getIfPresent(i + 13);
      }
    }
    // OK, 13 - 17 should have larger counts and should stick past next few collections
    cacheDecay.put(22, "22");
    cacheDecay.put(23, "23");
    cacheDecay.put(24, "24");
    cacheDecay.put(25, "25");
    itemsDecay = cacheDecay.policy().eviction().get().hottest(10);
    // 13 - 17 should be in cache, but 11 and 18 (among others) should not. Testing that elements before and
    // after the ones with increased counts are removed, and all the increased count ones are still in the cache
    assertNull(itemsDecay.get(11));
    assertNull(itemsDecay.get(18));
    assertNotNull(itemsDecay.get(13));
    assertNotNull(itemsDecay.get(14));
    assertNotNull(itemsDecay.get(15));
    assertNotNull(itemsDecay.get(16));
    assertNotNull(itemsDecay.get(17));


    // Testing that all the elements in front of the ones with increased counts are gone
    for (int idx = 26; idx < 32; ++idx) {
      cacheDecay.put(idx, Integer.toString(idx));
    }
    //Surplus count should be at 0
    itemsDecay = cacheDecay.policy().eviction().get().hottest(10);
    assertNull(itemsDecay.get(20));
    assertNull(itemsDecay.get(24));
    assertNotNull(itemsDecay.get(13));
    assertNotNull(itemsDecay.get(14));
    assertNotNull(itemsDecay.get(15));
    assertNotNull(itemsDecay.get(16));
    assertNotNull(itemsDecay.get(17));
  }

  @Test
  public void testMaxIdleTime() throws Exception {
    int IDLE_TIME_SEC = 5;
    CountDownLatch removed = new CountDownLatch(1);
    AtomicReference<RemovalCause> removalCause = new AtomicReference<>();
    CaffeineCache<String, String> cache = new CaffeineCache<String, String>() {
      @Override
      public void onRemoval(String key, String value, RemovalCause cause) {
        super.onRemoval(key, value, cause);
        removalCause.set(cause);
        removed.countDown();
      }
    };
    Map<String, String> params = new HashMap<>();
    params.put("size", "6");
    params.put("maxIdleTime", "" + IDLE_TIME_SEC);
    cache.init(params, null, new NoOpRegenerator());

    cache.put("foo", "bar");
    assertEquals("bar", cache.get("foo"));
    // sleep for at least the idle time before inserting other entries
    // the eviction is piggy-backed on put()
    Thread.sleep(TimeUnit.SECONDS.toMillis(IDLE_TIME_SEC * 2));
    cache.put("abc", "xyz");
    boolean await = removed.await(30, TimeUnit.SECONDS);
    assertTrue("did not expire entry in in time", await);
    assertEquals(RemovalCause.EXPIRED, removalCause.get());
    assertNull(cache.get("foo"));
  }

  @Test
  public void testSetLimits() throws Exception {
    AtomicReference<CountDownLatch> removed = new AtomicReference<>(new CountDownLatch(2));
    List<RemovalCause> removalCauses = new ArrayList<>();
    List<String> removedKeys = new ArrayList<>();
    Set<String> allKeys = new HashSet<>();
    CaffeineCache<String, Accountable> cache = new CaffeineCache<String, Accountable>() {
      @Override
      public Accountable put(String key, Accountable val) {
        allKeys.add(key);
        return super.put(key, val);
      }

      @Override
      public void onRemoval(String key, Accountable value, RemovalCause cause) {
        super.onRemoval(key, value, cause);
        removalCauses.add(cause);
        removedKeys.add(key);
        removed.get().countDown();
      }
    };
    Map<String, String> params = new HashMap<>();
    params.put("size", "5");
    cache.init(params, null, new NoOpRegenerator());

    for (int i = 0; i < 5; i++) {
      cache.put("foo-" + i, new Accountable() {
        @Override
        public long ramBytesUsed() {
          return 1024 * 1024;
        }
      });
    }
    assertEquals(5, cache.size());
    // no evictions yet
    assertEquals(2, removed.get().getCount());

    cache.put("abc1", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 1;
      }
    });
    cache.put("abc2", new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 2;
      }
    });
    boolean await = removed.get().await(30, TimeUnit.SECONDS);
    assertTrue("did not evict entries in in time", await);
    assertEquals(5, cache.size());
    assertEquals(2, cache.get("abc2").ramBytesUsed());
    for (String key : removedKeys) {
      assertNull("key " + key + " still present!", cache.get(key));
      allKeys.remove(key);
    }
    for (RemovalCause cause : removalCauses) {
      assertEquals(RemovalCause.SIZE, cause);
    }

    removed.set(new CountDownLatch(2));
    removalCauses.clear();
    removedKeys.clear();
    // trim down by item count
    cache.setMaxSize(3);
    cache.put("abc3",  new Accountable() {
      @Override
      public long ramBytesUsed() {
        return 3;
      }
    });
    await = removed.get().await(30, TimeUnit.SECONDS);
    assertTrue("did not evict entries in in time", await);
    assertEquals(3, cache.size());
    for (String key : removedKeys) {
      assertNull("key " + key + " still present!", cache.get(key));
      allKeys.remove(key);
    }
    for (RemovalCause cause : removalCauses) {
      assertEquals(RemovalCause.SIZE, cause);
    }

    // at least one item has to go
    removed.set(new CountDownLatch(1));
    removalCauses.clear();
    removedKeys.clear();
    // trim down by ram size
    cache.setMaxRamMB(1);
    await = removed.get().await(30, TimeUnit.SECONDS);
    assertTrue("did not evict entries in in time", await);
    for (String key : removedKeys) {
      assertNull("key " + key + " still present!", cache.get(key));
      allKeys.remove(key);
    }
    for (RemovalCause cause : removalCauses) {
      assertEquals(RemovalCause.SIZE, cause);
    }
    // check total size of remaining items
    long total = 0;
    for (String key : allKeys) {
      Accountable a = cache.get(key);
      assertNotNull("missing value for key " + key, a);
      total += a.ramBytesUsed();
    }
    assertTrue("total ram bytes should be greater than 0", total > 0);
    assertTrue("total ram bytes exceeded limit", total < 1024 * 1024);
    cache.close();
  }
}
