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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.MultipleFailureException;

/**
 *
 */
@LuceneTestCase.Slow
public class TestSolrCachePerf extends SolrTestCaseJ4 {

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final Class<? extends SolrCache>[] IMPLS = new Class[] {
      CaffeineCache.class,
      LRUCache.class,
      LFUCache.class,
      FastLRUCache.class
  };

  private final int NUM_KEYS = 5000;
  private final String[] keys = new String[NUM_KEYS];

  @Before
  public void setupKeys() {
    for (int i = 0; i < NUM_KEYS; i++) {
      keys[i] = String.valueOf(random().nextInt(100));
    }
  }

  @Test
  @BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-14094")
  public void testGetPutCompute() throws Exception {
    Map<String, SummaryStatistics> getPutRatio = new HashMap<>();
    Map<String, SummaryStatistics> computeRatio = new HashMap<>();
    Map<String, SummaryStatistics> getPutTime = new HashMap<>();
    Map<String, SummaryStatistics> computeTime = new HashMap<>();
    // warm-up
    int threads = 10;
    for (int i = 0; i < 10; i++) {
      doTestGetPutCompute(new HashMap<>(), new HashMap<>(), threads, false);
      doTestGetPutCompute(new HashMap<>(), new HashMap<>(), threads, true);
    }
    for (int i = 0; i < 100; i++) {
      doTestGetPutCompute(getPutRatio, getPutTime, threads, false);
      doTestGetPutCompute(computeRatio, computeTime, threads, true);
    }
    computeRatio.forEach((type, computeStats) -> {
      SummaryStatistics getPutStats = getPutRatio.get(type);
      assertGreaterThanOrEqual( "Cache " + type + ": compute ratio should be higher or equal to get/put ratio", computeStats.getMean(), getPutStats.getMean(), 0.001);
    });
  }

  private void assertGreaterThanOrEqual(String message, double greater, double smaller, double delta) {
    if (greater > smaller) {
      return;
    } else {
      if (Math.abs(greater - smaller) > delta) {
        fail(message + ": " + greater + " >= " + smaller);
      }
    }
  }

  static final String VALUE = "foo";

  @SuppressWarnings({"rawtypes"})
  private void doTestGetPutCompute(Map<String, SummaryStatistics> ratioStats, Map<String, SummaryStatistics> timeStats, int numThreads, boolean useCompute) throws Exception {
    for (Class<? extends SolrCache> clazz : IMPLS) {
      SolrMetricManager metricManager = new SolrMetricManager();
      @SuppressWarnings({"unchecked"})
      SolrCache<String, String> cache = clazz.getDeclaredConstructor().newInstance();
      Map<String, String> params = new HashMap<>();
      params.put("size", "" + NUM_KEYS);
      CacheRegenerator cr = new NoOpRegenerator();
      Object o = cache.init(params, null, cr);
      cache.setState(SolrCache.State.LIVE);
      cache.initializeMetrics(new SolrMetricsContext(metricManager, "foo", "bar"), "foo");
      AtomicBoolean stop = new AtomicBoolean();
      SummaryStatistics perImplRatio = ratioStats.computeIfAbsent(clazz.getSimpleName(), c -> new SummaryStatistics());
      SummaryStatistics perImplTime = timeStats.computeIfAbsent(clazz.getSimpleName(), c -> new SummaryStatistics());
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch stopLatch = new CountDownLatch(numThreads * NUM_KEYS);
      List<Thread> runners = new ArrayList<>();
      Set<Exception> exceptions = ConcurrentHashMap.newKeySet();
      for (int i = 0; i < numThreads; i++) {
        Thread t = new Thread(() -> {
          try {
            startLatch.await();
            int ik = 0;
            while (!stop.get()) {
              String key = keys[ik % NUM_KEYS];
              ik++;
              if (useCompute) {
                String value = cache.computeIfAbsent(key, k -> VALUE);
                assertNotNull(value);
              } else {
                String value = cache.get(key);
                if (value == null) {
                  // increase a likelihood of context switch
                  Thread.yield();
                  cache.put(key, VALUE);
                }
              }
              Thread.yield();
              stopLatch.countDown(); // Does this need to be in a finally block?
            }
          } catch (InterruptedException | IOException e) {
            exceptions.add(e);
          }
        });
        t.start();
        runners.add(t);
      }
      // fire them up
      long startTime = System.nanoTime();
      startLatch.countDown();
      stopLatch.await();
      stop.set(true);
      for (Thread t : runners) {
        t.join();
      }
      if (! exceptions.isEmpty()) {
        throw new MultipleFailureException(new ArrayList<>(exceptions));
      }
      long stopTime = System.nanoTime();
      Map<String, Object> metrics = cache.getMetricsSnapshot();
      perImplRatio.addValue(
          Double.parseDouble(String.valueOf(metrics.get("CACHE.foo.hitratio"))));
      perImplTime.addValue((double)(stopTime - startTime));
      cache.close();
    }
  }
}
