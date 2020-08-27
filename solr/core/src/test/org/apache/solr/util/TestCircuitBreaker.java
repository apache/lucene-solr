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

package org.apache.solr.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.circuitbreaker.CPUCircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerManager;
import org.apache.solr.util.circuitbreaker.MemoryCircuitBreaker;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.hamcrest.CoreMatchers.containsString;

@SuppressWarnings({"rawtypes"})
public class TestCircuitBreaker extends SolrTestCaseJ4 {
  private final static int NUM_DOCS = 20;

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  @BeforeClass
  public static void setUpClass() throws Exception {
    System.setProperty("filterCache.enabled", "false");
    System.setProperty("queryResultCache.enabled", "false");
    System.setProperty("documentCache.enabled", "true");

    initCore("solrconfig-memory-circuitbreaker.xml", "schema.xml");
    for (int i = 0 ; i < NUM_DOCS ; i ++) {
      assertU(adoc("name", "john smith", "id", "1"));
      assertU(adoc("name", "johathon smith", "id", "2"));
      assertU(adoc("name", "john percival smith", "id", "3"));
      assertU(adoc("id", "1", "title", "this is a title.", "inStock_b1", "true"));
      assertU(adoc("id", "2", "title", "this is another title.", "inStock_b1", "true"));
      assertU(adoc("id", "3", "title", "Mary had a little lamb.", "inStock_b1", "false"));

      //commit inside the loop to get multiple segments to make search as realistic as possible
      assertU(commit());
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @After
  public void after() {
    h.getCore().getCircuitBreakerManager().deregisterAll();
  }

  public void testCBAlwaysTrips() {
    HashMap<String, String> args = new HashMap<String, String>();

    args.put(QueryParsing.DEFTYPE, CircuitBreaker.NAME);
    args.put(CommonParams.FL, "id");

    removeAllExistingCircuitBreakers();

    PluginInfo pluginInfo = h.getCore().getSolrConfig().getPluginInfo(CircuitBreakerManager.class.getName());

    CircuitBreaker.CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerManager.buildCBConfig(pluginInfo);

    CircuitBreaker circuitBreaker = new MockCircuitBreaker(circuitBreakerConfig);

    h.getCore().getCircuitBreakerManager().register(circuitBreaker);

    expectThrows(SolrException.class, () -> {
      h.query(req("name:\"john smith\""));
    });
  }

  public void testCBFakeMemoryPressure() {
    HashMap<String, String> args = new HashMap<String, String>();

    args.put(QueryParsing.DEFTYPE, CircuitBreaker.NAME);
    args.put(CommonParams.FL, "id");

    removeAllExistingCircuitBreakers();

    PluginInfo pluginInfo = h.getCore().getSolrConfig().getPluginInfo(CircuitBreakerManager.class.getName());

    CircuitBreaker.CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerManager.buildCBConfig(pluginInfo);
    CircuitBreaker circuitBreaker = new FakeMemoryPressureCircuitBreaker(circuitBreakerConfig);

    h.getCore().getCircuitBreakerManager().register(circuitBreaker);

    expectThrows(SolrException.class, () -> {
      h.query(req("name:\"john smith\""));
    });
  }

  public void testBuildingMemoryPressure() {
    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool(
        new SolrNamedThreadFactory("TestCircuitBreaker"));
    HashMap<String, String> args = new HashMap<String, String>();

    args.put(QueryParsing.DEFTYPE, CircuitBreaker.NAME);
    args.put(CommonParams.FL, "id");

    AtomicInteger failureCount = new AtomicInteger();

    try {
      removeAllExistingCircuitBreakers();

      PluginInfo pluginInfo = h.getCore().getSolrConfig().getPluginInfo(CircuitBreakerManager.class.getName());

      CircuitBreaker.CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerManager.buildCBConfig(pluginInfo);
      CircuitBreaker circuitBreaker = new BuildingUpMemoryPressureCircuitBreaker(circuitBreakerConfig);

      h.getCore().getCircuitBreakerManager().register(circuitBreaker);

      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 5; i++) {
        Future<?> future = executor.submit(() -> {
          try {
            h.query(req("name:\"john smith\""));
          } catch (SolrException e) {

            assertThat(e.getMessage(), containsString("Circuit Breakers tripped"));
            failureCount.incrementAndGet();
          } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
          }
        });

        futures.add(future);
      }

      for  (Future<?> future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage());
        }
      }

      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e.getMessage());
      }

      assertEquals("Number of failed queries is not correct", 1, failureCount.get());
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdown();
      }
    }
  }

  public void testFakeCPUCircuitBreaker() {
    AtomicInteger failureCount = new AtomicInteger();

    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool(
        new SolrNamedThreadFactory("TestCircuitBreaker"));
    try {
      removeAllExistingCircuitBreakers();

      PluginInfo pluginInfo = h.getCore().getSolrConfig().getPluginInfo(CircuitBreakerManager.class.getName());

      CircuitBreaker.CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerManager.buildCBConfig(pluginInfo);
      CircuitBreaker circuitBreaker = new FakeCPUCircuitBreaker(circuitBreakerConfig);

      h.getCore().getCircuitBreakerManager().register(circuitBreaker);

      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 5; i++) {
        Future<?> future = executor.submit(() -> {
          try {
            h.query(req("name:\"john smith\""));
          } catch (SolrException e) {
            assertThat(e.getMessage(), containsString("Circuit Breakers tripped"));
            failureCount.incrementAndGet();
          } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
          }
        });

        futures.add(future);
      }

      for  (Future<?> future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage());
        }
      }

      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e.getMessage());
      }

      assertEquals("Number of failed queries is not correct",5, failureCount.get());
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdown();
      }
    }
  }

  public void testResponseWithCBTiming() {
    assertQ(req("q", "*:*", CommonParams.DEBUG_QUERY, "true"),
        "//str[@name='rawquerystring']='*:*'",
        "//str[@name='querystring']='*:*'",
        "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
        "//str[@name='parsedquery_toString']='*:*'",
        "count(//lst[@name='explain']/*)=3",
        "//lst[@name='explain']/str[@name='1']",
        "//lst[@name='explain']/str[@name='2']",
        "//lst[@name='explain']/str[@name='3']",
        "//str[@name='QParser']",
        "count(//lst[@name='timing']/*)=4",
        "//lst[@name='timing']/double[@name='time']",
        "count(//lst[@name='circuitbreaker']/*)>0",
        "//lst[@name='circuitbreaker']/double[@name='time']",
        "count(//lst[@name='prepare']/*)>0",
        "//lst[@name='prepare']/double[@name='time']",
        "count(//lst[@name='process']/*)>0",
        "//lst[@name='process']/double[@name='time']"
    );
  }

  private void removeAllExistingCircuitBreakers() {
    List<CircuitBreaker> registeredCircuitBreakers = h.getCore().getCircuitBreakerManager().getRegisteredCircuitBreakers();

    registeredCircuitBreakers.clear();
  }

  private static class MockCircuitBreaker extends MemoryCircuitBreaker {

    public MockCircuitBreaker(CircuitBreakerConfig config) {
      super(config);
    }

    @Override
    public boolean isTripped() {
      // Always return true
      return true;
    }

    @Override
    public String getDebugInfo() {
      return "MockCircuitBreaker";
    }
  }

  private static class FakeMemoryPressureCircuitBreaker extends MemoryCircuitBreaker {

    public FakeMemoryPressureCircuitBreaker(CircuitBreakerConfig config) {
      super(config);
    }

    @Override
    protected long calculateLiveMemoryUsage() {
      // Return a number large enough to trigger a pushback from the circuit breaker
      return Long.MAX_VALUE;
    }
  }

  private class BuildingUpMemoryPressureCircuitBreaker extends MemoryCircuitBreaker {
    private AtomicInteger count;

    public BuildingUpMemoryPressureCircuitBreaker(CircuitBreakerConfig config) {
      super(config);

      this.count = new AtomicInteger(0);
    }

    @Override
    protected long calculateLiveMemoryUsage() {
      if (count.getAndIncrement() >= 4) {
        return Long.MAX_VALUE;
      }

      return 5; // Random number guaranteed to not trip the circuit breaker
    }
  }

  private static class FakeCPUCircuitBreaker extends CPUCircuitBreaker {
    public FakeCPUCircuitBreaker(CircuitBreakerConfig config) {
      super(config);
    }

    @Override
    protected double calculateLiveCPUUsage() {
      return 92; // Return a value large enough to trigger the circuit breaker
    }
  }
}
