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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerType;
import org.apache.solr.util.circuitbreaker.MemoryCircuitBreaker;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestCircuitBreaker extends SolrTestCaseJ4 {
  private final static int NUM_DOCS = 20;

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

      //commit inside the loop to get multiple segments to make search as realistic as possible
      assertU(commit());
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("filterCache.enabled");
    System.clearProperty("queryResultCache.enabled");
    System.clearProperty("documentCache.enabled");
  }

  public void testCBAlwaysTrips() throws IOException {
    HashMap<String, String> args = new HashMap<String, String>();

    args.put(QueryParsing.DEFTYPE, CircuitBreaker.NAME);
    args.put(CommonParams.FL, "id");

    CircuitBreaker circuitBreaker = new MockCircuitBreaker(h.getCore());

    h.getCore().getCircuitBreakerManager().registerCircuitBreaker(CircuitBreakerType.MEMORY, circuitBreaker);

    expectThrows(SolrException.class, () -> {
      h.query(req("name:\"john smith\""));
    });
  }

  public void testCBFakeMemoryPressure() throws IOException {
    HashMap<String, String> args = new HashMap<String, String>();

    args.put(QueryParsing.DEFTYPE, CircuitBreaker.NAME);
    args.put(CommonParams.FL, "id");

    CircuitBreaker circuitBreaker = new FakeMemoryPressureCircuitBreaker(h.getCore());

    h.getCore().getCircuitBreakerManager().registerCircuitBreaker(CircuitBreakerType.MEMORY, circuitBreaker);

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
      CircuitBreaker circuitBreaker = new BuildingUpMemoryPressureCircuitBreaker(h.getCore());

      h.getCore().getCircuitBreakerManager().registerCircuitBreaker(CircuitBreakerType.MEMORY, circuitBreaker);

      for (int i = 0; i < 5; i++) {
        executor.submit(() -> {
          try {
            h.query(req("name:\"john smith\""));
          } catch (SolrException e) {
            failureCount.incrementAndGet();
          } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
          }
        });
      }

      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e.getMessage());
      }

      assertEquals("Number of failed queries is not correct", 1, failureCount.get());
    } finally {
      if (!executor.isShutdown()) {
        executor.shutdown();
      }
    }
  }

  private class MockCircuitBreaker extends CircuitBreaker {

    public MockCircuitBreaker(SolrCore solrCore) {
      super(solrCore);
    }

    @Override
    public boolean isCircuitBreakerGauntletTripped() {
      // Always return true
      return true;
    }

    @Override
    public String printDebugInfo() {
      return "MockCircuitBreaker";
    }
  }

  private class FakeMemoryPressureCircuitBreaker extends MemoryCircuitBreaker {

    public FakeMemoryPressureCircuitBreaker(SolrCore solrCore) {
      super(solrCore);
    }

    @Override
    protected long calculateLiveMemoryUsage() {
      // Return a number large enough to trigger a pushback from the circuit breaker
      return Long.MAX_VALUE;
    }
  }

  private class BuildingUpMemoryPressureCircuitBreaker extends MemoryCircuitBreaker {
    private AtomicInteger count = new AtomicInteger();

    public BuildingUpMemoryPressureCircuitBreaker(SolrCore solrCore) {
      super(solrCore);
    }

    @Override
    protected long calculateLiveMemoryUsage() {
      if (count.getAndIncrement() >= 4) {
        return Long.MAX_VALUE;
      }

      return 5; // Random number guaranteed to not trip the circuit breaker
    }
  }
}
