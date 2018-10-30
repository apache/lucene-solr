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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for cloud tests wanting to track authentication metrics
 */
public class SolrCloudAuthTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final List<String> AUTH_METRICS_KEYS = Arrays.asList("errors", "requests", "authenticated", 
      "passThrough", "failWrongCredentials", "failMissingCredentials", "requestTimes", "totalTime");
  private static final List<String> AUTH_METRICS_METER_KEYS = Arrays.asList("errors");
  private static final List<String> AUTH_METRICS_TIMER_KEYS = Collections.singletonList("requestTimes");
  private static final String METRICS_PREFIX_PKI = "SECURITY./authentication/pki.";
  private static final String METRICS_PREFIX = "SECURITY./authentication.";
  // Use ThreadLocal for baseline to accommodate for beasting. 
  // Outer map is prefix, i.e. either authentication or pki, inner map is the metric itself
  private ThreadLocal<Map<String,Map<String,Long>>> baselineCounts = new ThreadLocal<>();
  
  @Before
  public void before() {
    setMetricsBaseline();
  }

  /**
   * The baseline metrics count is automatically set in @Before. Call this method to reset counts baseline inside a single test
   */
  protected void setMetricsBaseline() {
    Map<String,Map<String,Long>> map = new HashMap<>();
    map.put(METRICS_PREFIX, getBaselineCounts(METRICS_PREFIX));
    map.put(METRICS_PREFIX_PKI, getBaselineCounts(METRICS_PREFIX_PKI));
    baselineCounts.set(map);
    log.info("Set baseline {} in thread {}", baselineCounts.get(), Thread.currentThread().getName());
  }

  /**
   * Used to check metric counts for PKI auth
   */
  protected void assertPkiAuthMetrics(int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int errors) {
    assertAuthMetrics(METRICS_PREFIX_PKI, requests, authenticated, passThrough, failWrongCredentials, failMissingCredentials, errors);
  }
  
  /**
   * Used to check metric counts for the AuthPlugin in use (except PKI)
   */
  protected void assertAuthMetrics(int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int errors) {
    assertAuthMetrics(METRICS_PREFIX, requests, authenticated, passThrough, failWrongCredentials, failMissingCredentials, errors);
  }  
  
  /**
   * Common test method to be able to check security from any authentication plugin
   * @param prefix the metrics key prefix, currently "SECURITY./authentication." for basic auth and "SECURITY./authentication/pki." for PKI 
   */
  private void assertAuthMetrics(String prefix, int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int errors) {
    List<Map<String, Metric>> metrics = new ArrayList<>();
    cluster.getJettySolrRunners().forEach(r -> {
      MetricRegistry registry = r.getCoreContainer().getMetricManager().registry("solr.node");
      assertNotNull(registry);
      metrics.add(registry.getMetrics());
    });

    Map<String,Long> counts = new HashMap<>();
    AUTH_METRICS_KEYS.forEach(k -> {
      counts.put(k, sumCount(prefix, k, metrics));
    });
    
    // check each counter
    assertExpectedMetrics(prefix, requests, "requests", counts);
    assertExpectedMetrics(prefix, authenticated, "authenticated", counts);
    assertExpectedMetrics(prefix, passThrough, "passThrough", counts);
    assertExpectedMetrics(prefix, failWrongCredentials, "failWrongCredentials", counts);
    assertExpectedMetrics(prefix, failMissingCredentials, "failMissingCredentials", counts);
    assertExpectedMetrics(prefix, errors, "errors", counts);
    if (counts.get("requests") > 0) {
      assertTrue("requestTimes count not > 1", counts.get("requestTimes") > 1);
      assertTrue("totalTime not > 0", counts.get("totalTime") > 0);
    }
  }

  // Check that the actual metric is equal to or greater than the expected value, never less
  private void assertExpectedMetrics(String prefix, int expected, String key, Map<String, Long> counts) {
    long cnt = counts.get(key);
    long bln = baselineCounts.get().get(prefix).get(key);
    log.info("Got baseline from thread {}", Thread.currentThread().getName());
    long got = (cnt - bln);
    if (got != expected)
      log.warn("Expected count {} for metric {} but got {} (baseline={})", expected, key, got, bln);
    assertTrue("Expected " + key + " metric count to be " + expected + " or higher, but got " + got + " (" + cnt + " - baseline " + bln + ")", 
        got >= expected);
  }

  // Have to sum the metrics from all three shards/nodes
  private long sumCount(String prefix, String key, List<Map<String, Metric>> metrics) {
    assertTrue("Metric " + prefix + key + " does not exist", metrics.get(0).containsKey(prefix + key)); 
    if (AUTH_METRICS_METER_KEYS.contains(key))
      return metrics.stream().mapToLong(l -> ((Meter)l.get(prefix + key)).getCount()).sum();
    else if (AUTH_METRICS_TIMER_KEYS.contains(key))
      return (long) ((long) 1000 * metrics.stream().mapToDouble(l -> ((Timer)l.get(prefix + key)).getMeanRate()).average().orElse(0.0d));
    else
      return metrics.stream().mapToLong(l -> ((Counter)l.get(prefix + key)).getCount()).sum();
  }
  
  // Record the baseline before test starts in case it is > 0. Should allow for running same test multiple times without re-init
  private Map<String,Long> getBaselineCounts(String prefix) {
    List<Map<String, Metric>> metrics = new ArrayList<>();
    cluster.getJettySolrRunners().forEach(r -> {
      MetricRegistry registry = r.getCoreContainer().getMetricManager().registry("solr.node");
      assertNotNull(registry);
      metrics.add(registry.getMetrics());
    });

    Map<String,Long> counts = new HashMap<>();
    AUTH_METRICS_KEYS.forEach(k -> {
      if (!metrics.get(0).containsKey(prefix + k)) {
        counts.put(k, 0L);
      } else {
        counts.put(k, sumCount(prefix, k, metrics));
      }
    });
    return counts;
  }
}
