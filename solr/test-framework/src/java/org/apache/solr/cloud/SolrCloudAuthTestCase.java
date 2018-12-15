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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for cloud tests wanting to track authentication metrics.
 * The assertions provided by this base class require a *minimum* count, not exact count from metrics.
 * Warning: Make sure that your test case does not break when beasting. 
 */
public class SolrCloudAuthTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final List<String> AUTH_METRICS_KEYS = Arrays.asList("errors", "requests", "authenticated", 
      "passThrough", "failWrongCredentials", "failMissingCredentials", "requestTimes", "totalTime");
  private static final List<String> AUTH_METRICS_METER_KEYS = Arrays.asList("errors");
  private static final List<String> AUTH_METRICS_TIMER_KEYS = Collections.singletonList("requestTimes");
  private static final String METRICS_PREFIX_PKI = "SECURITY./authentication/pki.";
  private static final String METRICS_PREFIX = "SECURITY./authentication.";
  
  /**
   * Used to check metric counts for PKI auth
   */
  protected void assertPkiAuthMetricsMinimums(int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int errors) {
    assertAuthMetricsMinimums(METRICS_PREFIX_PKI, requests, authenticated, passThrough, failWrongCredentials, failMissingCredentials, errors);
  }
  
  /**
   * Used to check metric counts for the AuthPlugin in use (except PKI)
   * 
   * TODO: many of these params have to be under specified - this should wait a bit to see the desired params and timeout
   */
  protected void assertAuthMetricsMinimums(int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int errors) {
    assertAuthMetricsMinimums(METRICS_PREFIX, requests, authenticated, passThrough, failWrongCredentials, failMissingCredentials, errors);
  }

  /**
   * Common test method to be able to check security from any authentication plugin
   * @param prefix the metrics key prefix, currently "SECURITY./authentication." for basic auth and "SECURITY./authentication/pki." for PKI 
   */
  Map<String,Long> countAuthMetrics(String prefix) {
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
    return counts;
  } 
  
  /**
   * Common test method to be able to check security from any authentication plugin
   * @param prefix the metrics key prefix, currently "SECURITY./authentication." for basic auth and "SECURITY./authentication/pki." for PKI 
   */
  private void assertAuthMetricsMinimums(String prefix, int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int errors) {
    Map<String, Long> counts = countAuthMetrics(prefix);
    
    // check each counter
    boolean success = isMetricEuqalOrLarger(requests, "requests", counts)
        & isMetricEuqalOrLarger(authenticated, "authenticated", counts)
        & isMetricEuqalOrLarger(passThrough, "passThrough", counts)
        & isMetricEuqalOrLarger(failWrongCredentials, "failWrongCredentials", counts)
        & isMetricEuqalOrLarger(failMissingCredentials, "failMissingCredentials", counts)
        & isMetricEuqalOrLarger(errors, "errors", counts);
    
    Map<String, Long> expectedCounts = new HashMap<>();
    expectedCounts.put("requests", (long) requests);
    expectedCounts.put("authenticated", (long) authenticated);
    expectedCounts.put("passThrough", (long) passThrough);
    expectedCounts.put("failWrongCredentials", (long) failWrongCredentials);
    expectedCounts.put("failMissingCredentials", (long) failMissingCredentials);
    expectedCounts.put("errors", (long) errors);
    assertTrue("Expected metric minimums for prefix " + prefix + ": " + expectedCounts + ", but got: " + counts, success);
    
    if (counts.get("requests") > 0) {
      assertTrue("requestTimes count not > 1", counts.get("requestTimes") > 1);
      assertTrue("totalTime not > 0", counts.get("totalTime") > 0);
    }
  }

  // Check that the actual metric is equal to or greater than the expected value, never less
  private boolean isMetricEuqalOrLarger(int expected, String key, Map<String, Long> counts) {
    long cnt = counts.get(key);
    log.debug("Asserting that auth metrics count ({}) > expected ({})", cnt, expected);
    return(cnt >= expected);
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
}
