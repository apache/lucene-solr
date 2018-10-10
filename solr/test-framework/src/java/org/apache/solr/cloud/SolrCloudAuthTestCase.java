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

public class SolrCloudAuthTestCase extends SolrCloudTestCase {
  private static final List<String> AUTH_METRICS_KEYS = Arrays.asList("errors", "requests", "authenticated", 
      "passThrough", "failWrongCredentials", "failMissingCredentials", "failInvalidCredentials", "requestTimes", "totalTime");
  private static final List<String> AUTH_METRICS_METER_KEYS = Arrays.asList("errors");
  private static final List<String> AUTH_METRICS_TIMER_KEYS = Collections.singletonList("requestTimes");

  /**
   * Used to check metric counts for PKI auth
   */
  protected static void assertPkiAuthMetrics(int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int failInvalidCredentials, int errors) {
    assertAuthMetrics("SECURITY./authentication/pki.", requests, authenticated, passThrough, failWrongCredentials, failMissingCredentials, failInvalidCredentials, errors);
  }
  
  /**
   * Used to check metric counts for the AuthPlugin in use (except PKI)
   */
  protected static void assertAuthMetrics(int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int failInvalidCredentials, int errors) {
    assertAuthMetrics("SECURITY./authentication.", requests, authenticated, passThrough, failWrongCredentials, failMissingCredentials, failInvalidCredentials, errors);
  }  
  
  /**
   * Common test method to be able to check security from any authentication plugin
   * @param prefix the metrics key prefix, currently "SECURITY./authentication." for basic auth and "SECURITY./authentication/pki." for PKI 
   */
  private static void assertAuthMetrics(String prefix, int requests, int authenticated, int passThrough, int failWrongCredentials, int failMissingCredentials, int failInvalidCredentials, int errors) {
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
    assertEquals("Metrics were: " + counts, requests, counts.get("requests").intValue());
    assertEquals("Metrics were: " + counts, authenticated, counts.get("authenticated").intValue());
    assertEquals("Metrics were: " + counts, passThrough, counts.get("passThrough").intValue());
    assertEquals("Metrics were: " + counts, failWrongCredentials, counts.get("failWrongCredentials").intValue());
    assertEquals("Metrics were: " + counts, failMissingCredentials, counts.get("failMissingCredentials").intValue());
    assertEquals("Metrics were: " + counts, failInvalidCredentials, counts.get("failInvalidCredentials").intValue());
    assertEquals("Metrics were: " + counts, errors, counts.get("errors").intValue());
    if (counts.get("requests") > 0) {
      assertTrue("Metrics were: " + counts, counts.get("requestTimes") > 1);
      assertTrue("Metrics were: " + counts, counts.get("totalTime") > 0);
    }
  }
 
  // Have to sum the metrics from all three shards/nodes
  static long sumCount(String prefix, String key, List<Map<String, Metric>> metrics) {
    assertTrue("Metric " + prefix + key + " does not exist", metrics.get(0).containsKey(prefix + key)); 
    if (AUTH_METRICS_METER_KEYS.contains(key))
      return metrics.stream().mapToLong(l -> ((Meter)l.get(prefix + key)).getCount()).sum();
    else if (AUTH_METRICS_TIMER_KEYS.contains(key))
      return (long) ((long) 1000 * metrics.stream().mapToDouble(l -> ((Timer)l.get(prefix + key)).getMeanRate()).average().orElse(0.0d));
    else
      return metrics.stream().mapToLong(l -> ((Counter)l.get(prefix + key)).getCount()).sum();
  }
}
