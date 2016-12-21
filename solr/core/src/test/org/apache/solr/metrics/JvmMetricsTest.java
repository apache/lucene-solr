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
package org.apache.solr.metrics;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.solr.SolrJettyTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test {@link OperatingSystemMetricSet} and proper JVM metrics registration.
 */
public class JvmMetricsTest extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(legacyExampleCollection1SolrHome());
  }

  @Test
  public void testOperatingSystemMetricsSet() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    OperatingSystemMetricSet set = new OperatingSystemMetricSet(mBeanServer);
    Map<String, Metric> metrics = set.getMetrics();
    assertTrue(metrics.size() > 0);
    for (String metric : OperatingSystemMetricSet.METRICS) {
      Gauge<?> gauge = (Gauge<?>)metrics.get(metric);
      if (gauge == null) { // some are optional depending on OS
        continue;
      }
      double value = ((Number)gauge.getValue()).doubleValue();
      assertTrue(value >= 0);
    }
  }

  @Test
  public void testSetupJvmMetrics() throws Exception {
    SolrMetricManager metricManager = jetty.getCoreContainer().getMetricManager();
    Map<String,Metric> metrics = metricManager.registry("solr.jvm").getMetrics();
    assertTrue(metrics.size() > 0);
    assertTrue(metrics.toString(), metrics.entrySet().stream().filter(e -> e.getKey().startsWith("buffers.")).count() > 0);
    assertTrue(metrics.toString(), metrics.entrySet().stream().filter(e -> e.getKey().startsWith("classes.")).count() > 0);
    assertTrue(metrics.toString(), metrics.entrySet().stream().filter(e -> e.getKey().startsWith("os.")).count() > 0);
    assertTrue(metrics.toString(), metrics.entrySet().stream().filter(e -> e.getKey().startsWith("gc.")).count() > 0);
    assertTrue(metrics.toString(), metrics.entrySet().stream().filter(e -> e.getKey().startsWith("memory.")).count() > 0);
    assertTrue(metrics.toString(), metrics.entrySet().stream().filter(e -> e.getKey().startsWith("threads.")).count() > 0);
  }
}
