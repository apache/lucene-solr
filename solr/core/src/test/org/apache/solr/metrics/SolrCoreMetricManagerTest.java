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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.apache.solr.schema.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrCoreMetricManagerTest extends SolrTestCaseJ4 {
  private static final int MAX_ITERATIONS = 100;

  private SolrCoreMetricManager metricManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");
  }

  @Before
  public void beforeTest() {
    metricManager = new SolrCoreMetricManager(h.getCore());
  }

  @After
  public void afterTest() throws IOException {
    metricManager.close();
    assertTrue(metricManager.getReporters().isEmpty());
  }

  @Test
  public void testRegisterMetrics() {
    Random random = random();

    String scope = SolrMetricTestUtils.getRandomScope(random);
    SolrInfoMBean.Category category = SolrMetricTestUtils.getRandomCategory(random);
    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(random);
    SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(category, scope, metrics);
    try {
      metricManager.registerMetricProducer(scope, producer);
      assertNotNull(scope);
      assertNotNull(category);
      assertNotNull(metrics);
      assertRegistered(scope, metrics, metricManager);
    } catch (final IllegalArgumentException e) {
      assertTrue("expected at least one null but got: scope="+scope+" category="+category+" metrics="+metrics,
          (scope == null || category == null || metrics == null));
      assertRegistered(scope, new HashMap<>(), metricManager);
    }
  }

  @Test
  public void testRegisterMetricsWithReplacements() {
    Random random = random();

    Map<String, Counter> registered = new HashMap<>();
    String scope = SolrMetricTestUtils.getRandomScope(random, true);
    SolrInfoMBean.Category category = SolrMetricTestUtils.getRandomCategory(random, true);

    int iterations = TestUtil.nextInt(random, 0, MAX_ITERATIONS);
    for (int i = 0; i < iterations; ++i) {
      Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetricsWithReplacements(random, registered);
      if (metrics.isEmpty()) {
        continue;
      }
      SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(category, scope, metrics);
      metricManager.registerMetricProducer(scope, producer);
      registered.putAll(metrics);
      assertRegistered(scope, registered, metricManager);
    }
  }

  @Test
  public void testLoadReporter() throws Exception {
    Random random = random();

    String className = SolrJmxReporter.class.getName();
    String reporterName = TestUtil.randomUnicodeString(random);

    Map<String, Object> attrs = new HashMap<>();
    attrs.put(FieldType.CLASS_NAME, className);
    attrs.put(CoreAdminParams.NAME, reporterName);

    boolean shouldDefinePlugin = random.nextBoolean();
    PluginInfo pluginInfo = shouldDefinePlugin ? new PluginInfo(TestUtil.randomUnicodeString(random), attrs) : null;

    try {
      metricManager.loadReporter(pluginInfo);
      assertNotNull(pluginInfo);
      assertEquals(1, metricManager.getReporters().size());
      assertNotNull(metricManager.getReporters().get(reporterName));
      assertTrue(metricManager.getReporters().get(reporterName) instanceof SolrJmxReporter);
    } catch (IllegalArgumentException e) {
      assertNull(pluginInfo);
      assertTrue(metricManager.getReporters().isEmpty());
    }
  }

  private static void assertRegistered(String scope, Map<String, Counter> newMetrics, SolrCoreMetricManager metricManager) {
    if (scope == null) {
      return;
    }
    String filter = "." + scope + ".";
    MetricRegistry registry = SolrMetricManager.registryFor(metricManager.getRegistryName());
    assertEquals(newMetrics.size(), registry.getMetrics().
        keySet().stream().filter(s -> s.contains(filter)).count());

    Map<String, Metric> registeredMetrics = registry.getMetrics().
        entrySet().stream().filter(e -> e.getKey() != null && e.getKey().contains(filter)).
        collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    for (Map.Entry<String, Metric> entry : registeredMetrics.entrySet()) {
      String name = entry.getKey();
      Metric expectedMetric = entry.getValue();

      Metric actualMetric = registry.getMetrics().get(name);

      assertNotNull(actualMetric);
      assertEquals(expectedMetric, actualMetric);
    }
  }
}
