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

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.UniformReservoir;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 *
 */
public class MetricsConfigTest extends SolrTestCaseJ4 {
  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  // tmp dir, cleaned up automatically.
  private static File solrHome = null;

  @BeforeClass
  public static void setupLoader() throws Exception {
    solrHome = createTempDir().toFile();
  }

  @AfterClass
  public static void cleanupLoader() throws Exception {
    solrHome = null;
  }

  @Test
  public void testDefaults() throws Exception {
    NodeConfig cfg = loadNodeConfig("solr-metricsconfig.xml");
    SolrMetricManager mgr = new SolrMetricManager(cfg.getSolrResourceLoader(), cfg.getMetricsConfig());
    assertTrue(mgr.getCounterSupplier() instanceof MetricSuppliers.DefaultCounterSupplier);
    assertTrue(mgr.getMeterSupplier() instanceof MetricSuppliers.DefaultMeterSupplier);
    assertTrue(mgr.getTimerSupplier() instanceof MetricSuppliers.DefaultTimerSupplier);
    assertTrue(mgr.getHistogramSupplier() instanceof MetricSuppliers.DefaultHistogramSupplier);
    Clock clk = ((MetricSuppliers.DefaultTimerSupplier)mgr.getTimerSupplier()).clk;
    assertTrue(clk instanceof Clock.UserTimeClock);
    Reservoir rsv = ((MetricSuppliers.DefaultTimerSupplier)mgr.getTimerSupplier()).getReservoir();
    assertTrue(rsv instanceof ExponentiallyDecayingReservoir);
  }

  @Test
  public void testCustomReservoir() throws Exception {
    System.setProperty("timer.reservoir", UniformReservoir.class.getName());
    System.setProperty("histogram.size", "2048");
    System.setProperty("histogram.window", "600");
    System.setProperty("histogram.reservoir", SlidingTimeWindowReservoir.class.getName());
    NodeConfig cfg = loadNodeConfig("solr-metricsconfig.xml");
    SolrMetricManager mgr = new SolrMetricManager(cfg.getSolrResourceLoader(), cfg.getMetricsConfig());
    assertTrue(mgr.getCounterSupplier() instanceof MetricSuppliers.DefaultCounterSupplier);
    assertTrue(mgr.getMeterSupplier() instanceof MetricSuppliers.DefaultMeterSupplier);
    assertTrue(mgr.getTimerSupplier() instanceof MetricSuppliers.DefaultTimerSupplier);
    assertTrue(mgr.getHistogramSupplier() instanceof MetricSuppliers.DefaultHistogramSupplier);
    Reservoir rsv = ((MetricSuppliers.DefaultTimerSupplier)mgr.getTimerSupplier()).getReservoir();
    assertTrue(rsv instanceof UniformReservoir);
    rsv = ((MetricSuppliers.DefaultHistogramSupplier)mgr.getHistogramSupplier()).getReservoir();
    assertTrue(rsv instanceof SlidingTimeWindowReservoir);
  }

  @Test
  public void testCustomSupplier() throws Exception {
    System.setProperty("counter.class", MockCounterSupplier.class.getName());
    System.setProperty("meter.class", MockMeterSupplier.class.getName());
    System.setProperty("timer.class", MockTimerSupplier.class.getName());
    System.setProperty("histogram.class", MockHistogramSupplier.class.getName());
    NodeConfig cfg = loadNodeConfig("solr-metricsconfig.xml");
    SolrMetricManager mgr = new SolrMetricManager(cfg.getSolrResourceLoader(), cfg.getMetricsConfig());
    assertTrue(mgr.getCounterSupplier() instanceof MockCounterSupplier);
    assertTrue(mgr.getMeterSupplier() instanceof MockMeterSupplier);
    assertTrue(mgr.getTimerSupplier() instanceof MockTimerSupplier);
    assertTrue(mgr.getHistogramSupplier() instanceof MockHistogramSupplier);

    // assert setter-based configuration
    MockCounterSupplier mockCounterSupplier = ((MockCounterSupplier)mgr.getCounterSupplier());
    assertEquals("bar", mockCounterSupplier.foo);
    MockMeterSupplier mockMeterSupplier = ((MockMeterSupplier)mgr.getMeterSupplier());
    assertEquals("bar", mockMeterSupplier.foo);
    MockTimerSupplier mockTimerSupplier = ((MockTimerSupplier)mgr.getTimerSupplier());
    assertEquals(true, mockTimerSupplier.boolParam);
    assertEquals("strParam", mockTimerSupplier.strParam);
    assertEquals(-100, mockTimerSupplier.intParam);

    // assert PluginInfoInitialized-based configuration
    MockHistogramSupplier mockHistogramSupplier = ((MockHistogramSupplier)mgr.getHistogramSupplier());
    assertNotNull(mockHistogramSupplier.info);
  }

  @Test
  public void testDisabledMetrics() throws Exception {
    System.setProperty("metricsEnabled", "false");
    NodeConfig cfg = loadNodeConfig("solr-metricsconfig.xml");
    SolrMetricManager mgr = new SolrMetricManager(cfg.getSolrResourceLoader(), cfg.getMetricsConfig());
    assertTrue(mgr.getCounterSupplier() instanceof MetricSuppliers.NoOpCounterSupplier);
    assertTrue(mgr.getMeterSupplier() instanceof MetricSuppliers.NoOpMeterSupplier);
    assertTrue(mgr.getTimerSupplier() instanceof MetricSuppliers.NoOpTimerSupplier);
    assertTrue(mgr.getHistogramSupplier() instanceof MetricSuppliers.NoOpHistogramSupplier);

  }

  @Test
  public void testMissingValuesConfig() throws Exception {
    NodeConfig cfg = loadNodeConfig("solr-metricsconfig1.xml");
    SolrMetricManager mgr = new SolrMetricManager(cfg.getSolrResourceLoader(), cfg.getMetricsConfig());
    assertEquals("nullNumber", null, mgr.nullNumber());
    assertEquals("notANumber", -1, mgr.notANumber());
    assertEquals("nullNumber", "", mgr.nullString());
    assertTrue("nullObject", mgr.nullObject() instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) mgr.nullObject();
    assertEquals("missing", map.get("value"));
  }

  private NodeConfig loadNodeConfig(String config) throws Exception {
    InputStream is = MetricsConfigTest.class.getResourceAsStream("/solr/" + config);
    return SolrXmlConfig.fromInputStream(TEST_PATH(), is, new Properties()); //TODO pass in props
  }
}
