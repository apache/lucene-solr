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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.metrics.reporters.MockMetricReporter;
import org.apache.solr.util.JmxUtil;
import org.apache.solr.util.TestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrMetricsIntegrationTest extends SolrTestCaseJ4 {
  private static final int MAX_ITERATIONS = 20;
  private static final String CORE_NAME = "metrics_integration";
  private static final String METRIC_NAME = "requestTimes";
  private static final String HANDLER_NAME = "/select";
  private static final String[] REPORTER_NAMES = {"reporter1", "reporter2"};
  private static final String UNIVERSAL = "universal";
  private static final String SPECIFIC = "specific";
  private static final String MULTIGROUP = "multigroup";
  private static final String MULTIREGISTRY = "multiregistry";
  private static final String[] INITIAL_REPORTERS = {REPORTER_NAMES[0], REPORTER_NAMES[1], UNIVERSAL, SPECIFIC, MULTIGROUP, MULTIREGISTRY};
  private static final String[] RENAMED_REPORTERS = {REPORTER_NAMES[0], REPORTER_NAMES[1], UNIVERSAL, MULTIGROUP};
  private static final SolrInfoBean.Category HANDLER_CATEGORY = SolrInfoBean.Category.QUERY;

  private CoreContainer cc;
  private SolrMetricManager metricManager;
  private String tag;
  private int jmxReporter;

  private void assertTagged(Map<String, SolrMetricReporter> reporters, String name) {
    assertTrue("Reporter '" + name + "' missing in " + reporters, reporters.containsKey(name + "@" + tag));
  }

  @Before
  public void beforeTest() throws Exception {
    Path home = Paths.get(TEST_HOME());
    // define these properties, they are used in solrconfig.xml
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    String solrXml = FileUtils.readFileToString(Paths.get(home.toString(), "solr-metricreporter.xml").toFile(), "UTF-8");
    NodeConfig cfg = SolrXmlConfig.fromString(home, solrXml);
    cc = createCoreContainer(cfg, new TestHarness.TestCoresLocator
                             (DEFAULT_TEST_CORENAME, initAndGetDataDir().getAbsolutePath(),
                              "solrconfig.xml", "schema.xml"));
                             
    h.coreName = DEFAULT_TEST_CORENAME;
    jmxReporter = JmxUtil.findFirstMBeanServer() != null ? 1 : 0;
    metricManager = cc.getMetricManager();
    tag = h.getCore().getCoreMetricManager().getTag();
    // initially there are more reporters, because two of them are added via a matching collection name
    Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.core." + DEFAULT_TEST_CORENAME);
    assertEquals(INITIAL_REPORTERS.length + jmxReporter, reporters.size());
    for (String r : INITIAL_REPORTERS) {
      assertTagged(reporters, r);
    }
    // test rename operation
    cc.rename(DEFAULT_TEST_CORENAME, CORE_NAME);
    h.coreName = CORE_NAME;
    cfg = cc.getConfig();
    PluginInfo[] plugins = cfg.getMetricsConfig().getMetricReporters();
    assertNotNull(plugins);
    assertEquals(10 + jmxReporter, plugins.length);
    reporters = metricManager.getReporters("solr.node");
    assertEquals(4 + jmxReporter, reporters.size());
    assertTrue("Reporter '" + REPORTER_NAMES[0] + "' missing in solr.node", reporters.containsKey(REPORTER_NAMES[0]));
    assertTrue("Reporter '" + UNIVERSAL + "' missing in solr.node", reporters.containsKey(UNIVERSAL));
    assertTrue("Reporter '" + MULTIGROUP + "' missing in solr.node", reporters.containsKey(MULTIGROUP));
    assertTrue("Reporter '" + MULTIREGISTRY + "' missing in solr.node", reporters.containsKey(MULTIREGISTRY));
    SolrMetricReporter reporter = reporters.get(REPORTER_NAMES[0]);
    assertTrue("Reporter " + reporter + " is not an instance of " + MockMetricReporter.class.getName(),
        reporter instanceof  MockMetricReporter);
    reporter = reporters.get(UNIVERSAL);
    assertTrue("Reporter " + reporter + " is not an instance of " + MockMetricReporter.class.getName(),
        reporter instanceof  MockMetricReporter);
  }

  @After
  public void afterTest() throws Exception {
    if (null == metricManager) {
      return; // test failed to init, nothing to cleanup
    }
      
    SolrCoreMetricManager coreMetricManager = h.getCore().getCoreMetricManager();
    Map<String, SolrMetricReporter> reporters = metricManager.getReporters(coreMetricManager.getRegistryName());

    Gauge<?> gauge = (Gauge<?>) coreMetricManager.getRegistry().getMetrics().get("CORE.indexDir");
    assertNotNull(gauge.getValue());
    h.getCore().close();
    assertEquals(metricManager.nullString(), gauge.getValue());

    deleteCore();

    for (String reporterName : RENAMED_REPORTERS) {
      SolrMetricReporter reporter = reporters.get(reporterName + "@" + tag);
      MockMetricReporter mockReporter = (MockMetricReporter) reporter;
      assertTrue("Reporter " + reporterName + " was not closed: " + mockReporter, mockReporter.didClose);
    }
  }

  @Test
  public void testConfigureReporter() throws Exception {
    Random random = random();

    String metricName = SolrMetricManager.mkName(METRIC_NAME, HANDLER_CATEGORY.toString(), HANDLER_NAME);
    SolrCoreMetricManager coreMetricManager = h.getCore().getCoreMetricManager();
    Timer timer = metricManager.timer(null, coreMetricManager.getRegistryName(), metricName);

    long initialCount = timer.getCount();

    int iterations = TestUtil.nextInt(random, 0, MAX_ITERATIONS);
    for (int i = 0; i < iterations; ++i) {
      h.query(req("*"));
    }

    long finalCount = timer.getCount();
    assertEquals("metric counter incorrect", iterations, finalCount - initialCount);
    Map<String, SolrMetricReporter> reporters = metricManager.getReporters(coreMetricManager.getRegistryName());
    assertEquals(RENAMED_REPORTERS.length + jmxReporter, reporters.size());

    // SPECIFIC and MULTIREGISTRY were skipped because they were
    // specific to collection1
    for (String reporterName : RENAMED_REPORTERS) {
      SolrMetricReporter reporter = reporters.get(reporterName + "@" + tag);
      assertNotNull("Reporter " + reporterName + " was not found.", reporter);
      assertTrue(reporter instanceof MockMetricReporter);

      MockMetricReporter mockReporter = (MockMetricReporter) reporter;
      assertTrue("Reporter " + reporterName + " was not initialized: " + mockReporter, mockReporter.didInit);
      assertTrue("Reporter " + reporterName + " was not validated: " + mockReporter, mockReporter.didValidate);
      assertFalse("Reporter " + reporterName + " was incorrectly closed: " + mockReporter, mockReporter.didClose);
    }
  }

  @Test
  public void testCoreContainerMetrics() throws Exception {
    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    assertTrue(cc.getMetricManager().registryNames().toString(), cc.getMetricManager().registryNames().contains(registryName));
    MetricRegistry registry = cc.getMetricManager().registry(registryName);
    Map<String, Metric> metrics = registry.getMetrics();
    assertTrue(metrics.containsKey("CONTAINER.cores.loaded"));
    assertTrue(metrics.containsKey("CONTAINER.cores.lazy"));
    assertTrue(metrics.containsKey("CONTAINER.cores.unloaded"));
    assertTrue(metrics.containsKey("CONTAINER.fs.totalSpace"));
    assertTrue(metrics.containsKey("CONTAINER.fs.usableSpace"));
    assertTrue(metrics.containsKey("CONTAINER.fs.path"));
    assertTrue(metrics.containsKey("CONTAINER.fs.spins"));
    assertTrue(metrics.containsKey("CONTAINER.fs.coreRoot.totalSpace"));
    assertTrue(metrics.containsKey("CONTAINER.fs.coreRoot.usableSpace"));
    assertTrue(metrics.containsKey("CONTAINER.fs.coreRoot.path"));
    assertTrue(metrics.containsKey("CONTAINER.fs.coreRoot.spins"));
    assertTrue(metrics.containsKey("CONTAINER.version.specification"));
    assertTrue(metrics.containsKey("CONTAINER.version.implementation"));
    Gauge<?> g = (Gauge<?>)metrics.get("CONTAINER.fs.path");
    assertEquals(g.getValue(), cc.getSolrHome());
    boolean spins = IOUtils.spins(cc.getCoreRootDirectory());
    g = (Gauge<?>)metrics.get("CONTAINER.fs.coreRoot.spins");
    assertEquals(spins, g.getValue());
    g = (Gauge<?>)metrics.get("CONTAINER.fs.spins");
    if (cc.getConfig().getSolrDataHome() != null) {
      spins = IOUtils.spins(cc.getConfig().getSolrDataHome());
      assertEquals(spins, g.getValue());
    } else {
      assertEquals(spins, g.getValue());
    }
  }
}
