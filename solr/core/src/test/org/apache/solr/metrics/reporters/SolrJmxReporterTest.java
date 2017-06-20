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
package org.apache.solr.metrics.reporters;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import java.rmi.registry.LocateRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.codahale.metrics.Counter;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.metrics.SolrMetricTestUtils;
import org.apache.solr.schema.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrJmxReporterTest extends SolrTestCaseJ4 {

  private static final int MAX_ITERATIONS = 20;

  private static int jmxPort;

  private String domain;

  private SolrCoreMetricManager coreMetricManager;
  private SolrMetricManager metricManager;
  private SolrJmxReporter reporter;
  private MBeanServer mBeanServer;
  private String reporterName;
  private String rootName;

  @BeforeClass
  public static void init() throws Exception {
    jmxPort = getNextAvailablePort();
    assertFalse(jmxPort == -1);
    LocateRegistry.createRegistry(jmxPort);
  }

  @Before
  public void beforeTest() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");

    final SolrCore core = h.getCore();
    domain = core.getName();
    rootName = TestUtil.randomSimpleString(random(), 5, 10);

    coreMetricManager = core.getCoreMetricManager();
    metricManager = core.getCoreContainer().getMetricManager();
    PluginInfo pluginInfo = createReporterPluginInfo(rootName, true);
    metricManager.loadReporter(coreMetricManager.getRegistryName(), coreMetricManager.getCore().getResourceLoader(),
        pluginInfo, coreMetricManager.getTag());

    Map<String, SolrMetricReporter> reporters = metricManager.getReporters(coreMetricManager.getRegistryName());
    assertTrue("reporters.size should be > 0, but was + " + reporters.size(), reporters.size() > 0);
    reporterName = pluginInfo.name;
    String taggedName = reporterName + "@" + coreMetricManager.getTag();
    assertNotNull("reporter " + taggedName + " not present among " + reporters, reporters.get(taggedName));
    assertTrue("wrong reporter class: " + reporters.get(taggedName), reporters.get(taggedName) instanceof SolrJmxReporter);

    reporter = (SolrJmxReporter) reporters.get(taggedName);
    mBeanServer = reporter.getMBeanServer();
    assertNotNull("MBean server not found.", mBeanServer);
  }

  private PluginInfo createReporterPluginInfo(String rootName, boolean enabled) {
    Random random = random();
    String className = SolrJmxReporter.class.getName();
    String reporterName = TestUtil.randomSimpleString(random, 5, 10);

    Map<String, Object> attrs = new HashMap<>();
    attrs.put(FieldType.CLASS_NAME, className);
    attrs.put(CoreAdminParams.NAME, reporterName);
    attrs.put("rootName", rootName);
    attrs.put("enabled", enabled);
    attrs.put("serviceUrl", "service:jmx:rmi:///jndi/rmi://localhost:" + jmxPort + "/solrjmx");

    boolean shouldOverrideDomain = random.nextBoolean();
    if (shouldOverrideDomain) {
      domain = TestUtil.randomSimpleString(random);
      attrs.put("domain", domain);
    }

    return new PluginInfo(TestUtil.randomUnicodeString(random), attrs);
  }

  @After
  public void afterTest() throws Exception {
    metricManager.closeReporters(coreMetricManager.getRegistryName());
    Set<ObjectInstance> objects =
        mBeanServer.queryMBeans(ObjectName.getInstance(domain + ":*"), null);
    assertTrue(objects.isEmpty());

    coreMetricManager.close();
    deleteCore();
  }

  @Test
  public void testReportMetrics() throws Exception {
    Random random = random();

    Map<String, Counter> registered = new HashMap<>();
    String scope = SolrMetricTestUtils.getRandomScope(random, true);
    SolrInfoBean.Category category = SolrMetricTestUtils.getRandomCategory(random, true);

    int iterations = TestUtil.nextInt(random, 0, MAX_ITERATIONS);
    for (int i = 0; i < iterations; ++i) {
      Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetricsWithReplacements(random, registered);
      SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(metricManager, category, scope, metrics);
      coreMetricManager.registerMetricProducer(scope, producer);
      registered.putAll(metrics);
      //waitForListener();
      Set<ObjectInstance> objects = mBeanServer.queryMBeans(null, null);
      assertEquals(registered.size(), objects.stream().
          filter(o -> scope.equals(o.getObjectName().getKeyProperty("scope")) &&
                      rootName.equals(o.getObjectName().getDomain())).count());
    }
  }

  @Test
  public void testReloadCore() throws Exception {
    Random random = random();

    String scope = SolrMetricTestUtils.getRandomScope(random, true);
    SolrInfoBean.Category category = SolrMetricTestUtils.getRandomCategory(random, true);
    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(random, true);
    SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(metricManager, category, scope, metrics);
    coreMetricManager.registerMetricProducer(scope, producer);
    Set<ObjectInstance> objects = mBeanServer.queryMBeans(null, null);
    assertEquals(metrics.size(), objects.stream().
        filter(o -> scope.equals(o.getObjectName().getKeyProperty("scope")) &&
        o.getObjectName().getDomain().equals(rootName)).count());

    h.getCoreContainer().reload(h.getCore().getName());
    PluginInfo pluginInfo = createReporterPluginInfo(rootName, true);
    metricManager.loadReporter(coreMetricManager.getRegistryName(), coreMetricManager.getCore().getResourceLoader(),
        pluginInfo, String.valueOf(coreMetricManager.getCore().hashCode()));
    coreMetricManager.registerMetricProducer(scope, producer);

    objects = mBeanServer.queryMBeans(null, null);
    assertEquals(metrics.size(), objects.stream().
        filter(o -> scope.equals(o.getObjectName().getKeyProperty("scope")) &&
            rootName.equals(o.getObjectName().getDomain())).count());
  }

  @Test
  public void testEnabled() throws Exception {
    String root1 = TestUtil.randomSimpleString(random(), 5, 10);
    PluginInfo pluginInfo1 = createReporterPluginInfo(root1, true);
    metricManager.loadReporter(coreMetricManager.getRegistryName(), coreMetricManager.getCore().getResourceLoader(),
        pluginInfo1, coreMetricManager.getTag());

    String root2 = TestUtil.randomSimpleString(random(), 5, 10);
    assertFalse(root2.equals(root1));
    PluginInfo pluginInfo2 = createReporterPluginInfo(root2, false);
    metricManager.loadReporter(coreMetricManager.getRegistryName(), coreMetricManager.getCore().getResourceLoader(),
        pluginInfo2, coreMetricManager.getTag());

    Map<String, SolrMetricReporter> reporters = metricManager.getReporters(coreMetricManager.getRegistryName());
    assertTrue(reporters.containsKey(pluginInfo1.name + "@" + coreMetricManager.getTag()));
    assertTrue(reporters.containsKey(pluginInfo2.name + "@" + coreMetricManager.getTag()));

    String scope = SolrMetricTestUtils.getRandomScope(random(), true);
    SolrInfoBean.Category category = SolrMetricTestUtils.getRandomCategory(random(), true);
    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(random(), true);
    SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(metricManager, category, scope, metrics);
    coreMetricManager.registerMetricProducer(scope, producer);
    Set<ObjectInstance> objects = mBeanServer.queryMBeans(null, null);
    assertEquals(metrics.size(), objects.stream().
        filter(o -> scope.equals(o.getObjectName().getKeyProperty("scope")) &&
            root1.equals(o.getObjectName().getDomain())).count());
    assertEquals(0, objects.stream().
        filter(o -> scope.equals(o.getObjectName().getKeyProperty("scope")) &&
            root2.equals(o.getObjectName().getDomain())).count());
  }

}
