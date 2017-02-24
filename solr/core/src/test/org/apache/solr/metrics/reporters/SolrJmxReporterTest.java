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
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.metrics.SolrMetricTestUtils;
import org.apache.solr.schema.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrJmxReporterTest extends SolrTestCaseJ4 {

  private static final int MAX_ITERATIONS = 20;

  private String domain;

  private SolrCoreMetricManager coreMetricManager;
  private SolrMetricManager metricManager;
  private SolrJmxReporter reporter;
  private MBeanServer mBeanServer;
  private String reporterName;

  @Before
  public void beforeTest() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");

    final SolrCore core = h.getCore();
    domain = core.getName();

    coreMetricManager = core.getCoreMetricManager();
    metricManager = core.getCoreDescriptor().getCoreContainer().getMetricManager();
    PluginInfo pluginInfo = createReporterPluginInfo();
    metricManager.loadReporter(coreMetricManager.getRegistryName(), coreMetricManager.getCore().getResourceLoader(), pluginInfo);

    Map<String, SolrMetricReporter> reporters = metricManager.getReporters(coreMetricManager.getRegistryName());
    assertTrue("reporters.size should be > 0, but was + " + reporters.size(), reporters.size() > 0);
    reporterName = pluginInfo.name;
    assertNotNull("reporter " + reporterName + " not present among " + reporters, reporters.get(reporterName));
    assertTrue("wrong reporter class: " + reporters.get(reporterName), reporters.get(reporterName) instanceof SolrJmxReporter);

    reporter = (SolrJmxReporter) reporters.get(reporterName);
    mBeanServer = reporter.getMBeanServer();
    assertNotNull("MBean server not found.", mBeanServer);
  }

  private PluginInfo createReporterPluginInfo() {
    Random random = random();
    String className = SolrJmxReporter.class.getName();
    String reporterName = TestUtil.randomSimpleString(random, 1, 10);

    Map<String, Object> attrs = new HashMap<>();
    attrs.put(FieldType.CLASS_NAME, className);
    attrs.put(CoreAdminParams.NAME, reporterName);

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
    SolrInfoMBean.Category category = SolrMetricTestUtils.getRandomCategory(random, true);

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
                      reporterName.equals(o.getObjectName().getKeyProperty("reporter"))).count());
    }
  }

  @Test
  public void testReloadCore() throws Exception {
    Random random = random();

    String scope = SolrMetricTestUtils.getRandomScope(random, true);
    SolrInfoMBean.Category category = SolrMetricTestUtils.getRandomCategory(random, true);
    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(random, true);
    SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(metricManager, category, scope, metrics);
    coreMetricManager.registerMetricProducer(scope, producer);
    Set<ObjectInstance> objects = mBeanServer.queryMBeans(null, null);
    assertEquals(metrics.size(), objects.stream().
        filter(o -> scope.equals(o.getObjectName().getKeyProperty("scope")) &&
            reporterName.equals(o.getObjectName().getKeyProperty("reporter"))).count());

    h.getCoreContainer().reload(h.getCore().getName());
    PluginInfo pluginInfo = createReporterPluginInfo();
    metricManager.loadReporter(coreMetricManager.getRegistryName(), coreMetricManager.getCore().getResourceLoader(), pluginInfo);
    coreMetricManager.registerMetricProducer(scope, producer);

    objects = mBeanServer.queryMBeans(null, null);
    assertEquals(metrics.size(), objects.stream().
        filter(o -> scope.equals(o.getObjectName().getKeyProperty("scope")) &&
            pluginInfo.name.equals(o.getObjectName().getKeyProperty("reporter"))).count());
  }

}
