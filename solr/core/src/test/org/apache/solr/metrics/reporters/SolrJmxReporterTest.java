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
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistryListener;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricTestUtils;
import org.apache.solr.schema.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrJmxReporterTest extends SolrTestCaseJ4 {

  private static final int MAX_ITERATIONS = 20;

  private String domain;

  private SolrCoreMetricManager metricManager;
  private SolrJmxReporter reporter;
  private MBeanServer mBeanServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");
  }

  @Before
  public void beforeTest() throws Exception {
    // Ensure we have at least one MBeanServer available
    MBeanServer platformServer = ManagementFactory.getPlatformMBeanServer();
    // TODO: can we skip this call? either always or randomly?

    Random random = random();

    final SolrCore core = h.getCore();
    domain = core.getName();

    String className = SolrJmxReporter.class.getName();
    String reporterName = TestUtil.randomUnicodeString(random);

    Map<String, Object> attrs = new HashMap<>();
    attrs.put(FieldType.CLASS_NAME, className);
    attrs.put(CoreAdminParams.NAME, reporterName);

    boolean shouldOverrideDomain = random.nextBoolean();
    if (shouldOverrideDomain) {
      domain = TestUtil.randomSimpleString(random);
      attrs.put("domain", domain);
    }

    PluginInfo pluginInfo = new PluginInfo(TestUtil.randomUnicodeString(random), attrs);
    metricManager = new SolrCoreMetricManager(core);
    metricManager.loadReporter(pluginInfo);

    assertEquals(1, metricManager.getReporters().size());
    assertNotNull(metricManager.getReporters().get(reporterName));
    assertTrue(metricManager.getReporters().get(reporterName) instanceof SolrJmxReporter);

    reporter = (SolrJmxReporter) metricManager.getReporters().get(reporterName);
    mBeanServer = reporter.getMBeanServer();
    assertNotNull("MBean server not found.", mBeanServer);
  }

  @After
  public void afterTest() throws Exception {
    reporter.close();
    Set<ObjectInstance> objects =
        mBeanServer.queryMBeans(ObjectName.getInstance(domain + ":*"), null);
    assertTrue(objects.isEmpty());

    metricManager.close();
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
      SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(category, scope, metrics);
      metricManager.registerMetricProducer(scope, producer);
      registered.putAll(metrics);
      //waitForListener();
      Set<ObjectInstance> objects = mBeanServer.queryMBeans(null, null);
      assertEquals(registered.size(), objects.stream().
          filter(o -> o.getObjectName().getKeyProperty("scope") != null &&
              o.getObjectName().getKeyProperty("scope").equals(scope)).count());
    }
  }

  @Test
  public void testReloadCore() throws Exception {
    Random random = random();

    String scope = SolrMetricTestUtils.getRandomScope(random, true);
    SolrInfoMBean.Category category = SolrMetricTestUtils.getRandomCategory(random, true);
    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(random, true);
    SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(category, scope, metrics);
    metricManager.registerMetricProducer(scope, producer);
    //waitForListener();

    Set<ObjectInstance> objects = mBeanServer.queryMBeans(null, null);
    assertEquals(metrics.size(), objects.stream().
        filter(o -> o.getObjectName().getKeyProperty("scope") != null &&
            o.getObjectName().getKeyProperty("scope").equals(scope)).count());

    h.getCoreContainer().reload(h.getCore().getName());
    metricManager.registerMetricProducer(scope, producer);

    objects = mBeanServer.queryMBeans(null, null);
    assertEquals(metrics.size(), objects.stream().
        filter(o -> o.getObjectName().getKeyProperty("scope") != null &&
            o.getObjectName().getKeyProperty("scope").equals(scope)).count());
  }

}
