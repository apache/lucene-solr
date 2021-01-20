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
package org.apache.solr.core;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.metrics.reporters.jmx.JmxMetricsReporter;
import org.apache.solr.metrics.reporters.jmx.JmxObjectNameFactory;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Test for JMX Integration
 *
 *
 * @since solr 1.3
 */
public class TestJmxIntegration extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static MBeanServer mbeanServer = null;
  private static MBeanServer newMbeanServer = null;
  private static JmxObjectNameFactory nameFactory = null;
  private static String registryName = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make sure that at least one MBeanServer is available
    // prior to initializing the core
    //
    // (test configs are setup to use existing server if any, 
    // otherwise skip JMX)
    newMbeanServer = MBeanServerFactory.createMBeanServer();

    initCore("solrconfig.xml", "schema.xml");

    // we should be able to see that the core has JmxIntegration enabled
    registryName = h.getCore().getCoreMetricManager().getRegistryName();
    SolrMetricManager manager = h.getCoreContainer().getMetricManager();
    Map<String,SolrMetricReporter> reporters = manager.getReporters(registryName);
    assertEquals(1, reporters.size());
    SolrMetricReporter reporter = reporters.values().iterator().next();
    assertTrue(reporter instanceof SolrJmxReporter);
    SolrJmxReporter jmx = (SolrJmxReporter)reporter;
    assertTrue("JMX not enabled", jmx.isActive());
    // and we should be able to see that the reporter
    // refers to the JMX server we started

    mbeanServer = jmx.getMBeanServer();

    assertNotNull("No JMX server found in the reporter",
        mbeanServer);

    // NOTE: we can't guarantee that "mbeanServer == platformServer"
    // the JVM may have multiple MBean servers running when the test started
    // and the contract of not specifying one when configuring solr.xml without
    // agetnId or serviceUrl is that it will use whatever the "first" MBean server
    // returned by the JVM is.

    nameFactory = new JmxObjectNameFactory("default", registryName);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (newMbeanServer != null) {
      MBeanServerFactory.releaseMBeanServer(newMbeanServer);
    }
    mbeanServer = null;
    newMbeanServer = null;
  }

  @Before
  public void resetIndex() throws Exception {
    clearIndex();
    assertU("commit", commit());
  }

  @Test
  public void testJmxRegistration() throws Exception {
    assertTrue("No MBeans found in server", mbeanServer.getMBeanCount() > 0);

    Set<ObjectInstance> objects = mbeanServer.queryMBeans(null, null);
    assertFalse("No objects found in mbean server", objects
        .isEmpty());
    int numDynamicMbeans = 0;
    for (ObjectInstance o : objects) {
      ObjectName name = o.getObjectName();
      assertNotNull("Null name on: " + o.toString(), name);
      MBeanInfo mbeanInfo = mbeanServer.getMBeanInfo(name);
      if (name.getDomain().equals("solr")) {
        numDynamicMbeans++;
        MBeanAttributeInfo[] attrs = mbeanInfo.getAttributes();
        if (name.getKeyProperty("name").equals("fetcher")) { // no attributes without active replication
          continue;
        }
        assertTrue("No Attributes found for mbean: " + o.getObjectName() + ", " + mbeanInfo,
            0 < attrs.length);
        for (MBeanAttributeInfo attr : attrs) {
          // ensure every advertised attribute is gettable
          try {
            Object trash = mbeanServer.getAttribute(o.getObjectName(), attr.getName());
          } catch (javax.management.AttributeNotFoundException e) {
            throw new RuntimeException("Unable to featch attribute for " + o.getObjectName()
                + ": " + attr.getName(), e);
          }
        }
      }
    }
    assertTrue("No MBeans found", 0 < numDynamicMbeans);
  }

  @Test
  public void testJmxUpdate() throws Exception {

    SolrInfoBean bean = null;
    // wait until searcher is registered
    for (int i=0; i<100; i++) {
      bean = h.getCore().getInfoRegistry().get("searcher");
      if (bean != null) break;
      Thread.sleep(250);
    }
    if (bean==null) throw new RuntimeException("searcher was never registered");
    ObjectName searcher = nameFactory.createName("gauge", registryName, "SEARCHER.searcher.*");

    if (log.isInfoEnabled()) {
      log.info("Mbeans in server: {}", mbeanServer.queryNames(null, null));
    }

    Set<ObjectInstance> objects = mbeanServer.queryMBeans(searcher, null);
    assertFalse("No mbean found for SolrIndexSearcher", mbeanServer.queryMBeans(searcher, null).isEmpty());

    ObjectName name = nameFactory.createName("gauge", registryName, "SEARCHER.searcher.numDocs");
    int oldNumDocs =  (Integer)mbeanServer.getAttribute(name, "Value");
    assertU(adoc("id", "1"));
    assertU("commit", commit());
    int numDocs = (Integer)mbeanServer.getAttribute(name, "Value");
    assertTrue("New numDocs is same as old numDocs as reported by JMX",
        numDocs > oldNumDocs);
  }

  @Test
  @SuppressWarnings({"try"})
  public void testJmxOnCoreReload() throws Exception {
    // make sure searcher beans are registered
    assertQ(req("q", "*:*"), "//result[@numFound='0']");

    SolrMetricManager mgr = h.getCoreContainer().getMetricManager();
    String registryName = h.getCore().getCoreMetricManager().getRegistryName();
    String coreName = h.getCore().getName();
    String coreHashCode = Integer.toHexString(h.getCore().hashCode());
    Map<String, SolrMetricReporter> reporters = mgr.getReporters(registryName);
    // take first JMX reporter
    SolrJmxReporter reporter = null;
    for (Map.Entry<String, SolrMetricReporter> e : reporters.entrySet()) {
      if (e.getKey().endsWith(coreHashCode) && e.getValue() instanceof SolrJmxReporter) {
        reporter = (SolrJmxReporter)e.getValue();
        break;
      }
    }
    assertNotNull("could not find JMX reporter for " + registryName, reporter);
    String tag = reporter.getInstanceTag();

    Set<ObjectInstance> oldBeans = mbeanServer.queryMBeans(null, null);
    int oldNumberOfObjects = 0;
    for (ObjectInstance bean : oldBeans) {
      try {
        if (tag.equals(mbeanServer.getAttribute(bean.getObjectName(), JmxMetricsReporter.INSTANCE_TAG))) {
          oldNumberOfObjects++;
        }
      } catch (AttributeNotFoundException e) {
        // expected
      }
    }

    int totalCoreMetrics = mgr.registry(registryName).getMetrics().size();
    log.info("Before Reload: size of all core metrics: {} MBeans: {}", totalCoreMetrics, oldNumberOfObjects);
    assertEquals("Number of registered MBeans is not the same as the number of core metrics", totalCoreMetrics, oldNumberOfObjects);
    h.getCoreContainer().reload(coreName);
    assertQ(req("q", "*:*"), "//result[@numFound='0']");

    reporters = mgr.getReporters(registryName);
    coreHashCode = Integer.toHexString(h.getCore().hashCode());
    // take first JMX reporter
    reporter = null;
    for (Map.Entry<String, SolrMetricReporter> e : reporters.entrySet()) {
      if (e.getKey().endsWith(coreHashCode) && e.getValue() instanceof SolrJmxReporter) {
        reporter = (SolrJmxReporter)e.getValue();
        break;
      }
    }
    assertNotNull("could not find JMX reporter for " + registryName, reporter);
    tag = reporter.getInstanceTag();

    Set<ObjectInstance> newBeans = mbeanServer.queryMBeans(null, null);
    int newNumberOfObjects = 0;
    Set<String> metricNames = new TreeSet<>();
    Set<String> beanNames = new TreeSet<>();
    try (SolrCore core = h.getCoreContainer().getCore(coreName)) {
      MetricRegistry registry = mgr.registry(registryName);
      metricNames.addAll(registry.getNames());
      totalCoreMetrics = registry.getMetrics().size();
      for (ObjectInstance bean : newBeans) {
        try {
          if (tag.equals(mbeanServer.getAttribute(bean.getObjectName(), JmxMetricsReporter.INSTANCE_TAG))) {
            String[] name = bean.getObjectName().toString().substring(32).split(",");
            StringBuilder sb = new StringBuilder();
            for (String n : name) {
              if (sb.length() > 0) {
                sb.append(".");
              }
              sb.append(n.split("=")[1]);
            }
            beanNames.add(sb.toString());
            newNumberOfObjects++;
          }
        } catch (AttributeNotFoundException e) {
          // expected
        }
      }
    }

    log.info("After Reload: size of all core metrics: {} MBeans: {}", totalCoreMetrics, newNumberOfObjects);
    if (totalCoreMetrics != newNumberOfObjects) {
      Set<String> errors = new TreeSet<>(beanNames);
      errors.removeAll(metricNames);
      log.error("Unexpected bean names: {}", errors);
      errors = new TreeSet<>(metricNames);
      errors.removeAll(beanNames);
      log.error("Unexpected metric names: {}", errors);
      fail("Number of registered MBeans is not the same as the number of core metrics: " + totalCoreMetrics + " != " + newNumberOfObjects);
    }
  }
}
