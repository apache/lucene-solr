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

import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.metrics.reporters.jmx.JmxObjectNameFactory;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.AfterClass;
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

/**
 * Test for JMX Integration
 *
 *
 * @since solr 1.3
 */
public class TestJmxIntegration extends AbstractSolrTestCase {

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

    log.info("Mbeans in server: " + mbeanServer.queryNames(null, null));

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

  @Test @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-2715") // timing problem?
  public void testJmxOnCoreReload() throws Exception {

    String coreName = h.getCore().getName();

    Set<ObjectInstance> oldBeans = mbeanServer.queryMBeans(null, null);
    int oldNumberOfObjects = 0;
    for (ObjectInstance bean : oldBeans) {
      try {
        if (String.valueOf(h.getCore().hashCode()).equals(mbeanServer.getAttribute(bean.getObjectName(), "coreHashCode"))) {
          oldNumberOfObjects++;
        }
      } catch (AttributeNotFoundException e) {
        // expected
      }
    }

    log.info("Before Reload: Size of infoRegistry: " + h.getCore().getInfoRegistry().size() + " MBeans: " + oldNumberOfObjects);
    assertEquals("Number of registered MBeans is not the same as info registry size", h.getCore().getInfoRegistry().size(), oldNumberOfObjects);

    h.getCoreContainer().reload(coreName);

    Set<ObjectInstance> newBeans = mbeanServer.queryMBeans(null, null);
    int newNumberOfObjects = 0;
    int registrySize = 0;
    try (SolrCore core = h.getCoreContainer().getCore(coreName)) {
      registrySize = core.getInfoRegistry().size();
      for (ObjectInstance bean : newBeans) {
        try {
          if (String.valueOf(core.hashCode()).equals(mbeanServer.getAttribute(bean.getObjectName(), "coreHashCode"))) {
            newNumberOfObjects++;
          }
        } catch (AttributeNotFoundException e) {
          // expected
        }
      }
    }

    log.info("After Reload: Size of infoRegistry: " + registrySize + " MBeans: " + newNumberOfObjects);
    assertEquals("Number of registered MBeans is not the same as info registry size", registrySize, newNumberOfObjects);
  }
}
