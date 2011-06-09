/**
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig.JmxConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.Query;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.net.ServerSocket;
import java.net.URL;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.Set;

/**
 * Test for JmxMonitoredMap
 *
 *
 * @since solr 1.3
 */
public class TestJmxMonitoredMap extends LuceneTestCase {

  private int port = 0;

  private JMXConnector connector;

  private MBeanServerConnection mbeanServer;

  private JmxMonitoredMap<String, SolrInfoMBean> monitoredMap;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    int retries = 5;
    for (int i = 0; i < retries; i++) {
      try {
        ServerSocket server = new ServerSocket(0);
        try {
          port = server.getLocalPort();
        } finally {
          server.close();
        }
        // System.out.println("Using port: " + port);
        try {
          LocateRegistry.createRegistry(port);
        } catch (RemoteException e) {
          throw e;
        }
        String url = "service:jmx:rmi:///jndi/rmi://:" + port + "/solrjmx";
        JmxConfiguration config = new JmxConfiguration(true, null, url, null);
        monitoredMap = new JmxMonitoredMap<String, SolrInfoMBean>(null, config);
        JMXServiceURL u = new JMXServiceURL(url);
        connector = JMXConnectorFactory.connect(u);
        mbeanServer = connector.getMBeanServerConnection();
        break;
      } catch (Exception e) {
        if(retries == (i + 1)) {
          throw e;
        }
      }
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      connector.close();
    } catch (Exception e) {
    }
    super.tearDown();
  }

  @Test
  public void testPutRemoveClear() throws Exception {
    MockInfoMBean mock = new MockInfoMBean();
    monitoredMap.put("mock", mock);

    Set<ObjectInstance> objects = mbeanServer.queryMBeans(null, Query.match(
            Query.attr("name"), Query.value("mock")));
    assertFalse("No MBean for mock object found in MBeanServer", objects
            .isEmpty());

    monitoredMap.remove("mock");
    objects = mbeanServer.queryMBeans(null, Query.match(Query.attr("name"),
            Query.value("mock")));
    assertTrue("MBean for mock object found in MBeanServer even after removal",
            objects.isEmpty());

    monitoredMap.put("mock", mock);
    monitoredMap.put("mock2", mock);
    objects = mbeanServer.queryMBeans(null, Query.match(Query.attr("name"),
            Query.value("mock")));
    assertFalse("No MBean for mock object found in MBeanServer", objects
            .isEmpty());

    monitoredMap.clear();
    objects = mbeanServer.queryMBeans(null, Query.match(Query.attr("name"),
            Query.value("mock")));
    assertTrue(
            "MBean for mock object found in MBeanServer even after clear has been called",
            objects.isEmpty());
  }

  private class MockInfoMBean implements SolrInfoMBean {
    public String getName() {
      return "mock";
    }

    public Category getCategory() {
      return Category.OTHER;
    }

    public String getDescription() {
      return "mock";
    }

    public URL[] getDocs() {
      // TODO Auto-generated method stub
      return null;
    }

    public String getVersion() {
      return "mock";
    }

    public String getSource() {
      return "mock";
    }

    @SuppressWarnings("unchecked")
    public NamedList getStatistics() {
      return null;
    }

    public String getSourceId() {
      return "mock";
    }
  }

}
