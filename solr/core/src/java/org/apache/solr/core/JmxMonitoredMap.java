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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig.JmxConfiguration;

import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Responsible for finding (or creating) a MBeanServer from given configuration
 * and registering all SolrInfoMBean objects with JMX.
 * </p>
 * <p/>
 * <p>
 * Please see http://wiki.apache.org/solr/SolrJmx for instructions on usage and configuration
 * </p>
 *
 *
 * @see org.apache.solr.core.SolrConfig.JmxConfiguration
 * @since solr 1.3
 */
public class JmxMonitoredMap<K, V> extends
        ConcurrentHashMap<String, SolrInfoMBean> {
  private static final Logger LOG = LoggerFactory.getLogger(JmxMonitoredMap.class
          .getName());

  private MBeanServer server = null;

  private String jmxRootName;

  public JmxMonitoredMap(final String coreName, 
                         final JmxConfiguration jmxConfig) {
    jmxRootName = (null != jmxConfig.rootName ? 
                   jmxConfig.rootName
                   : ("solr" + (null != coreName ? "/" + coreName : "")));
      
    if (jmxConfig.serviceUrl == null) {
      List<MBeanServer> servers = null;

      if (jmxConfig.agentId == null) {
        // Try to find the first MBeanServer
        servers = MBeanServerFactory.findMBeanServer(null);
      } else if (jmxConfig.agentId != null) {
        // Try to find the first MBean server with the given agentId
        servers = MBeanServerFactory.findMBeanServer(jmxConfig.agentId);
        // throw Exception if no servers were found with the given agentId
        if (servers == null || servers.isEmpty())
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                  "No JMX Servers found with agentId: " + jmxConfig.agentId);
      }

      if (servers == null || servers.isEmpty()) {
        LOG.info("No JMX servers found, not exposing Solr information with JMX.");
        return;
      }
      server = servers.get(0);
      LOG.info("JMX monitoring is enabled. Adding Solr mbeans to JMX Server: "
               + server);
    } else {
      try {
        // Create a new MBeanServer with the given serviceUrl
        server = MBeanServerFactory.newMBeanServer();
        JMXConnectorServer connector = JMXConnectorServerFactory
                .newJMXConnectorServer(new JMXServiceURL(jmxConfig.serviceUrl),
                        null, server);
        connector.start();
        LOG.info("JMX monitoring is enabled at " + jmxConfig.serviceUrl);
      } catch (Exception e) {
        // Release the reference
        server = null;
        throw new RuntimeException("Could not start JMX monitoring ", e);
      }
    }
  }

  /**
   * Clears the map and unregisters all SolrInfoMBeans in the map from
   * MBeanServer
   */
  @Override
  public void clear() {
    if (server != null) {
      for (Map.Entry<String, SolrInfoMBean> entry : entrySet()) {
        unregister(entry.getKey(), entry.getValue());
      }
    }

    super.clear();
  }

  /**
   * Adds the SolrInfoMBean to the map and registers the given SolrInfoMBean
   * instance with the MBeanServer defined for this core. If a SolrInfoMBean is
   * already registered with the MBeanServer then it is unregistered and then
   * re-registered.
   *
   * @param key      the JMX type name for this SolrInfoMBean
   * @param infoBean the SolrInfoMBean instance to be registered
   */
  @Override
  public SolrInfoMBean put(String key, SolrInfoMBean infoBean) {
    if (server != null && infoBean != null) {
      try {
        ObjectName name = getObjectName(key, infoBean);
        if (server.isRegistered(name))
          server.unregisterMBean(name);
        SolrDynamicMBean mbean = new SolrDynamicMBean(infoBean);
        server.registerMBean(mbean, name);
      } catch (Exception e) {
        LOG.warn( "Failed to register info bean: " + key, e);
      }
    }

    return super.put(key, infoBean);
  }

  /**
   * Removes the SolrInfoMBean object at the given key and unregisters it from
   * MBeanServer
   *
   * @param key the JMX type name for this SolrInfoMBean
   */
  @Override
  public SolrInfoMBean remove(Object key) {
    SolrInfoMBean infoBean = get(key);
    if (infoBean != null) {
      try {
        unregister((String) key, infoBean);
      } catch (RuntimeException e) {
        LOG.warn( "Failed to unregister info bean: " + key, e);
      }
    }
    return super.remove(key);
  }

  private void unregister(String key, SolrInfoMBean infoBean) {
    if (server == null)
      return;

    try {
      ObjectName name = getObjectName(key, infoBean);
      if (server.isRegistered(name)) {
        server.unregisterMBean(name);
      } else {
        LOG.info("Failed to unregister mbean: " + key
                + " because it was not registered");
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Failed to unregister info bean: " + key, e);
    }
  }

  private ObjectName getObjectName(String key, SolrInfoMBean infoBean)
          throws MalformedObjectNameException {
    Hashtable<String, String> map = new Hashtable<String, String>();
    map.put("type", key);
    if (infoBean.getName() != null && !"".equals(infoBean.getName())) {
      map.put("id", infoBean.getName());
    }
    return ObjectName.getInstance(jmxRootName, map);
  }

  /**
   * DynamicMBean is used to dynamically expose all SolrInfoMBean
   * getStatistics() NameList keys as String getters.
   */
  static class SolrDynamicMBean implements DynamicMBean {
    private SolrInfoMBean infoBean;

    private HashSet<String> staticStats;

    public SolrDynamicMBean(SolrInfoMBean managedResource) {
      this.infoBean = managedResource;
      staticStats = new HashSet<String>();

      // For which getters are already available in SolrInfoMBean
      staticStats.add("name");
      staticStats.add("version");
      staticStats.add("description");
      staticStats.add("category");
      staticStats.add("sourceId");
      staticStats.add("source");
    }

    public MBeanInfo getMBeanInfo() {
      ArrayList<MBeanAttributeInfo> attrInfoList = new ArrayList<MBeanAttributeInfo>();

      for (String stat : staticStats) {
        attrInfoList.add(new MBeanAttributeInfo(stat, String.class.getName(),
                null, true, false, false));
      }

      try {
        NamedList dynamicStats = infoBean.getStatistics();
        if (dynamicStats != null) {
          for (int i = 0; i < dynamicStats.size(); i++) {
            String name = dynamicStats.getName(i);
            if (!staticStats.contains(name))
              attrInfoList.add(new MBeanAttributeInfo(dynamicStats.getName(i),
                      String.class.getName(), null, true, false, false));
          }
        }
      } catch (Exception e) {
        LOG.warn( "Could not getStatistics on info bean "
                + infoBean.getName(), e);
      }

      MBeanAttributeInfo[] attrInfoArr = attrInfoList
              .toArray(new MBeanAttributeInfo[attrInfoList.size()]);
      return new MBeanInfo(getClass().getName(), infoBean
              .getDescription(), attrInfoArr, null, null, null);
    }

    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
      Object val;
      if (staticStats.contains(attribute) && attribute != null
              && attribute.length() > 0) {
        try {
          String getter = "get" + attribute.substring(0, 1).toUpperCase(Locale.ENGLISH)
                  + attribute.substring(1);
          Method meth = infoBean.getClass().getMethod(getter);
          val = meth.invoke(infoBean);
        } catch (Exception e) {
          throw new AttributeNotFoundException(attribute);
        }
      } else {
        NamedList list = infoBean.getStatistics();
        val = list.get(attribute);
      }

      if (val != null)
        return val.toString();
      else
        return val;

    }

    public AttributeList getAttributes(String[] attributes) {
      AttributeList list = new AttributeList();
      for (String attribute : attributes) {
        try {
          list.add(new Attribute(attribute, getAttribute(attribute)));
        } catch (Exception e) {
          LOG.warn("Could not get attibute " + attribute);
        }
      }

      return list;
    }

    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException,
            MBeanException, ReflectionException {
      throw new UnsupportedOperationException("Operation not Supported");
    }

    public AttributeList setAttributes(AttributeList attributes) {
      throw new UnsupportedOperationException("Operation not Supported");
    }

    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
      throw new UnsupportedOperationException("Operation not Supported");
    }
  }
}
