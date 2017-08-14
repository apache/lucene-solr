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

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig.JmxConfiguration;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.reporters.jmx.JmxObjectNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * <p>
 * Responsible for finding (or creating) a MBeanServer from given configuration
 * and registering all SolrInfoMBean objects with JMX.
 * </p>
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
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String REPORTER_NAME = "_jmx_";

  // set to true to use cached statistics NamedLists between getMBeanInfo calls to work
  // around over calling getStatistics on MBeanInfos when iterating over all attributes (SOLR-6586)
  private final boolean useCachedStatsBetweenGetMBeanInfoCalls = Boolean.getBoolean("useCachedStatsBetweenGetMBeanInfoCalls");
  
  private final MBeanServer server;

  private final String jmxRootName;

  private final String coreHashCode;

  private final JmxObjectNameFactory nameFactory;

  private final String registryName;

  public JmxMonitoredMap(String coreName, String coreHashCode,
                         final JmxConfiguration jmxConfig) {
    this.coreHashCode = coreHashCode;
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
        server = null;
        registryName = null;
        nameFactory = null;
        log.debug("No JMX servers found, not exposing Solr information with JMX.");
        return;
      }
      server = servers.get(0);
      log.info("JMX monitoring is enabled. Adding Solr mbeans to JMX Server: "
               + server);
    } else {
      MBeanServer newServer = null;
      try {
        // Create a new MBeanServer with the given serviceUrl
        newServer = MBeanServerFactory.newMBeanServer();
        JMXConnectorServer connector = JMXConnectorServerFactory
                .newJMXConnectorServer(new JMXServiceURL(jmxConfig.serviceUrl),
                        null, newServer);
        connector.start();
        log.info("JMX monitoring is enabled at " + jmxConfig.serviceUrl);
      } catch (Exception e) {
        // Release the reference
        throw new RuntimeException("Could not start JMX monitoring ", e);
      }
      server = newServer;
    }
    registryName = SolrCoreMetricManager.createRegistryName(null, coreName);
    nameFactory = new JmxObjectNameFactory(REPORTER_NAME + coreHashCode, registryName);
  }

  /**
   * Clears the map and unregisters all SolrInfoMBeans in the map from
   * MBeanServer
   */
  @Override
  public void clear() {
    if (server != null) {
      QueryExp exp = Query.or(Query.eq(Query.attr("coreHashCode"), Query.value(coreHashCode)),
                            Query.eq(Query.attr("reporter"), Query.value(REPORTER_NAME + coreHashCode)));
      
      Set<ObjectName> objectNames = null;
      try {
        objectNames = server.queryNames(null, exp);
      } catch (Exception e) {
        log.warn("Exception querying for mbeans", e);
      }
      
      if (objectNames != null)  {
        for (ObjectName name : objectNames) {
          try {
            server.unregisterMBean(name);
          } catch (Exception e) {
            log.warn("Exception un-registering mbean {}", name, e);
          }
        }
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
        // back-compat name
        ObjectName name = getObjectName(key, infoBean);
        if (server.isRegistered(name))
          server.unregisterMBean(name);
        SolrDynamicMBean mbean = new SolrDynamicMBean(coreHashCode, infoBean, useCachedStatsBetweenGetMBeanInfoCalls);
        server.registerMBean(mbean, name);
        // now register it also under new name
        String beanName = createBeanName(infoBean, key);
        name = nameFactory.createName(null, registryName, beanName);
        if (server.isRegistered(name))
          server.unregisterMBean(name);
        server.registerMBean(mbean, name);
      } catch (Exception e) {
        log.warn( "Failed to register info bean: key=" + key + ", infoBean=" + infoBean, e);
      }
    }

    return super.put(key, infoBean);
  }

  private String createBeanName(SolrInfoMBean infoBean, String key) {
    if (infoBean.getCategory() == null) {
      throw new IllegalArgumentException("SolrInfoMBean.category must never be null: " + infoBean);
    }
    StringBuilder sb = new StringBuilder();
    sb.append(infoBean.getCategory().toString());
    sb.append('.');
    sb.append(key);
    sb.append('.');
    sb.append(infoBean.getName());
    return sb.toString();
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
        log.warn( "Failed to unregister info bean: " + key, e);
      }
    }
    return super.remove(key);
  }

  private void unregister(String key, SolrInfoMBean infoBean) {
    if (server == null)
      return;

    try {
      // remove legacy name
      ObjectName name = getObjectName(key, infoBean);
      if (server.isRegistered(name) && coreHashCode.equals(server.getAttribute(name, "coreHashCode"))) {
        server.unregisterMBean(name);
      }
      // remove new name
      String beanName = createBeanName(infoBean, key);
      name = nameFactory.createName(null, registryName, beanName);
      if (server.isRegistered(name)) {
        server.unregisterMBean(name);
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Failed to unregister info bean: " + key, e);
    }
  }

  private ObjectName getObjectName(String key, SolrInfoMBean infoBean)
          throws MalformedObjectNameException {
    Hashtable<String, String> map = new Hashtable<>();
    map.put("type", key);
    if (infoBean.getName() != null && !"".equals(infoBean.getName())) {
      map.put(ID, infoBean.getName());
    }
    return ObjectName.getInstance(jmxRootName, map);
  }

  /** For test verification */
  public MBeanServer getServer() {
    return server;
  }

  /**
   * DynamicMBean is used to dynamically expose all SolrInfoMBean
   * getStatistics() NameList keys as String getters.
   */
  static class SolrDynamicMBean implements DynamicMBean {
    private SolrInfoMBean infoBean;

    private HashSet<String> staticStats;

    private String coreHashCode;
    
    private volatile NamedList cachedDynamicStats;
    
    private boolean useCachedStatsBetweenGetMBeanInfoCalls;
    
    public SolrDynamicMBean(String coreHashCode, SolrInfoMBean managedResource) {
      this(coreHashCode, managedResource, false);
    }

    public SolrDynamicMBean(String coreHashCode, SolrInfoMBean managedResource, boolean useCachedStatsBetweenGetMBeanInfoCalls) {
      this.useCachedStatsBetweenGetMBeanInfoCalls = useCachedStatsBetweenGetMBeanInfoCalls;
      if (managedResource instanceof JmxAugmentedSolrInfoMBean) {
        final JmxAugmentedSolrInfoMBean jmxSpecific = (JmxAugmentedSolrInfoMBean)managedResource;
        this.infoBean = new SolrInfoMBeanWrapper(jmxSpecific) {
          @Override
          public NamedList getStatistics() { return jmxSpecific.getStatisticsForJmx(); }
        };
      } else {
        this.infoBean = managedResource;
      }
      staticStats = new HashSet<>();

      // For which getters are already available in SolrInfoMBean
      staticStats.add(NAME);
      staticStats.add("version");
      staticStats.add("description");
      staticStats.add("category");
      staticStats.add("source");
      this.coreHashCode = coreHashCode;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
      ArrayList<MBeanAttributeInfo> attrInfoList = new ArrayList<>();

      for (String stat : staticStats) {
        attrInfoList.add(new MBeanAttributeInfo(stat, String.class.getName(),
                null, true, false, false));
      }

      // add core's hashcode
      attrInfoList.add(new MBeanAttributeInfo("coreHashCode", String.class.getName(),
                null, true, false, false));

      try {
        NamedList dynamicStats = infoBean.getStatistics();
        
        if (useCachedStatsBetweenGetMBeanInfoCalls) {
          cachedDynamicStats = dynamicStats;
        }
        
        if (dynamicStats != null) {
          for (int i = 0; i < dynamicStats.size(); i++) {
            String name = dynamicStats.getName(i);
            if (staticStats.contains(name)) {
              continue;
            }
            Class type = dynamicStats.get(name).getClass();
            OpenType typeBox = determineType(type);
            if (type.equals(String.class) || typeBox == null) {
              attrInfoList.add(new MBeanAttributeInfo(dynamicStats.getName(i),
                  String.class.getName(), null, true, false, false));
            } else {
              attrInfoList.add(new OpenMBeanAttributeInfoSupport(
                  dynamicStats.getName(i), dynamicStats.getName(i), typeBox,
                  true, false, false));
            }
          }
        }
      } catch (Exception e) {
        // don't log issue if the core is closing
        if (!(SolrException.getRootCause(e) instanceof AlreadyClosedException))
          log.warn("Could not getStatistics on info bean {}", infoBean.getName(), e);
      }

      MBeanAttributeInfo[] attrInfoArr = attrInfoList
              .toArray(new MBeanAttributeInfo[attrInfoList.size()]);
      return new MBeanInfo(getClass().getName(), infoBean
              .getDescription(), attrInfoArr, null, null, null);
    }

    private OpenType determineType(Class type) {
      try {
        for (Field field : SimpleType.class.getFields()) {
          if (field.getType().equals(SimpleType.class)) {
            SimpleType candidate = (SimpleType) field.get(SimpleType.class);
            if (candidate.getTypeName().equals(type.getName())) {
              return candidate;
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
      Object val;
      if ("coreHashCode".equals(attribute)) {
        val = coreHashCode;
      } else if (staticStats.contains(attribute) && attribute != null
              && attribute.length() > 0) {
        try {
          String getter = "get" + attribute.substring(0, 1).toUpperCase(Locale.ROOT)
                  + attribute.substring(1);
          Method meth = infoBean.getClass().getMethod(getter);
          val = meth.invoke(infoBean);
        } catch (Exception e) {
          throw new AttributeNotFoundException(attribute);
        }
      } else {
        NamedList stats = null;
        if (useCachedStatsBetweenGetMBeanInfoCalls) {
          NamedList cachedStats = this.cachedDynamicStats;
          if (cachedStats != null) {
            stats = cachedStats;
          }
        }
        if (stats == null) {
          stats = infoBean.getStatistics();
        }
        val = stats.get(attribute);
      }

      if (val != null) {
        // It's String or one of the simple types, just return it as JMX suggests direct support for such types
        for (String simpleTypeName : SimpleType.ALLOWED_CLASSNAMES_LIST) {
          if (val.getClass().getName().equals(simpleTypeName)) {
            return val;
          }
        }
        // It's an arbitrary object which could be something complex and odd, return its toString, assuming that is
        // a workable representation of the object
        return val.toString();
      }
      return null;
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
      AttributeList list = new AttributeList();
      for (String attribute : attributes) {
        try {
          list.add(new Attribute(attribute, getAttribute(attribute)));
        } catch (Exception e) {
          log.warn("Could not get attribute " + attribute);
        }
      }

      return list;
    }

    @Override
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException,
            MBeanException, ReflectionException {
      throw new UnsupportedOperationException("Operation not Supported");
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
      throw new UnsupportedOperationException("Operation not Supported");
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
      throw new UnsupportedOperationException("Operation not Supported");
    }
  }

  /**
   * SolrInfoMBean that provides JMX-specific statistics.  Used, for example,
   * if generating full statistics is expensive; the expensive statistics can
   * be generated normally for use with the web ui, while an abbreviated version
   * are generated for period jmx use.
   */
  public interface JmxAugmentedSolrInfoMBean extends SolrInfoMBean {
    /**
     * JMX-specific statistics
     */
    public NamedList getStatisticsForJmx();
  }
}
