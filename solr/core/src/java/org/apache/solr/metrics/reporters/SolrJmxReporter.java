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

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.util.JmxUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SolrMetricReporter} that finds (or creates) a MBeanServer from
 * the given configuration and registers metrics to it with JMX.
 * <p>NOTE: {@link JmxReporter} that this class uses exports only newly added metrics (it doesn't
 * process already existing metrics in a registry)</p>
 */
public class SolrJmxReporter extends SolrMetricReporter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final ReporterClientCache<MBeanServer> serviceRegistry = new ReporterClientCache<>();

  private String domain;
  private String agentId;
  private String serviceUrl;
  private String rootName;
  private List<String> filters = new ArrayList<>();

  private JmxReporter reporter;
  private MetricRegistry registry;
  private MBeanServer mBeanServer;
  private MetricsMapListener listener;

  /**
   * Creates a new instance of {@link SolrJmxReporter}.
   *
   * @param registryName name of the registry to report
   */
  public SolrJmxReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
    setDomain(registryName);
  }

  /**
   * Initializes the reporter by finding an MBeanServer
   * and registering the metricManager's metric registry.
   *
   * @param pluginInfo the configuration for the reporter
   */
  @Override
  public synchronized void init(PluginInfo pluginInfo) {
    super.init(pluginInfo);
    if (!enabled) {
      log.info("Reporter disabled for registry " + registryName);
      return;
    }
    log.debug("Initializing for registry " + registryName);
    if (serviceUrl != null && agentId != null) {
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.warn("No more than one of serviceUrl({}) and agentId({}) should be configured, using first MBeanServer instead of configuration.",
          serviceUrl, agentId, mBeanServer);
    } else if (serviceUrl != null) {
      // reuse existing services
      mBeanServer = serviceRegistry.getOrCreate(serviceUrl, () -> JmxUtil.findMBeanServerForServiceUrl(serviceUrl));
    } else if (agentId != null) {
      mBeanServer = JmxUtil.findMBeanServerForAgentId(agentId);
    } else {
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.debug("No serviceUrl or agentId was configured, using first MBeanServer: " + mBeanServer);
    }

    if (mBeanServer == null) {
      log.warn("No JMX server found. Not exposing Solr metrics via JMX.");
      return;
    }

    if (domain == null || domain.isEmpty()) {
      domain = registryName;
    }
    String fullDomain = domain;
    if (rootName != null && !rootName.isEmpty()) {
      fullDomain = rootName + "." + domain;
    }
    JmxObjectNameFactory jmxObjectNameFactory = new JmxObjectNameFactory(pluginInfo.name, fullDomain);
    registry = metricManager.registry(registryName);
    // filter out MetricsMap gauges - we have a better way of handling them
    MetricFilter mmFilter = (name, metric) -> !(metric instanceof MetricsMap);
    MetricFilter filter;
    if (filters.isEmpty()) {
      filter = mmFilter;
    } else {
      // apply also prefix filters
      SolrMetricManager.PrefixFilter prefixFilter = new SolrMetricManager.PrefixFilter(filters);
      filter = new SolrMetricManager.AndFilter(prefixFilter, mmFilter);
    }

    reporter = JmxReporter.forRegistry(registry)
                          .registerWith(mBeanServer)
                          .inDomain(fullDomain)
                          .filter(filter)
                          .createsObjectNamesWith(jmxObjectNameFactory)
                          .build();
    reporter.start();
    // workaround for inability to register custom MBeans (to be available in metrics 4.0?)
    listener = new MetricsMapListener(mBeanServer, jmxObjectNameFactory);
    registry.addListener(listener);

    log.info("JMX monitoring for '" + fullDomain + "' (registry '" + registryName + "') enabled at server: " + mBeanServer);
  }

  /**
   * Stops the reporter from publishing metrics.
   */
  @Override
  public synchronized void close() {
    if (reporter != null) {
      reporter.close();
      reporter = null;
    }
    if (listener != null && registry != null) {
      registry.removeListener(listener);
      listener.close();
      listener = null;
    }
  }

  /**
   * Validates that the reporter has been correctly configured.
   * Note that all configurable arguments are currently optional.
   *
   * @throws IllegalStateException if the reporter is not properly configured
   */
  @Override
  protected void validate() throws IllegalStateException {
    // Nothing to validate
  }


  /**
   * Set root name of the JMX hierarchy for this reporter. Default (null or empty) is none, ie.
   * the hierarchy will start from the domain name.
   * @param rootName root name of the JMX name hierarchy, or null or empty for default.
   */
  public void setRootName(String rootName) {
    this.rootName = rootName;
  }

  /**
   * Sets the domain with which MBeans are published. If none is set,
   * the domain defaults to the name of the registry.
   *
   * @param domain the domain
   */
  public void setDomain(String domain) {
    if (domain != null) {
      this.domain = domain;
    } else {
      this.domain = registryName;
    }
  }

  /**
   * Sets the service url for a JMX server.
   * Note that this configuration is optional.
   *
   * @param serviceUrl the service url
   */
  public void setServiceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
  }

  /**
   * Sets the agent id for a JMX server.
   * Note that this configuration is optional.
   *
   * @param agentId the agent id
   */
  public void setAgentId(String agentId) {
    this.agentId = agentId;
  }

  /**
   * Return configured agentId or null.
   */
  public String getAgentId() {
    return agentId;
  }

  /**
   * Return configured serviceUrl or null.
   */
  public String getServiceUrl() {
    return serviceUrl;
  }

  /**
   * Return configured domain or null.
   */
  public String getDomain() {
    return domain;
  }

  /**
   * Report only metrics with names matching any of the prefix filters.
   * @param filters list of 0 or more prefixes. If the list is empty then
   *                all names will match.
   */
  public void setFilter(List<String> filters) {
    if (filters == null || filters.isEmpty()) {
      return;
    }
    this.filters.addAll(filters);
  }

  public void setFilter(String filter) {
    if (filter != null && !filter.isEmpty()) {
      this.filters.add(filter);
    }
  }

  /**
   * Return the reporter's MBeanServer.
   *
   * @return the reporter's MBeanServer
   */
  public MBeanServer getMBeanServer() {
    return mBeanServer;
  }

  /**
   * For unit tests.
   * @return true if this reporter is actively reporting metrics to JMX.
   */
  public boolean isActive() {
    return reporter != null;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "[%s@%s: rootName = %s, domain = %s, service url = %s, agent id = %s]",
        getClass().getName(), Integer.toHexString(hashCode()), rootName, domain, serviceUrl, agentId);
  }

  private static class MetricsMapListener extends MetricRegistryListener.Base {
    MBeanServer server;
    JmxObjectNameFactory nameFactory;
    // keep the names so that we can unregister them on core close
    Set<ObjectName> registered = new HashSet<>();

    MetricsMapListener(MBeanServer server, JmxObjectNameFactory nameFactory) {
      this.server = server;
      this.nameFactory = nameFactory;
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
      if (!(gauge instanceof MetricsMap)) {
        return;
      }
      synchronized (server) {
        try {
          ObjectName objectName = nameFactory.createName("gauges", nameFactory.getDomain(), name);
          log.debug("REGISTER " + objectName);
          if (registered.contains(objectName) || server.isRegistered(objectName)) {
            log.debug("-unregistering old instance of " + objectName);
            try {
              server.unregisterMBean(objectName);
            } catch (InstanceNotFoundException e) {
              // ignore
            }
          }
          // some MBean servers re-write object name to include additional properties
          ObjectInstance instance = server.registerMBean(gauge, objectName);
          if (instance != null) {
            registered.add(instance.getObjectName());
          }
        } catch (Exception e) {
          log.warn("bean registration error", e);
        }
      }
    }

    public void close() {
      synchronized (server) {
        for (ObjectName name : registered) {
          try {
            if (server.isRegistered(name)) {
              server.unregisterMBean(name);
            }
          } catch (Exception e) {
            log.debug("bean unregistration error", e);
          }
        }
        registered.clear();
      }
    }
  }
}
