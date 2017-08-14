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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.PluginInfo;

import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.metrics.reporters.jmx.JmxMetricsReporter;
import org.apache.solr.metrics.reporters.jmx.JmxObjectNameFactory;
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

  private MetricRegistry registry;
  private MBeanServer mBeanServer;
  private JmxMetricsReporter reporter;
  private boolean started;

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
    MetricFilter filter;
    if (filters.isEmpty()) {
      filter = MetricFilter.ALL;
    } else {
      filter = new SolrMetricManager.PrefixFilter(filters);
    }

    String tag = Integer.toHexString(this.hashCode());
    reporter = JmxMetricsReporter.forRegistry(registry)
                          .registerWith(mBeanServer)
                          .inDomain(fullDomain)
                          .filter(filter)
                          .createsObjectNamesWith(jmxObjectNameFactory)
                          .withTag(tag)
                          .build();
    reporter.start();
    started = true;
    log.info("JMX monitoring for '" + fullDomain + "' (registry '" + registryName + "') enabled at server: " + mBeanServer);
  }

  /**
   * Stops the reporter from publishing metrics.
   */
  @Override
  public synchronized void close() {
    log.info("Closing reporter " + this + " for registry " + registryName + " / " + registry);
    started = false;
    if (reporter != null) {
      reporter.close();
      reporter = null;
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
   * @return true if this reporter is going to report metrics to JMX.
   */
  public boolean isActive() {
    return reporter != null;
  }

  /**
   * For unit tests.
   * @return true if this reporter has been started and is reporting metrics to JMX.
   */
  public boolean isStarted() {
    return started;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "[%s@%s: rootName = %s, domain = %s, service url = %s, agent id = %s]",
        getClass().getName(), Integer.toHexString(hashCode()), rootName, domain, serviceUrl, agentId);
  }
}
