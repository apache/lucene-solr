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
import java.util.Locale;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import org.apache.solr.metrics.FilteringSolrMetricReporter;
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
 * <p>NOTE: {@link com.codahale.metrics.jmx.JmxReporter} that this class uses exports only newly added metrics (it doesn't
 * process already existing metrics in a registry)</p>
 */
public class SolrJmxReporter extends FilteringSolrMetricReporter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final ReporterClientCache<MBeanServer> serviceRegistry = new ReporterClientCache<>();

  private String domain;
  private String agentId;
  private String serviceUrl;
  private String rootName;

  private MetricRegistry registry;
  private MBeanServer mBeanServer;
  private JmxMetricsReporter reporter;
  private String instanceTag;
  private boolean started;

  /**
   * Creates a new instance of {@link SolrJmxReporter}.
   *
   * @param registryName name of the registry to report
   */
  public SolrJmxReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
    period = 0; // setting to zero to indicate not applicable
    setDomain(registryName);
  }

  protected synchronized void doInit() {
    if (serviceUrl != null && agentId != null) {
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.warn("No more than one of serviceUrl({}) and agentId({}) should be configured, using first MBeanServer {} instead of configuration.",
          serviceUrl, agentId, mBeanServer);
    } else if (serviceUrl != null) {
      // reuse existing services
      mBeanServer = serviceRegistry.getOrCreate(serviceUrl, () -> JmxUtil.findMBeanServerForServiceUrl(serviceUrl));
    } else if (agentId != null) {
      mBeanServer = JmxUtil.findMBeanServerForAgentId(agentId);
    } else {
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.debug("No serviceUrl or agentId was configured, using first MBeanServer: {}", mBeanServer);
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

    final MetricFilter filter = newMetricFilter();
    instanceTag = Integer.toHexString(this.hashCode());
    reporter = JmxMetricsReporter.forRegistry(registry)
                          .registerWith(mBeanServer)
                          .inDomain(fullDomain)
                          .filter(filter)
                          .createsObjectNamesWith(jmxObjectNameFactory)
                          .withTag(instanceTag)
                          .build();
    reporter.start();
    started = true;
    log.info("JMX monitoring for '{}' (registry '{}') enabled at server: {}", fullDomain, registryName, mBeanServer);
  }

  /**
   * For unit tests.
   */
  public String getInstanceTag() {
    return instanceTag;
  }

  /**
   * Stops the reporter from publishing metrics.
   */
  @Override
  public synchronized void close() {
    log.info("Closing reporter {} for registry {}/{}", this, registryName, registry);
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
    if (period != 0) {
      throw new IllegalStateException("Init argument 'period' is not supported for "+getClass().getCanonicalName());
    }
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
