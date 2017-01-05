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
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.Locale;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.ObjectNameFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.metrics.SolrMetricInfo;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.util.JmxUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SolrMetricReporter} that finds (or creates) a MBeanServer from
 * the given configuration and registers metrics to it with JMX.
 */
public class SolrJmxReporter extends SolrMetricReporter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String domain;
  private String agentId;
  private String serviceUrl;

  private JmxReporter reporter;
  private MBeanServer mBeanServer;

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
   * Initializes the reporter by finding (or creating) a MBeanServer
   * and registering the metricManager's metric registry.
   *
   * @param pluginInfo the configuration for the reporter
   */
  @Override
  public synchronized void init(PluginInfo pluginInfo) {
    super.init(pluginInfo);

    if (serviceUrl != null && agentId != null) {
      ManagementFactory.getPlatformMBeanServer(); // Ensure at least one MBeanServer is available.
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.warn("No more than one of serviceUrl(%s) and agentId(%s) should be configured, using first MBeanServer instead of configuration.",
          serviceUrl, agentId, mBeanServer);
    }
    else if (serviceUrl != null) {
      try {
        mBeanServer = JmxUtil.findMBeanServerForServiceUrl(serviceUrl);
      } catch (IOException e) {
        log.warn("findMBeanServerForServiceUrl(%s) exception: %s", serviceUrl, e);
        mBeanServer = null;
      }
    }
    else if (agentId != null) {
      mBeanServer = JmxUtil.findMBeanServerForAgentId(agentId);
    } else {
      ManagementFactory.getPlatformMBeanServer(); // Ensure at least one MBeanServer is available.
      mBeanServer = JmxUtil.findFirstMBeanServer();
      log.warn("No serviceUrl or agentId was configured, using first MBeanServer.", mBeanServer);
    }

    if (mBeanServer == null) {
      log.warn("No JMX server found. Not exposing Solr metrics.");
      return;
    }

    JmxObjectNameFactory jmxObjectNameFactory = new JmxObjectNameFactory(pluginInfo.name, domain);

    reporter = JmxReporter.forRegistry(metricManager.registry(registryName))
                          .registerWith(mBeanServer)
                          .inDomain(domain)
                          .createsObjectNamesWith(jmxObjectNameFactory)
                          .build();
    reporter.start();

    log.info("JMX monitoring enabled at server: " + mBeanServer);
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
   * Sets the domain with which MBeans are published. If none is set,
   * the domain defaults to the name of the core.
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
   * Retrieves the reporter's MBeanServer.
   *
   * @return the reporter's MBeanServer
   */
  public MBeanServer getMBeanServer() {
    return mBeanServer;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "[%s@%s: domain = %s, service url = %s, agent id = %s]",
        getClass().getName(), Integer.toHexString(hashCode()), domain, serviceUrl, agentId);
  }

  /**
   * Factory to create MBean names for a given metric.
   */
  private static class JmxObjectNameFactory implements ObjectNameFactory {

    private final String domain;
    private final String[] subdomains;
    private final String reporterName;

    JmxObjectNameFactory(String reporterName, String domain) {
      this.reporterName = reporterName;
      this.domain = domain;
      this.subdomains = domain.split("\\.");
    }

    /**
     * Create a hierarchical name of a metric.
     *
     * @param type    metric class, eg. "counters"
     * @param currentDomain  JMX domain
     * @param name    metric name
     */
    @Override
    public ObjectName createName(String type, String currentDomain, String name) {
      SolrMetricInfo metricInfo = SolrMetricInfo.of(name);

      // It turns out that ObjectName(String) mostly preserves key ordering
      // as specified in the constructor (except for the 'type' key that ends
      // up at top level) - unlike ObjectName(String, Map) constructor
      // that seems to have a mind of its own...
      StringBuilder sb = new StringBuilder();
      if (domain.equals(currentDomain)) {
        if (subdomains != null && subdomains.length > 1) {
          // use only first segment as domain
          sb.append(subdomains[0]);
          sb.append(':');
          // use remaining segments as properties
          for (int i = 1; i < subdomains.length; i++) {
            if (i > 1) {
              sb.append(',');
            }
            sb.append("dom");
            sb.append(String.valueOf(i));
            sb.append('=');
            sb.append(subdomains[i]);
          }
          sb.append(','); // separate from other properties
        } else {
          sb.append(currentDomain);
          sb.append(':');
        }
      } else {
        sb.append(currentDomain);
        sb.append(':');
      }
      sb.append("reporter=");
      sb.append(reporterName);
      sb.append(',');
      if (metricInfo != null) {
        sb.append("category=");
        sb.append(metricInfo.category.toString());
        sb.append(",scope=");
        sb.append(metricInfo.scope);
        // we could also split by type, but don't call it 'type' :)
        // sb.append(",class=");
        //sb.append(type);
        sb.append(",name=");
        sb.append(metricInfo.name);
      } else {
        // make dotted names into hierarchies
        String[] path = name.split("\\.");
        for (int i = 0; i < path.length - 1; i++) {
          if (i > 0) {
            sb.append(',');
          }
          sb.append("name"); sb.append(String.valueOf(i));
          sb.append('=');
          sb.append(path[i]);
        }
        if (path.length > 1) {
          sb.append(',');
        }
        // split by type
        // sb.append("class=");
        // sb.append(type);
        sb.append("name=");
        sb.append(path[path.length - 1]);
      }

      ObjectName objectName;

      try {
        objectName = new ObjectName(sb.toString());
      } catch (MalformedObjectNameException e) {
        throw new RuntimeException(sb.toString(), e);
      }

      return objectName;
    }
  }

}
