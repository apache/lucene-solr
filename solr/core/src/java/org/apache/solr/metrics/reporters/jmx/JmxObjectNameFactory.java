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
package org.apache.solr.metrics.reporters.jmx;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.util.Arrays;

import com.codahale.metrics.jmx.ObjectNameFactory;
import org.apache.solr.metrics.SolrMetricInfo;

/**
 * Factory to create MBean names for a given metric.
 */
public class JmxObjectNameFactory implements ObjectNameFactory {

  private final String domain;
  private final String[] subdomains;
  private final String reporterName;
  private final String[] props;

  /**
   * Create ObjectName factory.
   * @param reporterName name of the reporter
   * @param domain JMX domain name
   * @param additionalProperties additional properties as key, value pairs.
   */
  public JmxObjectNameFactory(String reporterName, String domain, String... additionalProperties) {
    this.reporterName = reporterName.replaceAll(":", "_");
    this.domain = domain;
    this.subdomains = domain.replaceAll(":", "_").split("\\.");
    if (additionalProperties != null && (additionalProperties.length % 2) != 0) {
      throw new IllegalArgumentException("additionalProperties length must be even: " + Arrays.toString(additionalProperties));
    }
    this.props = additionalProperties;
  }

  /**
   * Return current domain.
   */
  public String getDomain() {
    return domain;
  }

  /**
   * Return current reporterName.
   */
  public String getReporterName() {
    return reporterName;
  }

  /**
   * Create a hierarchical name.
   *
   * @param type    metric class, eg. "counters", may be null for non-metric MBeans
   * @param currentDomain  JMX domain
   * @param name    object name
   */
  @Override
  public ObjectName createName(String type, String currentDomain, String name) {
    SolrMetricInfo metricInfo = SolrMetricInfo.of(name);
    String safeName = metricInfo != null ? metricInfo.name : name;
    safeName = safeName.replaceAll(":", "_");
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
        sb.append(currentDomain.replaceAll(":", "_"));
        sb.append(':');
      }
    } else {
      sb.append(currentDomain);
      sb.append(':');
    }
    if (props != null && props.length > 0) {
      boolean added = false;
      for (int i = 0; i < props.length; i += 2) {
        if (props[i] == null || props[i].isEmpty()) {
          continue;
        }
        if (props[i + 1] == null || props[i + 1].isEmpty()) {
          continue;
        }
        sb.append(',');
        sb.append(props[i]);
        sb.append('=');
        sb.append(props[i + 1]);
        added = true;
      }
      if (added) {
        sb.append(',');
      }
    }
    if (metricInfo != null) {
      sb.append("category=");
      sb.append(metricInfo.category.toString());
      if (metricInfo.scope != null) {
        sb.append(",scope=");
        sb.append(metricInfo.scope);
      }
      // we could also split by type, but don't call it 'type' :)
      // if (type != null) {
      //   sb.append(",class=");
      //   sb.append(type);
      // }
      sb.append(",name=");
      sb.append(safeName);
    } else {
      // make dotted names into hierarchies
      String[] path = safeName.split("\\.");
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
      // if (type != null) {
      //   sb.append("class=");
      //   sb.append(type);
      // }
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
