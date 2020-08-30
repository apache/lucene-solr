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

package org.apache.solr.cluster.placement.impl;

import java.util.Collection;

import com.google.common.base.Preconditions;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;

/**
 * Superclass for all {@link org.apache.solr.cluster.placement.PropertyKey} that target a {@link Node} and whose implementation
 * is based on using {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}.
 */
abstract class AbstractNodePropertyKey implements PropertyKey {
  private final Node node;
  private final String snitchTag;

  AbstractNodePropertyKey(Node node, String snitchTag) {
    this.node = node;
    this.snitchTag = snitchTag;
  }

  @Override
  public Node getPropertyValueSource() {
    return node;
  }

  /**
   * @return the tag corresponding to this instance of {@link PropertyKey} that can be used to fetch the value from a
   * {@link org.apache.solr.cluster.placement.Node} using {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}.
   * It is a design decision to do a 1:1 correspondence between {@link PropertyKey} and snitches so that each returned
   * {@link org.apache.solr.cluster.placement.PropertyValue} is complete (if it were to be assembled from multiple snitches
   * we'd have to deal with some snitches being returned and some not).
   */
  public String getNodeSnitchTag() {
    return snitchTag;
  }

  /**
   * Given the object returned by {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}
   * for the tag {@link #getNodeSnitchTag()}, builds the appropriate {@link PropertyValue} representing that value.
   * @param nodeValue the value to convert. Is never {@code null}.
   */
  public abstract PropertyValue getPropertyValueFromNodeValue(Object nodeValue);
}


class CoreCountKeyImpl extends AbstractNodePropertyKey {
  public CoreCountKeyImpl(Node node) {
    super(node, ImplicitSnitch.CORES);
  }

  @Override
  public CoresCountPropertyValueImpl getPropertyValueFromNodeValue(final Object nodeValue) {
    return new CoresCountPropertyValueImpl(this, nodeValue);
  }
}

class DiskTypeKeyImpl extends AbstractNodePropertyKey {
  public DiskTypeKeyImpl(Node node) {
    super(node, ImplicitSnitch.DISKTYPE);
  }

  @Override
  public DiskTypePropertyValue getPropertyValueFromNodeValue(Object nodeValue) {
    return new DiskTypePropertyValueImpl(this, nodeValue);
  }
}

class FreeDiskKeyImpl extends AbstractNodePropertyKey {
  public FreeDiskKeyImpl(Node node) {
    super(node, ImplicitSnitch.DISK);
  }

  @Override
  public FreeDiskPropertyValueImpl getPropertyValueFromNodeValue(Object nodeValue) {
    return new FreeDiskPropertyValueImpl(this, nodeValue);
  }
}

class HeapUsageKeyImpl extends AbstractNodePropertyKey {
  public HeapUsageKeyImpl(Node node) {
    super(node, ImplicitSnitch.HEAPUSAGE);
  }

  @Override
  public HeapUsagePropertyValueImpl getPropertyValueFromNodeValue(Object nodeValue) {
    return new HeapUsagePropertyValueImpl(this, nodeValue);
  }
}

class NodeMetricKeyImpl extends AbstractNodePropertyKey {
  public NodeMetricKeyImpl(Node nodeMetricSource, String metricName, PropertyKeyFactory.NodeMetricRegistry registry) {
    super(nodeMetricSource, SolrClientNodeStateProvider.METRICS_PREFIX + SolrMetricManager.getRegistryName(getGroupFromRegistry(registry), metricName));
  }

  private static SolrInfoBean.Group getGroupFromRegistry(PropertyKeyFactory.NodeMetricRegistry registry) {
    switch(registry) {
      case SOLR_JVM:
        return SolrInfoBean.Group.jvm;
      case SOLR_NODE:
        return SolrInfoBean.Group.node;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported registry value " + registry);
    }
  }

  @Override
  public PropertyValue getPropertyValueFromNodeValue(Object nodeValue) {
    return new MetricPropertyValueImpl(this, nodeValue);
  }
}

class SyspropKeyImpl extends AbstractNodePropertyKey {
  public SyspropKeyImpl(Node node, String syspropName) {
    super(node, ImplicitSnitch.SYSPROP + syspropName);
  }

  @Override
  public SyspropPropertyValue getPropertyValueFromNodeValue(Object nodeValue) {
    return new SyspropPropertyValueImpl(this, nodeValue);
  }
}

class SystemLoadKeyImpl extends AbstractNodePropertyKey {
  public SystemLoadKeyImpl(Node node) {
    super(node, ImplicitSnitch.SYSLOADAVG);
  }

  @Override
  public SystemLoadPropertyValue getPropertyValueFromNodeValue(Object nodeValue) {
    return new SystemLoadPropertyValueImpl(this, nodeValue);
  }
}

class TotalDiskKeyImpl extends AbstractNodePropertyKey {
  public TotalDiskKeyImpl(Node node) {
    super(node, SolrClientNodeStateProvider.Variable.TOTALDISK.tagName);
  }

  @Override
  public TotalDiskPropertyValueImpl getPropertyValueFromNodeValue(Object nodeValue) {
    return new TotalDiskPropertyValueImpl(this, nodeValue);
  }
}

class NonNodeMetricKeyImpl implements PropertyKey {
  private final String metricName;
  private final PropertyValueSource metricSource;

  public NonNodeMetricKeyImpl(PropertyValueSource metricSource, String metricName) {
    Preconditions.checkState(!(metricSource instanceof Node), "Illegal argument type " + Node.class);
    this.metricSource = metricSource;
    this.metricName = metricName;
  }

  @Override
  public PropertyValueSource getPropertyValueSource() {
    return metricSource;
  }
}
