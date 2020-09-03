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
import java.util.Optional;

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

  public Node getNode() {
    return node;
  }

  /**
   * @return the tag corresponding to this instance of {@link PropertyKey} that can be used to fetch the value from a
   * {@link org.apache.solr.cluster.placement.Node} using {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}.
   *
   * If ever the design is changed so {@link PropertyKey}'s can serve different values (for example free disk and total
   * disk in single {@link PropertyKey} as opposed to two), we could be returning multiple tags here and later when doing
   * the resolution set those of the values that were received (given all values are returned in {@link Optional}, this is possible).
   */
  public String getNodeSnitchTag() {
    return snitchTag;
  }

  /**
   * Given the object returned by {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}
   * for the tag {@link #getNodeSnitchTag()}, casts appropriately and sets the optional value on the key returned by the
   * key specific value getter methods.
   *
   * @param nodeValue the value to convert. Is never {@code null}.
   */
  abstract void setValue(final Object nodeValue);
  abstract void setEmpty();


  static class CoreCountImpl extends AbstractNodePropertyKey implements PropertyKey.CoresCount {
    Optional<Integer> coresCount = Optional.empty();

    public CoreCountImpl(Node node) {
      super(node, ImplicitSnitch.CORES);
    }

    @Override
    void setEmpty() {
      coresCount = Optional.empty();
    }

    @Override
    void setValue(Object nodeValue) {
      coresCount = Optional.of(((Number) nodeValue).intValue());
    }

    @Override
    public Optional<Integer> getCoresCount() {
      return coresCount;
    }
  }


  static class DiskTypeImpl extends AbstractNodePropertyKey implements PropertyKey.DiskType {
    Optional<DiskType.HardwareType> hardwareType = Optional.empty();

    public DiskTypeImpl(Node node) {
      super(node, ImplicitSnitch.DISKTYPE);
    }

    @Override
    void setEmpty() {
      hardwareType = Optional.empty();
    }

    @Override
    void setValue(Object nodeValue) {
      if ("rotational".equals(nodeValue)) {
        hardwareType = Optional.of(HardwareType.ROTATIONAL);
      } else if ("ssd".equals(nodeValue)) {
        hardwareType = Optional.of(HardwareType.SSD);
      } else {
        setEmpty();
      }
    }

    @Override
    public Optional<HardwareType> getHardwareType() {
      return hardwareType;
    }
  }


  static class TotalDiskImpl extends AbstractNodePropertyKey implements PropertyKey.TotalDisk {
    Optional<Long> totalDiskSize = Optional.empty();

    public TotalDiskImpl(Node node) {
      super(node, SolrClientNodeStateProvider.Variable.TOTALDISK.tagName);
    }

    @Override
    void setEmpty() {
      totalDiskSize = Optional.empty();
    }

    @Override
    void setValue(final Object nodeValue) {
      totalDiskSize = Optional.of(((Number) nodeValue).longValue());
    }

    @Override
    public Optional<Long> getTotalSizeGB() {
      return totalDiskSize;
    }
  }


  static class FreeDiskImpl extends AbstractNodePropertyKey implements PropertyKey.FreeDisk {
    Optional<Long> freeDiskSize = Optional.empty();

    public FreeDiskImpl(Node node) {
      super(node, SolrClientNodeStateProvider.Variable.FREEDISK.tagName);
    }

    @Override
    void setEmpty() {
      freeDiskSize = Optional.empty();
    }

    @Override
    void setValue(final Object nodeValue) {
      freeDiskSize = Optional.of(((Number) nodeValue).longValue());
    }

    @Override
    public Optional<Long> getFreeSizeGB() {
      return freeDiskSize;
    }
  }


  static class HeapUsageImpl extends AbstractNodePropertyKey implements HeapUsage {
    Optional<Double> heapUsage = Optional.empty();

    public HeapUsageImpl(Node node) {
      super(node, ImplicitSnitch.HEAPUSAGE);
    }

    @Override
    void setEmpty() {
      heapUsage = Optional.empty();
    }

    @Override
    void setValue(Object nodeValue) {
      heapUsage = Optional.of(((Number) nodeValue).doubleValue());
    }

    @Override
    public Optional<Double> getUsedHeapMemoryUsage() {
      return heapUsage;
    }
  }

  static class NodeMetricImpl extends AbstractNodePropertyKey implements Metric {
    Optional<Double> metricValue = Optional.empty();

    public NodeMetricImpl(Node nodeMetricSource, String metricName, PropertyKeyFactory.NodeMetricRegistry registry) {
      super(nodeMetricSource, SolrClientNodeStateProvider.METRICS_PREFIX + SolrMetricManager.getRegistryName(getGroupFromRegistry(registry), metricName));
    }

    private static SolrInfoBean.Group getGroupFromRegistry(PropertyKeyFactory.NodeMetricRegistry registry) {
      switch (registry) {
        case SOLR_JVM:
          return SolrInfoBean.Group.jvm;
        case SOLR_NODE:
          return SolrInfoBean.Group.node;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported registry value " + registry);
      }
    }

    @Override
    void setEmpty() {
      metricValue = Optional.empty();
    }

    @Override
    void setValue(final Object nodeValue) {
      metricValue = Optional.of(((Number) nodeValue).doubleValue());
    }

    @Override
    public Optional<Double> getNumberValue() {
      return metricValue;
    }
  }

  static class SyspropImpl extends AbstractNodePropertyKey implements Sysprop {
    Optional<String> systemProperty = Optional.empty();
    public SyspropImpl(Node node, String syspropName) {
      super(node, ImplicitSnitch.SYSPROP + syspropName);
    }

    @Override
    void setEmpty() {
      systemProperty = Optional.empty();
    }

    @Override
    void setValue(final Object nodeValue) {
      systemProperty = Optional.of((String) nodeValue);
    }

    @Override
    public Optional<String> getSystemPropertyValue() {
      return systemProperty;
    }
  }

  static class SystemLoadImpl extends AbstractNodePropertyKey implements SystemLoad {
    Optional<Double> systemLoadAverage = Optional.empty();

    public SystemLoadImpl(Node node) {
      super(node, ImplicitSnitch.SYSLOADAVG);
    }

    @Override
    void setEmpty() {
      systemLoadAverage = Optional.empty();
    }

    @Override
    void setValue(final Object nodeValue) {
      systemLoadAverage = Optional.of(((Number) nodeValue).doubleValue());
    }

    @Override
    public Optional<Double> getSystemLoadAverage() {
      return systemLoadAverage;
    }
  }
}

/**
 * This class (as its name implies) is not a {@link Node} metric. Therefore it doesn't extend {@link AbstractNodePropertyKey}.
 */
class NonNodeMetricKeyImpl implements PropertyKey.Metric {
  private final String metricName;
  private final PropertyValueSource metricSource;

  public NonNodeMetricKeyImpl(PropertyValueSource metricSource, String metricName) {
    Preconditions.checkState(!(metricSource instanceof Node), "Illegal argument type " + Node.class);
    this.metricSource = metricSource;
    this.metricName = metricName;
  }

  @Override
  public Optional<Double> getNumberValue() {
    throw new UnsupportedOperationException("Non node metrics not yet implemented");
  }
}
