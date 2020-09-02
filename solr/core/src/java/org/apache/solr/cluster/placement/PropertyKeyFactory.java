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

package org.apache.solr.cluster.placement;

import java.util.Map;
import java.util.Set;

/**
 * Factory used by the plugin to create property keys to request fetching data from Solr.<p>
 *
 * Building of a {@link PropertyKey} requires specifying the target (context) from which the value of that key should be
 * obtained. This is done by specifying the appropriate {@link PropertyValueSource}.<br>
 * For clarity, when only a single type of target is acceptable, the corresponding subtype of {@link PropertyValueSource} is used instead
 * (for example {@link Node}).
 */
public interface PropertyKeyFactory {
  /**
   * Returns a property key to request the number of cores on a {@link Node}.
   */
  PropertyKey.CoresCount createCoreCountKey(Node node);

  /**
   * Returns a property key to query the disk type on a {@link Node}.
   */
  PropertyKey.DiskType createDiskTypeKey(Node node);

  /**
   * Returns a property key to query the disk free size on a {@link Node}.
   */
  PropertyKey.FreeDisk createFreeDiskKey(Node node);

  /**
   * Returns a property key to query the total disk sizes on a {@link Node}.
   */
  PropertyKey.TotalDisk createTotalDiskKey(Node node);

  /**
   * Returns a property key to access heap usage data on a {@link Node}.
   */
  PropertyKey.HeapUsage createHeapUsageKey(Node node);


  /**
   * Returns a property key to request the value of a sysprop (a.k.a. system property) on a {@link Node}.
   * A system property can be passed to {@code java} using the {@code -DpropertyName=value} parameter.
   * @param syspropName the name of the system property to retrieve.
   */
  PropertyKey.Sysprop createSyspropKey(Node node, String syspropName);

  /**
   * Calls {@link #createSyspropKey} for all nodes and returns a map from each node to corresponding {@link PropertyKey}.
   */
  Map<Node, PropertyKey> createSyspropKeys(Set<Node> nodes, String syspropName);

  /**
   * Returns a property key to get a {@link Node} metric.
   */
  PropertyKey.Metric createMetricKey(Node nodeMetricSource, String metricName, NodeMetricRegistry registry);

  /**
   * Registry options when requesting a {@link Node} metric using {@link #createMetricKey(Node, String, NodeMetricRegistry)}.
   */
  enum NodeMetricRegistry {
    SOLR_NODE, // corresponds to solr.node
    SOLR_JVM; // corresponds to solr.jvm
  }

  /**
   * Returns a property key to access system load data on a {@link Node}.
   */
  PropertyKey.SystemLoad createSystemLoadKey(Node node);


  /**
   * <p>Returns a property key to request the value of a metric that is not {@link Node} related (for {@link Node} metrics
   * use {@link #createMetricKey(Node, String, NodeMetricRegistry)}.
   *
   * <p>Not all metrics make sense everywhere, but metrics can be applied to different objects. For example
   * <code>SEARCHER.searcher.indexCommitSize</code> would make sense for a given replica of a given shard of a given collection,
   * and possibly in other contexts.
   *
   * <p>TODO: implementation note (to remove) SolrInfoBean.Group can be inferred from metricSource representing replica, shard, collection.
   *
   * @param metricSource The registry of the metric. For example a specific {@link Replica}.
   *                     <p>This method <b>does not</b> accept
   *                     {@link Node} as a metric source. Please use {@link #createMetricKey(Node, String, NodeMetricRegistry)}
   *                     for {@link Node} metrics.
   * @param metricName for example <code>SEARCHER.searcher.indexCommitSize</code>.
   */
  PropertyKey.Metric createMetricKey(PropertyValueSource metricSource, String metricName);
}
