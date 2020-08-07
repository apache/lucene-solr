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

/**
 * Factory used by the plugin to create property keys to request property values from Solr.<p>
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
  PropertyKey createCoreCountKey(Node node);

  /**
   * Returns a property key to request disk related info on a {@link Node}.
   */
  PropertyKey createDiskInfoKey(Node node);

  /**
   * Returns a property key to request the value of a sysprop (a.k.a. system property) on a {@link Node}.
   * A system property can be passed to {@code java} using the {@code -DpropertyName=value} parameter.
   * @param syspropName the name of the system property to retrieve.
   */
  PropertyKey createSyspropKey(Node node, String syspropName);

  /**
   * Returns a property key to request the value of an environment variable of the OS where a {@link Node} runs.
   * An environment variable is for example {@code "HOME"} and its value could be {@code "/Users/johndoe"}.
   * @param envVarName the name of the environment variable to retrieve.
   */
  PropertyKey createEnvvarKey(Node node, String envVarName);

  /**
   * <p>Returns a property key to request the value of a metric.
   *
   * <p>Not all metrics make sense everywhere, but metrics can be applied to different objects. For example
   * <code>SEARCHER.searcher.indexCommitSize</code> would make sense for a given replica of a given shard of a given collection,
   * and possibly in other contexts.
   *
   * <p>The {@link PropertyValue} instances corresponding to this metric can be of types:
   * <ul>
   *   <li>{@link NumberMetricPropertyValue},</li>
   *   <li>{@link StringMetricPropertyValue},</li>
   *   <li>And possibly more complex types (maps?) in the future...</li>
   * </ul>
   *
   * <p>TODO: implementation node (to remove) SolrInfoBean.Group can be inferred from metricSource representing replica, shard, collection.
   *
   * @param metricSource The registry of the metric. For example a specific {@link Replica}.
   *                     <p>This method <b>does not</b> accept
   *                     {@link Node} as a metric source. Please use {@link #createMetricKey(Node, String, NodeMetricRegistry)}
   *                     for {@link Node} metrics.
   * @param metricName for example <code>SEARCHER.searcher.indexCommitSize</code>.
   */
  PropertyKey createMetricKey(PropertyValueSource metricSource, String metricName);

  /**
   * Returns a property key to request the value of a {@link Node} metric. Corresponding {@link PropertyValue}'s are similar
   * to those of {@link #createMetricKey(PropertyValueSource, String)}.
   */
   PropertyKey createMetricKey(Node metricSource, String metricName, NodeMetricRegistry registry);

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
  PropertyKey createSystemLoadKey(Node node);

}
