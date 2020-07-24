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

package org.apache.solr.cloud.gumi;

/**
 * Factory used by the plugin to create property keys to request property values from Solr.<p>
 *
 * Note there currently exist sub-interfaces of {@link PropertyKey} for all the keys. This is for initial readability,
 * but is not required. Some of the interfaces are empty, and even those that return the values used to build the
 * keys are not really required, the plugin code can keep track in other ways of what is what.
 * Leaving it this way for now but very tempted to have all the factory methods simply return a {@link PropertyKey} instead.<p>
 *
 * Building of a {@link PropertyKey} requires specifying the target (context) from which the value of that key should be
 * obtained. This is done by specifying the appropriate {@link PropertyKeyTarget}.<br>
 * For clarity, when only a single type of target is acceptable, the corresponding subtype of {@link PropertyKeyTarget} is used instead
 * (for example {@link Node}).
 */
public interface PropertyKeyFactory {
  /**
   * Returns a property key to request the number of cores on a {@link Node}.
   */
  CoresCountPropertyKey createCoreCountKey(Node node);

  /**
   * Returns a property key to request disk related info on a {@link Node}.
   */
  DiskInfoPropertyKey createDiskInfoKey(Node node);

  /**
   * Returns a property key to request the value of a system property on a {@link Node}.
   * @param systemPropertyName the name of the system property to retrieve.
   */
  SystemPropertyPropertyKey createSystemPropertyKey(Node node, String systemPropertyName);

  /**
   * Returns a property key to request the value of a metric.<p>
   *
   * Not all metrics make sense everywhere, but metrics can be applied to different objects. For example
   * <code>SEARCHER.searcher.indexCommitSize</code> would make sense for a given replica of a given shard of a given collection,
   * and possibly in other contexts.<p>
   *
   * @param metricSource The registry of the metric. For example a specific {@link Replica}.
   * @param metricName for example <code>SEARCHER.searcher.indexCommitSize</code>.
   */
  MetricPropertyKey createMetricKey(PropertyKeyTarget metricSource, String metricName);

  /**
   * Returns a property key to access system load data on a {@link Node}.
   */
  SystemLoadPropertyKey createSystemLoadKey(Node node);
}
