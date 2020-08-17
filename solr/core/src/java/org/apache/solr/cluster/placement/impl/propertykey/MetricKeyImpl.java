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

package org.apache.solr.cluster.placement.impl.propertykey;

import com.google.common.base.Preconditions;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyKeyFactory;
import org.apache.solr.cluster.placement.PropertyValueSource;

public class MetricKeyImpl extends AbstractPropertyKey implements PropertyKey {
  private final String metricName;
  private final PropertyKeyFactory.NodeMetricRegistry registry; // non null when propertyValueSource is a Node

  public MetricKeyImpl(PropertyValueSource metricSource, String metricName) {
    super(metricSource);
    Preconditions.checkState(!(metricSource instanceof Node), "Illegal argument type " + Node.class);
    this.metricName = metricName;
    this.registry = null; // When propertyValueSource (see superclass) is not a Node, registry is not used.
  }

  public MetricKeyImpl(Node nodeMetricSource, String metricName, PropertyKeyFactory.NodeMetricRegistry registry) {
    super(nodeMetricSource);
    // Node based metrics must specify a registry
    Preconditions.checkState(registry != null, "Node registry can't be null");
    this.metricName = metricName;
    this.registry = registry;
  }
}
