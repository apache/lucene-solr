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

import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyKeyFactory;
import org.apache.solr.cluster.placement.PropertyValueSource;
import org.apache.solr.cluster.placement.impl.propertykey.CoreCountKeyImpl;
import org.apache.solr.cluster.placement.impl.propertykey.DiskInfoKeyImpl;
import org.apache.solr.cluster.placement.impl.propertykey.EnvvarKeyImpl;
import org.apache.solr.cluster.placement.impl.propertykey.MetricKeyImpl;
import org.apache.solr.cluster.placement.impl.propertykey.SyspropKeyImpl;
import org.apache.solr.cluster.placement.impl.propertykey.SystemLoadKeyImpl;

public class PropertyKeyFactoryImpl implements PropertyKeyFactory {
  @Override
  public PropertyKey createCoreCountKey(Node node) {
    return new CoreCountKeyImpl(node);
  }

  @Override
  public PropertyKey createDiskInfoKey(Node node) {
    return new DiskInfoKeyImpl(node);
  }

  @Override
  public PropertyKey createSyspropKey(Node node, String syspropName) {
    return new SyspropKeyImpl(node, syspropName);
  }

  @Override
  public PropertyKey createEnvvarKey(Node node, String envVarName) {
    return new EnvvarKeyImpl(node, envVarName);
  }

  @Override
  public PropertyKey createMetricKey(PropertyValueSource metricSource, String metricName) {
    return new MetricKeyImpl(metricSource, metricName);
  }

  @Override
  public PropertyKey createMetricKey(Node nodeMetricSource, String metricName, NodeMetricRegistry registry) {
    return new MetricKeyImpl(nodeMetricSource, metricName, registry);
  }

  @Override
  public PropertyKey createSystemLoadKey(Node node) {
    return new SystemLoadKeyImpl(node);
  }
}
