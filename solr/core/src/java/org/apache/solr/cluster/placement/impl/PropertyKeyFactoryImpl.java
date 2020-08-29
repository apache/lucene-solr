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
import org.apache.solr.cluster.placement.PropertyKeyFactory;
import org.apache.solr.cluster.placement.PropertyValueSource;

public class PropertyKeyFactoryImpl implements PropertyKeyFactory {
  @Override
  public CoreCountKeyImpl createCoreCountKey(Node node) {
    return new CoreCountKeyImpl(node);
  }

  @Override
  public FreeDiskKeyImpl createFreeDiskKey(Node node) {
    return new FreeDiskKeyImpl(node);
  }

  @Override
  public TotalDiskKeyImpl createTotalDiskKey(Node node) {
    return new TotalDiskKeyImpl(node);
  }

  @Override
  public DiskTypeKeyImpl createDiskTypeKey(Node node) {
    return new DiskTypeKeyImpl(node);
  }

  @Override
  public SyspropKeyImpl createSyspropKey(Node node, String syspropName) {
    return new SyspropKeyImpl(node, syspropName);
  }

  @Override
  public NonNodeMetricKeyImpl createMetricKey(PropertyValueSource metricSource, String metricName) {
    return new NonNodeMetricKeyImpl(metricSource, metricName);
  }

  @Override
  public NodeMetricKeyImpl createMetricKey(Node nodeMetricSource, String metricName, NodeMetricRegistry registry) {
    return new NodeMetricKeyImpl(nodeMetricSource, metricName, registry);
  }

  @Override
  public SystemLoadKeyImpl createSystemLoadKey(Node node) {
    return new SystemLoadKeyImpl(node);
  }

  @Override
  public HeapUsageKeyImpl createHeapUsageKey(Node node) {
    return new HeapUsageKeyImpl(node);
  }
}
