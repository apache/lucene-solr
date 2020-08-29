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

import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PropertyKeyFactory;
import org.apache.solr.cluster.placement.PropertyValue;
import org.apache.solr.cluster.placement.impl.propertyvalue.MetricPropertyValueImpl;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;

public class NodeMetricKeyImpl extends AbstractNodePropertyKey {

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
