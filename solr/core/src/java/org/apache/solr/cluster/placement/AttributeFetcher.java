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

import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.SolrCollection;

import java.util.Set;

/**
 * <p>Instances of this interface are used to fetch various attributes from nodes (and other sources) in the cluster.</p>
 */
public interface AttributeFetcher {
  /**
   * Request the number of cores on each node. To get the value use {@link AttributeValues#getCoresCount(Node)}
   */
  default AttributeFetcher requestNodeCoresCount() {
    return requestNodeMetric(NodeMetric.NUM_CORES);
  }

  /**
   * Request the free disk size on each node. To get the value use {@link AttributeValues#getFreeDisk(Node)}
   */
  default AttributeFetcher requestNodeFreeDisk() {
    return requestNodeMetric(NodeMetric.FREE_DISK_GB);
  }

  /**
   * Request the total disk size on each node. To get the value use {@link AttributeValues#getTotalDisk(Node)}
   */
  default AttributeFetcher requestNodeTotalDisk() {
    return requestNodeMetric(NodeMetric.TOTAL_DISK_GB);
  }

  /**
   * Request the heap usage on each node. To get the value use {@link AttributeValues#getHeapUsage(Node)}
   */
  default AttributeFetcher requestNodeHeapUsage() {
    return requestNodeMetric(NodeMetric.HEAP_USAGE);
  }

  /**
   * Request the system load average on each node. To get the value use {@link AttributeValues#getSystemLoadAverage(Node)}
   */
  default AttributeFetcher requestNodeSystemLoadAverage() {
    return requestNodeMetric(NodeMetric.SYSLOAD_AVG);
  }

  /**
   * Request a given system property on each node. To get the value use {@link AttributeValues#getSystemProperty(Node, String)}
   * @param name system property name
   */
  AttributeFetcher requestNodeSystemProperty(String name);

  /**
   * Request an environment variable on each node. To get the value use {@link AttributeValues#getEnvironmentVariable(Node, String)}
   * @param name environment property name
   */
  AttributeFetcher requestNodeEnvironmentVariable(String name);

  /**
   * Request a node metric from each node. To get the value use {@link AttributeValues#getNodeMetric(Node, NodeMetric)}
   * @param metric metric to retrieve (see {@link NodeMetric})
   */
  AttributeFetcher requestNodeMetric(NodeMetric<?> metric);

  /**
   * Rfrom any metric registry on each node, using a fully-qualified metric key,
   * for example <code>solr.jvm:system.properties:user.name</code>.
   * To get the value use {@link AttributeValues#getNodeMetric(Node, String)}
   * @param metricKey fully-qualified metric key
   */
  default AttributeFetcher requestNodeMetric(String metricKey) {
    return requestNodeMetric(new NodeMetric<>(metricKey));
  }

  /**
   * Request collection-level metrics. To get the values use {@link AttributeValues#getCollectionMetrics(String)}.
   * Note that this request will fetch information from nodes relevant to the collection
   * replicas and not the ones specified in {@link #fetchFrom(Set)} (though they may overlap).
   * @param solrCollection request metrics for this collection
   * @param metrics metrics to retrieve (see {@link ReplicaMetric})
   */
  AttributeFetcher requestCollectionMetrics(SolrCollection solrCollection, Set<ReplicaMetric<?>> metrics);

  /**
   * The set of nodes from which to fetch all node related attributes. Calling this method is mandatory if any of the {@code requestNode*}
   * methods got called.
   * @param nodes nodes to fetch from
   */
  AttributeFetcher fetchFrom(Set<Node> nodes);

  /**
   * Fetches all requested node attributes from all nodes passed to {@link #fetchFrom(Set)} as well as non node attributes
   * (those requested for example using {@link #requestNodeMetric(String)}.
   *
   * @return An instance allowing retrieval of all attributed that could be fetched.
   */
  AttributeValues fetchAttributes();

  /**
   * Registry options for {@link Node} metrics.
   */
  enum NodeMetricRegistry {
    /**
     * corresponds to solr.node
     */
    SOLR_NODE,
    /**
     * corresponds to solr.jvm
     */
    SOLR_JVM,
    /**
     * corresponds to solr.jetty
     */
    SOLR_JETTY
  }
}
