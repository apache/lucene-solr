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
 * Node metric identifier, corresponding
 * to a node-level metric registry and the internal metric name.
 */
public interface NodeMetric<T> extends Metric<T> {

  /**
   * Metric registry. If this metric identifier uses a fully-qualified
   * metric key instead, then this method will return {@link Registry#UNSPECIFIED}.
   */
  Registry getRegistry();

  /**
   * Registry options for node metrics.
   */
  enum Registry {
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
    SOLR_JETTY,
    /**
     * In case when the registry name is not relevant (eg. a fully-qualified
     * metric key was provided as the metric name).
     */
    UNSPECIFIED
  }
}
