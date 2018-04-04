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
package org.apache.solr.metrics;

/**
 * Used by objects that expose metrics through {@link SolrMetricManager}.
 */
public interface SolrMetricProducer {

  /**
   * Initializes metrics specific to this producer.
   * <p>Note: for back-compatibility this method by default calls {@link #initializeMetrics(SolrMetricManager, String, String)}.</p>
   * @param manager an instance of {@link SolrMetricManager}
   * @param registry registry name where metrics are registered
   * @param tag symbolic tag that represents a group of related instances that
   * have the same life-cycle. Parent component can use the <code>tag</code> when
   * calling {@link SolrMetricManager#unregisterGauges(String, String)}
   * to unregister metrics created by this instance of the producer.
   * @param scope scope of the metrics (eg. handler name) to separate metrics of
   *              instances of the same component executing in different contexts
   */
  default void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    initializeMetrics(manager, registry, scope);
  }

  /**
   * Initializes metrics specific to this producer.
   * <p>Note: for back-compatibility this method has a default no-op implementation.</p>
   * @param manager an instance of {@link SolrMetricManager}
   * @param registry registry name where metrics are registered
   * @param scope scope of the metrics (eg. handler name) to separate metrics of
   *              instances of the same component executing in different contexts
   * @deprecated this method doesn't provide enough context to properly manage
   * life-cycle of some metrics (see SOLR-11882).
   * Instead use {@link #initializeMetrics(SolrMetricManager, String, String, String)}.
   */
  @Deprecated
  default void initializeMetrics(SolrMetricManager manager, String registry, String scope) {

  }
}
