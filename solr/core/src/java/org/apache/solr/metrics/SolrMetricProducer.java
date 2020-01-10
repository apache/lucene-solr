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
public interface SolrMetricProducer extends AutoCloseable {

  /**
   * Unique metric tag identifies components with the same life-cycle, which should
   * be registered / unregistered together. It is in the format of A:B:C, where
   * A is the parent of B is the parent of C and so on.
   * If object "B" is unregistered C also must get unregistered.
   * If object "A" is unregistered B and C also must get unregistered.
   * @param o object to create a tag for
   * @param parentName parent object name, or null if no parent exists
   */
  static String getUniqueMetricTag(Object o, String parentName) {
    String name = o.getClass().getSimpleName() + "@" + Integer.toHexString(o.hashCode());
    if (parentName != null && parentName.contains(name)) {
      throw new RuntimeException("Parent already includes this component! parent=" + parentName + ", this=" + name);
    }
    return parentName == null ?
        name :
        parentName + ":" + name;
  }

  /**
   * Initialize metrics specific to this producer.
   * @param parentContext parent metrics context. If this component has the same life-cycle as the parent
   *                it can simply use the parent context, otherwise it should obtain a child context
   *                using {@link SolrMetricsContext#getChildContext(Object)} passing <code>this</code>
   *                as the child object.
   * @param scope component scope
   */
  void initializeMetrics(SolrMetricsContext parentContext, String scope);

  /**
   * Implementations should return the context used in
   * {@link #initializeMetrics(SolrMetricsContext, String)} to ensure proper cleanup of metrics
   * at the end of the life-cycle of this component. This should be the child context if one was created,
   * or null if the parent context was used.
   */
  SolrMetricsContext getSolrMetricsContext();

  /**
   * Implementations should always call <code>SolrMetricProducer.super.close()</code> to ensure that
   * metrics with the same life-cycle as this component are properly unregistered. This prevents
   * obscure memory leaks.
   */
  @Override
  default void close() throws Exception {
    SolrMetricsContext context = getSolrMetricsContext();
    if (context == null) {
      return;
    } else {
      context.unregister();
    }
  }
}
