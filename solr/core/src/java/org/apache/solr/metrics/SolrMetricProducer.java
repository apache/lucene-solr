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

import java.io.IOException;

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
   * Initializes metrics specific to this producer
   *
   * @param manager  an instance of {@link SolrMetricManager}
   * @param registry registry name where metrics are registered
   * @param tag      a symbolic tag that represents this instance of the producer,
   *                 or a group of related instances that have the same life-cycle. This tag is
   *                 used when managing life-cycle of some metrics.
   * @param scope    scope of the metrics (eg. handler name) to separate metrics of components with
   *                 the same implementation but different scope.
   * @deprecated use {@link #initializeMetrics(SolrMetricsContext, String)} instead
   */
  @Deprecated
  default void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    initializeMetrics(new SolrMetricsContext(manager, registry, tag), scope);

  }

  /**
   * Initialize metrics specific to this producer.
   * @param parentContext parent metrics context. If this component has the same life-cycle as the parent
   *                it can simply use the parent context, otherwise it should obtain a child context
   *                using {@link SolrMetricsContext#getChildContext(Object)} passing <code>this</code>
   *                as the child.
   * @param scope component scope
   */
  default void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    throw new RuntimeException("In class " + getClass().getName() +
        " you must implement either initializeMetrics(SolrMetricsContext, String) or " +
        "initializeMetrics(SolrMetricManager, String, String, String)");

  }

  /**
   * Implementing classes should override this method to provide the context obtained in
   * {@link #initializeMetrics(SolrMetricsContext, String)} to ensure proper cleanup of metrics
   * at the end of the life-cycle of this component.
   */
  default SolrMetricsContext getSolrMetricsContext() {
    return null;
  }

  /**
   * Implementations should always call <code>SolrMetricProducer.super.close()</code> to ensure that
   * metrics with the same life-cycle as this component are properly unregistered. This prevents
   * obscure memory leaks.
   *
   * from: https://docs.oracle.com/javase/8/docs/api/java/lang/AutoCloseable.html
   * While this interface method is declared to throw Exception, implementers are strongly encouraged
   * to declare concrete implementations of the close method to throw more specific exceptions, or to
   * throw no exception at all if the close operation cannot fail.
   */
  @Override
  default void close() throws IOException {
    SolrMetricsContext context = getSolrMetricsContext();
    if (context == null) {
      return;
    } else {
      context.unregister();
    }
    // ??? (ab) no idea what this was supposed to avoid
    //if (info == null || info.tag.indexOf(':') == -1) return;//this will end up unregistering the root itself
  }
}
