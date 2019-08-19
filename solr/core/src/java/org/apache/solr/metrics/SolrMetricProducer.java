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

import java.util.ArrayList;
import java.util.Collections;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.core.SolrInfoBean;

import static org.apache.solr.metrics.SolrMetricManager.makeName;

/**
 * Used by objects that expose metrics through {@link SolrCoreMetricManager}.
 */
public interface SolrMetricProducer extends AutoCloseable {

  /**
   * Unique metric name is in the format of A.B.C
   * A is the parent of B is the parent of C and so on.
   * If object "B" is unregistered , C also must get unregistered.
   * If object "A" is unregistered ,  B , C also must get unregistered.
   */
  default String getUniqueMetricTag(String parentName) {
    String name = getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
    if (parentName != null && parentName.contains(name)) return parentName;
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
   *                 used when managing life-cycle of some metrics and is set when
   *                 {@link #initializeMetrics(SolrMetricManager, String, String, String)} is called.
   * @param scope    scope of the metrics (eg. handler name) to separate metrics of
   */
  default void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    initializeMetrics(new MetricsInfo(manager, registry, tag, scope));

  }

  default void initializeMetrics(MetricsInfo info) {
    throw new RuntimeException("This means , the class has not implemented both of these methods");

  }

  default MetricsInfo getMetricsInfo() {
    return null;
  }

  @Override
  default void close() throws Exception {
    MetricsInfo info = getMetricsInfo();
    if (info == null || info.tag.indexOf(':') == -1) return;//this will end up unregistering the root itself
    info.unregister();
  }

  class MetricsInfo {
    public final String registry;
    public final SolrMetricManager metricManager;
    public final String tag;
    public final String scope;
    private MetricsInfo parent;

    public MetricsInfo(SolrMetricManager metricManager, String registry, String tag, String scope) {
      this.registry = registry;
      this.metricManager = metricManager;
      this.tag = tag;
      this.scope = scope;
    }

    public String getTag() {
      return tag;
    }

    public void unregister() {
      metricManager.unregisterGauges(registry, tag);
    }

    public MetricsInfo getChildInfo(SolrMetricProducer producer) {
      MetricsInfo metricsInfo = new MetricsInfo(metricManager, registry, producer.getUniqueMetricTag(tag), scope);
      metricsInfo.parent = this;
      return metricsInfo;
    }

    public Meter meter(SolrInfoBean info, String metricName, String... metricpath) {
      return metricManager.meter(info, getRegistry(), createName(metricName, metricpath));
    }

    private String createName(String metricName, String... metricpath) {
      ArrayList<String> l = new ArrayList<>();
      if(metricpath != null ) {
        Collections.addAll(l, metricpath);
      }
      l.add(scope);
      return makeName(l, metricName);
    }

    public Counter counter(SolrInfoBean info, String metricName, String... metricpath) {
      return metricManager.counter(info, getRegistry(), createName(metricName, metricpath));

    }

    public void gauge(SolrInfoBean info, Gauge<?> gauge, boolean force, String metricName, String... metricpath) {
      metricManager.registerGauge(info, getRegistry(), new SolrMetricManager.GaugeWrapper<>(gauge, tag), force, createName(metricName, metricpath));
    }

    public Timer timer(SolrInfoBean info, String metricName, String... metricpath) {
      return metricManager.timer(info, getRegistry(), createName(metricName, metricpath));

    }

    public MetricsInfo getParent() {
      return parent;
    }

    public MetricRegistry getRegistry() {
      return metricManager.registry(registry);
    }
  }
}
