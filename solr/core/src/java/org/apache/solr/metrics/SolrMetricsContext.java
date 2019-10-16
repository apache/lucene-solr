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

public class SolrMetricsContext {
  public final String registry;
  public final SolrMetricManager metricManager;
  public final String tag;

  public SolrMetricsContext(SolrMetricManager metricManager, String registry, String tag) {
    this.registry = registry;
    this.metricManager = metricManager;
    this.tag = tag;
  }

  public String getTag() {
    return tag;
  }

  public void unregister() {
    metricManager.unregisterGauges(registry, tag);
  }

  public SolrMetricsContext getChildContext(Object child) {
    SolrMetricsContext childContext = new SolrMetricsContext(metricManager, registry, SolrMetricProducer.getUniqueMetricTag(child, tag));
    return childContext;
  }

  public Meter meter(SolrInfoBean info, String metricName, String... metricpath) {
    return metricManager.meter(info, registry, metricName, metricpath);
  }

  public Counter counter(SolrInfoBean info, String metricName, String... metricpath) {
    return metricManager.counter(info, registry, metricName, metricpath);

  }

  public void gauge(SolrInfoBean info, Gauge<?> gauge, boolean force, String metricName, String... metricpath) {
    metricManager.registerGauge(info, registry, gauge, tag, force, metricName, metricpath);
  }

  public Timer timer(SolrInfoBean info, String metricName, String... metricpath) {
    return metricManager.timer(info, registry, metricName, metricpath);

  }

  public MetricRegistry getMetricRegistry() {
    return metricManager.registry(registry);
  }
}
