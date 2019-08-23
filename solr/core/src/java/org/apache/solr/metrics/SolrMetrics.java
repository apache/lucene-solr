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

public class SolrMetrics {
  public final String registry;
  public final SolrMetricManager metricManager;
  public final String tag;
  public final String scope;
  private SolrMetrics parent;

  public SolrMetrics(SolrMetricManager metricManager, String registry, String tag, String scope) {
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

  public SolrMetrics getChildInfo(SolrMetricProducer producer) {
    SolrMetrics metricsInfo = new SolrMetrics(metricManager, registry, producer.getUniqueMetricTag(tag), scope);
    metricsInfo.parent = this;
    return metricsInfo;
  }

  public Meter meter(SolrInfoBean info, String metricName, String... metricpath) {
    return metricManager.meter(info, getRegistry(), createName(metricName, metricpath));
  }

  private String createName(String metricName, String... metricpath) {
    ArrayList<String> l = new ArrayList<>();
    if (metricpath != null) {
      Collections.addAll(l, metricpath);
    }
    l.add(scope);
    return makeName(l, metricName);
  }

  public Counter counter(SolrInfoBean info, String metricName, String... metricpath) {
    return metricManager.counter(info, getRegistry(), createName(metricName, metricpath));

  }

  public void gauge(SolrInfoBean info, Gauge<?> gauge, boolean force, String metricName, String... metricpath) {
    String name = metricpath == null || metricpath.length == 0 ? metricName : createName(metricName, metricpath);
    metricManager.registerGauge(info, getRegistry(), new SolrMetricManager.GaugeWrapper<>(gauge, tag), force, name);
  }

  public Timer timer(SolrInfoBean info, String metricName, String... metricpath) {
    return metricManager.timer(info, getRegistry(), createName(metricName, metricpath));

  }

  public SolrMetrics getParent() {
    return parent;
  }

  public MetricRegistry getRegistry() {
    return metricManager.registry(registry);
  }
}
