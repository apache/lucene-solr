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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.solr.core.SolrInfoBean;

public class SolrDelegateRegistryMetricsContext extends SolrMetricsContext {

  private final String delegateRegistry;

  public SolrDelegateRegistryMetricsContext(SolrMetricManager metricManager, String registry, String tag, String delegateRegistry) {
    super(metricManager, registry, tag);
    this.delegateRegistry = delegateRegistry;
  }

  @Override
  public Meter meter(SolrInfoBean info, String metricName, String... metricPath) {
    return new DelegateRegistryMeter(super.meter(info, metricName, metricPath), metricManager.meter(info, delegateRegistry, metricName, metricPath));
  }

  @Override
  public Counter counter(SolrInfoBean info, String metricName, String... metricPath) {
    return new DelegateRegistryCounter(super.counter(info, metricName, metricPath), metricManager.counter(info, delegateRegistry, metricName, metricPath));
  }

  @Override
  public Timer timer(SolrInfoBean info, String metricName, String... metricPath) {
    return new DelegateRegistryTimer(MetricSuppliers.getClock(metricManager.getMetricsConfig().getTimerSupplier(), MetricSuppliers.CLOCK), super.timer(info, metricName, metricPath), metricManager.timer(info, delegateRegistry, metricName, metricPath));
  }

  @Override
  public Histogram histogram(SolrInfoBean info, String metricName, String... metricPath) {
    return new DelegateRegistryHistogram(super.histogram(info, metricName, metricPath), metricManager.histogram(info, delegateRegistry, metricName, metricPath));
  }

  @Override
  public SolrMetricsContext getChildContext(Object child) {
    return new SolrDelegateRegistryMetricsContext(metricManager, registry, SolrMetricProducer.getUniqueMetricTag(child, tag), delegateRegistry);
  }
}
