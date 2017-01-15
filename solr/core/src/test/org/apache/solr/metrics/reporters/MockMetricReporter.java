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
package org.apache.solr.metrics.reporters;

import java.io.IOException;
import java.util.Locale;
import java.util.NoSuchElementException;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;

public class MockMetricReporter extends SolrMetricReporter {

  public String configurable;

  public boolean didInit = false;
  public boolean didClose = false;
  public boolean didValidate = false;

  public MockMetricReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  @Override
  public void init(PluginInfo pluginInfo) {
    super.init(pluginInfo);
    didInit = true;
  }

  @Override
  public void close() throws IOException {
    didClose = true;
  }

  @Override
  protected void validate() throws IllegalStateException {
    didValidate = true;
    if (configurable == null) {
      throw new IllegalStateException("MockMetricReporter::configurable not defined.");
    }
  }

  public void setConfigurable(String configurable) {
    this.configurable = configurable;
  }

  public Metric reportMetric(String metricName) throws NoSuchElementException {
    MetricRegistry registry = metricManager.registry(registryName);
    Metric metric = registry.getMetrics().get(metricName);
    if (metric == null) {
      throw new NoSuchElementException("Metric was not found for metric name = " + metricName);
    }

    return metric;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "[%s@%s: configurable = %s, didInit = %b, didValidate = %b, didClose = %b]",
        getClass().getName(), Integer.toHexString(hashCode()), configurable, didInit, didValidate, didClose);

  }
}
