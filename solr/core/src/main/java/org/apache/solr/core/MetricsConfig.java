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
package org.apache.solr.core;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class MetricsConfig {

  private final PluginInfo[] metricReporters;
  private final Set<String> hiddenSysProps;
  private final PluginInfo counterSupplier;
  private final PluginInfo meterSupplier;
  private final PluginInfo timerSupplier;
  private final PluginInfo histogramSupplier;
  private final PluginInfo historyHandler;

  private MetricsConfig(PluginInfo[] metricReporters, Set<String> hiddenSysProps,
                        PluginInfo counterSupplier, PluginInfo meterSupplier,
                        PluginInfo timerSupplier, PluginInfo histogramSupplier,
                        PluginInfo historyHandler) {
    this.metricReporters = metricReporters;
    this.hiddenSysProps = hiddenSysProps;
    this.counterSupplier = counterSupplier;
    this.meterSupplier = meterSupplier;
    this.timerSupplier = timerSupplier;
    this.histogramSupplier = histogramSupplier;
    this.historyHandler = historyHandler;
  }

  public PluginInfo[] getMetricReporters() {
    return metricReporters;
  }

  public Set<String> getHiddenSysProps() {
    return hiddenSysProps;
  }

  public PluginInfo getCounterSupplier() {
    return counterSupplier;
  }

  public PluginInfo getMeterSupplier() {
    return meterSupplier;
  }

  public PluginInfo getTimerSupplier() {
    return timerSupplier;
  }

  public PluginInfo getHistogramSupplier() {
    return histogramSupplier;
  }

  public PluginInfo getHistoryHandler() {
    return historyHandler;
  }

  public static class MetricsConfigBuilder {
    private PluginInfo[] metricReporterPlugins = new PluginInfo[0];
    private Set<String> hiddenSysProps = new HashSet<>();
    private PluginInfo counterSupplier;
    private PluginInfo meterSupplier;
    private PluginInfo timerSupplier;
    private PluginInfo histogramSupplier;
    private PluginInfo historyHandler;

    public MetricsConfigBuilder() {

    }

    public MetricsConfigBuilder setHiddenSysProps(Set<String> hiddenSysProps) {
      if (hiddenSysProps != null && !hiddenSysProps.isEmpty()) {
        this.hiddenSysProps.clear();
        this.hiddenSysProps.addAll(hiddenSysProps);
      }
      return this;
    }

    public MetricsConfigBuilder setMetricReporterPlugins(PluginInfo[] metricReporterPlugins) {
      this.metricReporterPlugins = metricReporterPlugins != null ? metricReporterPlugins : new PluginInfo[0];
      return this;
    }

    public MetricsConfigBuilder setCounterSupplier(PluginInfo info) {
      this.counterSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setMeterSupplier(PluginInfo info) {
      this.meterSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setTimerSupplier(PluginInfo info) {
      this.timerSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setHistogramSupplier(PluginInfo info) {
      this.histogramSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setHistoryHandler(PluginInfo info) {
      this.historyHandler = info;
      return this;
    }

    public MetricsConfig build() {
      return new MetricsConfig(metricReporterPlugins, hiddenSysProps, counterSupplier, meterSupplier,
          timerSupplier, histogramSupplier, historyHandler);
    }

  }

}
