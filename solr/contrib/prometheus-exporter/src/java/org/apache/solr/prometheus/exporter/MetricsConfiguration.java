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

package org.apache.solr.prometheus.exporter;

import java.util.Collections;
import java.util.List;

import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.solr.core.XmlConfigFile;
import org.w3c.dom.Node;

public class MetricsConfiguration {

  private final PrometheusExporterSettings settings;

  private final List<MetricsQuery> pingConfiguration;
  private final List<MetricsQuery> metricsConfiguration;
  private final List<MetricsQuery> collectionsConfiguration;
  private final List<MetricsQuery> searchConfiguration;

  private MetricsConfiguration(
      PrometheusExporterSettings settings,
      List<MetricsQuery> pingConfiguration,
      List<MetricsQuery> metricsConfiguration,
      List<MetricsQuery> collectionsConfiguration,
      List<MetricsQuery> searchConfiguration) {
    this.settings = settings;
    this.pingConfiguration = pingConfiguration;
    this.metricsConfiguration = metricsConfiguration;
    this.collectionsConfiguration = collectionsConfiguration;
    this.searchConfiguration = searchConfiguration;
  }

  public PrometheusExporterSettings getSettings() {
    return settings;
  }

  public List<MetricsQuery> getPingConfiguration() {
    return pingConfiguration;
  }

  public List<MetricsQuery> getMetricsConfiguration() {
    return metricsConfiguration;
  }

  public List<MetricsQuery> getCollectionsConfiguration() {
    return collectionsConfiguration;
  }

  public List<MetricsQuery> getSearchConfiguration() {
    return searchConfiguration;
  }

  public static MetricsConfiguration from(XmlConfigFile config) throws Exception {
    Node settings = config.getNode("/config/settings", false);

    Node pingConfig = config.getNode("/config/rules/ping", false);
    Node metricsConfig = config.getNode("/config/rules/metrics", false);
    Node collectionsConfig = config.getNode("/config/rules/collections", false);
    Node searchConfiguration = config.getNode("/config/rules/search", false);

    return new MetricsConfiguration(
        settings == null ? PrometheusExporterSettings.builder().build() : PrometheusExporterSettings.from(settings),
        toMetricQueries(pingConfig),
        toMetricQueries(metricsConfig),
        toMetricQueries(collectionsConfig),
        toMetricQueries(searchConfiguration)
    );
  }

  private static List<MetricsQuery> toMetricQueries(Node node) throws JsonQueryException {
    if (node == null) {
      return Collections.emptyList();
    }

    return MetricsQuery.from(node);
  }

}
