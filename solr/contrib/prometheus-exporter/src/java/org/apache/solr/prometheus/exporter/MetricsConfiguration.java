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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.core.XmlConfigFile;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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

  public static MetricsConfiguration from(XmlConfigFile config) throws Exception {
    Node settings = config.getNode("/config/settings", false);

    NodeList jqTemplates = config.getNodeList("/config/jq-templates/template", false);
    Map<String, MetricsQueryTemplate> jqTemplatesMap =
        (jqTemplates != null && jqTemplates.getLength() > 0) ? loadJqTemplates(jqTemplates) : Collections.emptyMap();

    Node pingConfig = config.getNode("/config/rules/ping", false);
    Node metricsConfig = config.getNode("/config/rules/metrics", false);
    Node collectionsConfig = config.getNode("/config/rules/collections", false);
    Node searchConfiguration = config.getNode("/config/rules/search", false);

    return new MetricsConfiguration(
        settings == null ? PrometheusExporterSettings.builder().build() : PrometheusExporterSettings.from(settings),
        toMetricQueries(pingConfig, jqTemplatesMap),
        toMetricQueries(metricsConfig, jqTemplatesMap),
        toMetricQueries(collectionsConfig, jqTemplatesMap),
        toMetricQueries(searchConfiguration, jqTemplatesMap)
    );
  }

  private static List<MetricsQuery> toMetricQueries(Node node, Map<String, MetricsQueryTemplate> jqTemplatesMap) throws JsonQueryException {
    if (node == null) {
      return Collections.emptyList();
    }

    return MetricsQuery.from(node, jqTemplatesMap);
  }

  static Map<String, MetricsQueryTemplate> loadJqTemplates(NodeList jqTemplates) {
    Map<String, MetricsQueryTemplate> map = new HashMap<>();
    for (int t = 0; t < jqTemplates.getLength(); t++) {
      Node template = jqTemplates.item(t);
      if (template.getNodeType() == Node.ELEMENT_NODE && template.hasAttributes()) {
        Node nameAttr = template.getAttributes().getNamedItem("name");
        String name = nameAttr != null ? nameAttr.getNodeValue() : null;
        String tmpl = template.getTextContent();
        if (!StringUtils.isEmpty(name) && !StringUtils.isEmpty(tmpl)) {
          Node defaultTypeAttr = template.getAttributes().getNamedItem("defaultType");
          String defaultType = defaultTypeAttr != null ? defaultTypeAttr.getNodeValue() : null;
          map.put(name, new MetricsQueryTemplate(name, tmpl, defaultType));
        }
      }
    }
    return map;
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
}
