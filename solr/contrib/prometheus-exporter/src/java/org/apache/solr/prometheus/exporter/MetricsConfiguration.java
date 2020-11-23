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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MetricsConfiguration {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

  public static MetricsConfiguration from(String resource) throws Exception {
    // See solr-core XmlConfigFile
    final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    try {
      dbf.setXIncludeAware(true);
      dbf.setNamespaceAware(true);
    } catch (UnsupportedOperationException e) {
      log.warn("{} XML parser doesn't support XInclude option", resource);
    }

    Document document;
    Path path = Path.of(resource);
    if (Files.exists(path)) {
      document = dbf.newDocumentBuilder().parse(path.toUri().toASCIIString());
    } else {
      try (InputStream configInputStream = MethodHandles.lookup().lookupClass().getClassLoader().getResourceAsStream(resource.replace(File.separatorChar, '/'))) {
        document = dbf.newDocumentBuilder().parse(configInputStream);
      }
    }

    return from(document);
  }

  public static MetricsConfiguration from(Document config) throws Exception {
    Node settings = getNode(config, "/config/settings");

    Node pingConfig = getNode(config, "/config/rules/ping");
    Node metricsConfig = getNode(config, "/config/rules/metrics");
    Node collectionsConfig = getNode(config, "/config/rules/collections");
    Node searchConfiguration = getNode(config, "/config/rules/search");

    return new MetricsConfiguration(
        settings == null ? PrometheusExporterSettings.builder().build() : PrometheusExporterSettings.from(settings),
        toMetricQueries(pingConfig),
        toMetricQueries(metricsConfig),
        toMetricQueries(collectionsConfig),
        toMetricQueries(searchConfiguration)
    );
  }

  static final XPathFactory xpathFactory = XPathFactory.newInstance();

  private static Node getNode(Document doc, String path) {
    // Copied from solr-core XmlConfigFile.getNode with simplifications
    XPath xpath = xpathFactory.newXPath();
    String xstr = path; //normalize(path);

    try {
      NodeList nodes = (NodeList) xpath.evaluate(xstr, doc,
          XPathConstants.NODESET);
      if (nodes == null || 0 == nodes.getLength()) {
        return null;
      }
      if (1 < nodes.getLength()) {
        throw new RuntimeException("more than one value");
      }
      return nodes.item(0);
    } catch (Exception e) {
      throw new RuntimeException("Error in xpath:" + xstr, e);
    }
  }

  private static List<MetricsQuery> toMetricQueries(Node node) throws JsonQueryException {
    if (node == null) {
      return Collections.emptyList();
    }

    return MetricsQuery.from(node);
  }

}
