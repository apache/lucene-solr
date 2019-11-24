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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.prometheus.PrometheusExporterTestBase;
import org.apache.solr.prometheus.utils.Helpers;
import org.junit.After;

/**
 * Test base class.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SolrExporterTestBase extends PrometheusExporterTestBase {

  private SolrExporter solrExporter;
  private CloseableHttpClient httpClient;
  private int promtheusExporterPort;

  @SuppressWarnings("deprecation")
  @Override
  @After
  public void tearDown() throws Exception {
    if (solrExporter != null) {
      solrExporter.stop();
    }
    IOUtils.closeQuietly(httpClient);
    super.tearDown();
  }

  protected void startMetricsExporterWithConfiguration(String scrapeConfiguration) throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      promtheusExporterPort = socket.getLocalPort();
    }

    solrExporter = new SolrExporter(
        promtheusExporterPort,
        25,
        10,
        SolrScrapeConfiguration.solrCloud(cluster.getZkServer().getZkAddress()),
        Helpers.loadConfiguration(scrapeConfiguration));

    solrExporter.start();
    httpClient = HttpClients.createDefault();

    for (int i = 0; i < 50; ++i) {
      Thread.sleep(100);

      try {
        getAllMetrics();
        System.out.println("Prometheus exporter running");
        break;
      } catch (IOException exception) {
        if (i % 10 == 0) {
          System.out.println("Waiting for Prometheus exporter");
        }
      }
    }
  }

  protected Map<String, Double> getAllMetrics() throws URISyntaxException, IOException {
    URI uri = new URI("http://localhost:" + promtheusExporterPort + "/metrics");

    HttpGet request = new HttpGet(uri);

    Map<String, Double> metrics = new HashMap<>();

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
        String currentLine;

        while ((currentLine = reader.readLine()) != null) {
          // Lines that begin with a # are a comment in prometheus.
          if (currentLine.startsWith("#")) {
            continue;
          }

          String[] parts = currentLine.split(" ");

          assertEquals("Metric must have name and value: " + currentLine, 2, parts.length);

          metrics.put(parts[0], Double.valueOf(parts[1]));
        }
      }
    }

    return metrics;
  }

}
