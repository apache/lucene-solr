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

package org.apache.solr.prometheus.scraper;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import io.prometheus.client.Collector;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.prometheus.PrometheusExporterTestBase;
import org.apache.solr.prometheus.collector.MetricSamples;
import org.apache.solr.prometheus.exporter.MetricsConfiguration;
import org.apache.solr.prometheus.utils.Helpers;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.RestTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrStandaloneScraperTest extends RestTestBase {

  private static MetricsConfiguration configuration;
  private static SolrStandaloneScraper solrScraper;
  private static ExecutorService executor;
  private static HttpSolrClient solrClient;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    File tmpSolrHome = createTempDir().toFile();
    tmpSolrHome.deleteOnExit();

    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    initCore("solrconfig.xml", "managed-schema");

    createJettyAndHarness(
        tmpSolrHome.getAbsolutePath(),
        "solrconfig.xml",
        "managed-schema",
        "/solr",
        true,
        null);

    executor = ExecutorUtil.newMDCAwareFixedThreadPool(25, new SolrNamedThreadFactory("solr-cloud-scraper-tests"));
    configuration = Helpers.loadConfiguration("conf/prometheus-solr-exporter-scraper-test-config.xml");

    solrClient = getHttpSolrClient(restTestHarness.getAdminURL());
    solrScraper = new SolrStandaloneScraper(solrClient, executor);

    NoOpResponseParser responseParser = new NoOpResponseParser();
    responseParser.setWriterType("json");

    solrClient.setParser(responseParser);

    Helpers.indexAllDocs(solrClient);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    IOUtils.closeQuietly(solrScraper);
    IOUtils.closeQuietly(solrClient);
    cleanUpHarness();
    if (null != executor) {
      executor.shutdownNow();
      executor = null;
    }
    if (null != jetty) {
      jetty.stop();
      jetty = null;
    }
    solrScraper = null;
    solrClient = null;
  }

  @Test
  public void pingCollections() throws IOException {
    Map<String, MetricSamples> collectionMetrics = solrScraper.pingAllCollections(
        configuration.getPingConfiguration().get(0));

    assertTrue(collectionMetrics.isEmpty());
  }

  @Test
  public void pingCores() throws Exception {
    Map<String, MetricSamples> allCoreMetrics = solrScraper.pingAllCores(
        configuration.getPingConfiguration().get(0));

    assertEquals(1, allCoreMetrics.size());

    List<Collector.MetricFamilySamples> allSamples = allCoreMetrics.get("collection1").asList();
    Collector.MetricFamilySamples samples = allSamples.get(0);

    assertEquals("solr_ping", samples.name);
    assertEquals(1, samples.samples.size());
    assertEquals(1.0, samples.samples.get(0).value, 0.001);
    assertEquals(Collections.singletonList("base_url"), samples.samples.get(0).labelNames);
    assertEquals(Collections.singletonList(restTestHarness.getAdminURL()), samples.samples.get(0).labelValues);
  }

  @Test
  public void queryCollections() throws Exception {
    List<Collector.MetricFamilySamples> collection1Metrics = solrScraper.collections(
        configuration.getCollectionsConfiguration().get(0)).asList();

    assertTrue(collection1Metrics.isEmpty());
  }

  @Test
  public void metricsForHost() throws Exception {
    Map<String, MetricSamples> metricsByHost = solrScraper.metricsForAllHosts(configuration.getMetricsConfiguration().get(0));

    assertEquals(1, metricsByHost.size());

    List<Collector.MetricFamilySamples> replicaSamples = metricsByHost.get(restTestHarness.getAdminURL()).asList();

    assertEquals(1, replicaSamples.size());

    assertEquals(1, replicaSamples.size());
    assertEquals("solr_metrics_jvm_buffers", replicaSamples.get(0).name);
  }

  @Test
  public void search() throws Exception {
    List<Collector.MetricFamilySamples> samples = solrScraper.search(configuration.getSearchConfiguration().get(0)).asList();

    assertEquals(1, samples.size());

    Collector.MetricFamilySamples sampleFamily = samples.get(0);
    assertEquals("solr_facets_category", sampleFamily.name);
    assertEquals(PrometheusExporterTestBase.FACET_VALUES.size(), sampleFamily.samples.size());

    for (Collector.MetricFamilySamples.Sample sample : sampleFamily.samples) {
      assertEquals(PrometheusExporterTestBase.FACET_VALUES.get(sample.labelValues.get(0)), sample.value, 0.001);
    }
  }

}
