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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import io.prometheus.client.Collector;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.prometheus.PrometheusExporterTestBase;
import org.apache.solr.prometheus.collector.MetricSamples;
import org.apache.solr.prometheus.exporter.MetricsConfiguration;
import org.apache.solr.prometheus.exporter.PrometheusExporterSettings;
import org.apache.solr.prometheus.exporter.SolrClientFactory;
import org.apache.solr.prometheus.utils.Helpers;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrCloudScraperTest extends PrometheusExporterTestBase {

  private MetricsConfiguration configuration;
  private SolrCloudScraper solrCloudScraper;
  private ExecutorService executor;

  private SolrCloudScraper createSolrCloudScraper() {
    CloudSolrClient solrClient = new CloudSolrClient.Builder(
        Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
        .build();

    NoOpResponseParser responseParser = new NoOpResponseParser();
    responseParser.setWriterType("json");

    solrClient.setParser(responseParser);

    solrClient.connect();

    SolrClientFactory factory = new SolrClientFactory(PrometheusExporterSettings.builder().build());

    return new SolrCloudScraper(solrClient, executor, factory);
  }

  private ClusterState getClusterState() {
    return cluster.getSolrClient().getZkStateReader().getClusterState();
  }

  private DocCollection getCollectionState() {
    return getClusterState().getCollection(PrometheusExporterTestBase.COLLECTION);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    executor = ExecutorUtil.newMDCAwareFixedThreadPool(25, new SolrNamedThreadFactory("solr-cloud-scraper-tests"));
    configuration = Helpers.loadConfiguration("conf/prometheus-solr-exporter-scraper-test-config.xml");
    solrCloudScraper = createSolrCloudScraper();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.closeQuietly(solrCloudScraper);
    if (null != executor) {
      executor.shutdownNow();
      executor = null;
    }
  }

  @Test
  public void pingCollections() throws Exception {
    Map<String, MetricSamples> collectionMetrics = solrCloudScraper.pingAllCollections(
        configuration.getPingConfiguration().get(0));

    assertEquals(1, collectionMetrics.size());
    assertTrue(collectionMetrics.containsKey(PrometheusExporterTestBase.COLLECTION));

    List<Collector.MetricFamilySamples> collectionSamples = collectionMetrics.get(PrometheusExporterTestBase.COLLECTION).asList();
    assertEquals(1, collectionSamples.size());
    Collector.MetricFamilySamples collection1Metrics = collectionSamples.get(0);
    assertEquals("solr_ping", collection1Metrics.name);
    assertEquals(1, collection1Metrics.samples.size());

    assertEquals(1.0, collection1Metrics.samples.get(0).value, 0.001);
    assertEquals(Collections.singletonList("zk_host"), collection1Metrics.samples.get(0).labelNames);
    assertEquals(Collections.singletonList(cluster.getZkServer().getZkAddress()), collection1Metrics.samples.get(0).labelValues);
  }

  @Test
  public void pingCores() throws Exception {
    Map<String, MetricSamples> allCoreMetrics = solrCloudScraper.pingAllCores(
        configuration.getPingConfiguration().get(0));

    Map<String, DocCollection> collectionStates = getClusterState().getCollectionsMap();

    long coreCount = collectionStates.entrySet()
        .stream()
        .mapToInt(entry -> entry.getValue().getReplicas().size())
        .sum();

    assertEquals(coreCount, allCoreMetrics.size());

    for (Map.Entry<String, DocCollection> entry : collectionStates.entrySet()) {
      String coreName = entry.getValue().getReplicas().get(0).getCoreName();
      assertTrue(allCoreMetrics.containsKey(coreName));
      List<Collector.MetricFamilySamples> coreMetrics = allCoreMetrics.get(coreName).asList();
      assertEquals(1, coreMetrics.size());
      assertEquals("solr_ping", coreMetrics.get(0).name);
      assertEquals(1, coreMetrics.get(0).samples.size());
      assertEquals(1.0, coreMetrics.get(0).samples.get(0).value, 0.001);
    }
  }

  @Test
  public void queryCollections() throws Exception {
    List<Collector.MetricFamilySamples> collection1Metrics = solrCloudScraper.collections(
        configuration.getCollectionsConfiguration().get(0)).asList();

    assertEquals(2, collection1Metrics.size());
    Collector.MetricFamilySamples liveNodeSamples = collection1Metrics.get(0);
    assertEquals("solr_collections_live_nodes", liveNodeSamples.name);
    assertEquals("See following URL: https://lucene.apache.org/solr/guide/collections-api.html#clusterstatus", liveNodeSamples.help);
    assertEquals(1, liveNodeSamples.samples.size());

    assertEquals(
        getClusterState().getLiveNodes().size(),
        liveNodeSamples.samples.get(0).value, 0.001);

    Collector.MetricFamilySamples shardLeaderSamples = collection1Metrics.get(1);

    DocCollection collection = getCollectionState();
    List<Replica> allReplicas = collection.getReplicas();
    assertEquals(allReplicas.size(), shardLeaderSamples.samples.size());

    Collection<Slice> slices = getCollectionState().getSlices();

    Set<String> leaderCoreNames = slices.stream()
        .map(slice -> collection.getLeader(slice.getName()).getCoreName())
        .collect(Collectors.toSet());

    for (Collector.MetricFamilySamples.Sample sample : shardLeaderSamples.samples) {
      assertEquals("solr_collections_shard_leader", sample.name);
      assertEquals(Arrays.asList("collection", "shard", "replica", "core", "type", "zk_host"), sample.labelNames);
      assertEquals(leaderCoreNames.contains(sample.labelValues.get(3)) ? 1.0 : 0.0, sample.value, 0.001);
    }
  }

  @Test
  public void metricsForEachHost() throws Exception {
    Map<String, MetricSamples> metricsByHost = solrCloudScraper.metricsForAllHosts(configuration.getMetricsConfiguration().get(0));

    List<Replica> replicas = getCollectionState().getReplicas();
    assertEquals(replicas.size(), metricsByHost.size());

    for (Replica replica : replicas) {
      List<Collector.MetricFamilySamples> replicaSamples = metricsByHost.get(replica.getBaseUrl()).asList();
      assertEquals(1, replicaSamples.size());
      assertEquals("solr_metrics_jvm_buffers", replicaSamples.get(0).name);
    }
  }

  @Test
  public void search() throws Exception {
    List<Collector.MetricFamilySamples> samples = solrCloudScraper.search(configuration.getSearchConfiguration().get(0)).asList();

    assertEquals(1, samples.size());

    Collector.MetricFamilySamples sampleFamily = samples.get(0);
    assertEquals("solr_facets_category", sampleFamily.name);
    assertEquals(FACET_VALUES.size(), sampleFamily.samples.size());

    for (Collector.MetricFamilySamples.Sample sample : sampleFamily.samples) {
      assertEquals(FACET_VALUES.get(sample.labelValues.get(0)), sample.value, 0.001);
    }
  }

}
