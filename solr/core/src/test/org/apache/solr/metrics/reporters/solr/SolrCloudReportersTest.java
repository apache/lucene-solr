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
package org.apache.solr.metrics.reporters.solr;

import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.SolrCoreContainerReporter;
import org.apache.solr.metrics.SolrCoreReporter;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Metric;

/**
 *
 */
public class SolrCloudReportersTest extends SolrCloudTestCase {
  volatile int leaderRegistries;
  volatile int clusterRegistries;
  volatile int jmxReporter;



  @BeforeClass
  public static void configureDummyCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(0).configure();
  }

  @Before
  public void closePreviousCluster() throws Exception {
    shutdownCluster();
    leaderRegistries = 0;
    clusterRegistries = 0;
  }

  @Test
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void testExplicitConfiguration() throws Exception {
    String solrXml = IOUtils.toString(SolrCloudReportersTest.class.getResourceAsStream("/solr/solr-solrreporter.xml"), "UTF-8");
    configureCluster(2)
        .withSolrXml(solrXml).configure();
    cluster.uploadConfigSet(Paths.get(TEST_PATH().toString(), "configsets", "minimal", "conf"), "test");

    CollectionAdminRequest.createCollection("test_collection", "test", 2, 2)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setMaxShardsPerNode(4)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("test_collection", 2, 4);
    
    waitForState("Expected test_collection with 2 shards and 2 replicas", "test_collection", clusterShape(2, 4));
 
    // TODO this is no good
    Thread.sleep(10000);
    
    cluster.getJettySolrRunners().forEach(jetty -> {
      CoreContainer cc = jetty.getCoreContainer();
      // verify registry names
      for (String name : cc.getLoadedCoreNames()) {
        SolrCore core = cc.getCore(name);
        try {
          String registryName = core.getCoreMetricManager().getRegistryName();
          String leaderRegistryName = core.getCoreMetricManager().getLeaderRegistryName();
          String coreName = core.getName();
          String collectionName = core.getCoreDescriptor().getCollectionName();
          String replicaName = coreName.substring(coreName.indexOf("_replica_") + 1);
          String shardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();

          assertEquals("solr.core." + collectionName + "." + shardId + "." + replicaName, registryName);
          assertEquals("solr.collection." + collectionName + "." + shardId + ".leader", leaderRegistryName);

        } finally {
          if (core != null) {
            core.close();
          }
        }
      }
      SolrMetricManager metricManager = cc.getMetricManager();
      Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.cluster");
      assertEquals(reporters.toString(), 1, reporters.size());
      SolrMetricReporter reporter = reporters.get("test");
      assertNotNull(reporter);
      assertTrue(reporter.toString(), reporter instanceof SolrClusterReporter);
      assertEquals(5, reporter.getPeriod());
      assertTrue(reporter.toString(), reporter instanceof SolrCoreContainerReporter);
      SolrCoreContainerReporter solrCoreContainerReporter = (SolrCoreContainerReporter)reporter;
      assertNotNull(solrCoreContainerReporter.getCoreContainer());
      for (String registryName : metricManager.registryNames(".*\\.shard[0-9]\\.replica.*")) {
        reporters = metricManager.getReporters(registryName);
        jmxReporter = 0;
        reporters.forEach((k, v) -> {
          if (v instanceof SolrJmxReporter) {
            jmxReporter++;
          }
        });
        assertEquals(reporters.toString(), 1 + jmxReporter, reporters.size());
        reporter = null;
        for (String name : reporters.keySet()) {
          if (name.startsWith("test")) {
            reporter = reporters.get(name);
          }
        }
        assertNotNull(reporter);
        assertTrue(reporter.toString(), reporter instanceof SolrShardReporter);
        assertEquals(5, reporter.getPeriod());
        assertTrue(reporter.toString(), reporter instanceof SolrCoreReporter);
        SolrCoreReporter solrCoreReporter = (SolrCoreReporter)reporter;
        assertNotNull(solrCoreReporter.getCore());
      }
      for (String registryName : metricManager.registryNames(".*\\.leader")) {
        leaderRegistries++;
        reporters = metricManager.getReporters(registryName);
        // no reporters registered for leader registry
        assertEquals(reporters.toString(), 0, reporters.size());
        // verify specific metrics
        Map<String, Metric> metrics = metricManager.registry(registryName).getMetrics();
        String key = "QUERY./select.requests";
        assertTrue(key, metrics.containsKey(key));
        assertTrue(key, metrics.get(key) instanceof AggregateMetric);
        key = "UPDATE./update.requests";
        assertTrue(key, metrics.containsKey(key));
        assertTrue(key, metrics.get(key) instanceof AggregateMetric);
      }
      if (metricManager.registryNames().contains("solr.cluster")) {
        clusterRegistries++;
        Map<String,Metric> metrics = metricManager.registry("solr.cluster").getMetrics();
        String key = "jvm.memory.heap.init";
        assertTrue(key, metrics.containsKey(key));
        assertTrue(key, metrics.get(key) instanceof AggregateMetric);
        key = "leader.test_collection.shard1.UPDATE./update.requests.max";
        assertTrue(key, metrics.containsKey(key));
        assertTrue(key, metrics.get(key) instanceof AggregateMetric);
      }
    });

    assertEquals("leaderRegistries", 2, leaderRegistries);
    assertEquals("clusterRegistries", 1, clusterRegistries);
  }

  @Test
  // commented 15-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void testDefaultPlugins() throws Exception {
    String solrXml = IOUtils.toString(SolrCloudReportersTest.class.getResourceAsStream("/solr/solr.xml"), "UTF-8");
    configureCluster(2)
        .withSolrXml(solrXml).configure();
    cluster.uploadConfigSet(Paths.get(TEST_PATH().toString(), "configsets", "minimal", "conf"), "test");

    CollectionAdminRequest.createCollection("test_collection", "test", 2, 2)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setMaxShardsPerNode(4)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("test_collection", 2, 4);
    waitForState("Expected test_collection with 2 shards and 2 replicas", "test_collection", clusterShape(2, 4));
    cluster.getJettySolrRunners().forEach(jetty -> {
      CoreContainer cc = jetty.getCoreContainer();
      SolrMetricManager metricManager = cc.getMetricManager();
      Map<String, SolrMetricReporter> reporters = metricManager.getReporters("solr.cluster");
      assertEquals(reporters.toString(), 0, reporters.size());
      for (String registryName : metricManager.registryNames(".*\\.shard[0-9]\\.replica.*")) {
        reporters = metricManager.getReporters(registryName);
        jmxReporter = 0;
        reporters.forEach((k, v) -> {
          if (v instanceof SolrJmxReporter) {
            jmxReporter++;
          }
        });
        assertEquals(reporters.toString(), 0 + jmxReporter, reporters.size());
      }
    });
  }
}
