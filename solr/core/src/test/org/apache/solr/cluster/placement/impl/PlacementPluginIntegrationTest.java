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

package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.CollectionMetrics;
import org.apache.solr.cluster.placement.NodeMetric;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.cluster.placement.ReplicaMetrics;
import org.apache.solr.cluster.placement.ShardMetrics;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementConfig;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cluster.placement.plugins.MinimizeCoresPlacementFactory;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonMap;

/**
 * Test for {@link MinimizeCoresPlacementFactory} using a {@link MiniSolrCloudCluster}.
 */
@LogLevel("org.apache.solr.cluster.placement.impl=DEBUG")
public class PlacementPluginIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = PlacementPluginIntegrationTest.class.getName() + "_collection";

  private static SolrCloudManager cloudManager;
  private static CoreContainer cc;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // placement plugins need metrics
    System.setProperty("metricsEnabled", "true");
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    cc = cluster.getJettySolrRunner(0).getCoreContainer();
    cloudManager = cc.getZkController().getSolrCloudManager();
  }

  @After
  public void cleanup() throws Exception {
    cluster.deleteAllCollections();
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .GET()
        .build();
    V2Response rsp = req.process(cluster.getSolrClient());
    if (rsp._get(Arrays.asList("plugin", PlacementPluginFactory.PLUGIN_NAME), null) != null) {
      req = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .POST()
          .withPayload("{remove: '" + PlacementPluginFactory.PLUGIN_NAME + "'}")
          .build();
      req.process(cluster.getSolrClient());
    }
  }

  @Test
  public void testMinimizeCores() throws Exception {
    PluginMeta plugin = new PluginMeta();
    plugin.name = PlacementPluginFactory.PLUGIN_NAME;
    plugin.klass = MinimizeCoresPlacementFactory.class.getName();
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("add", plugin))
        .build();
    req.process(cluster.getSolrClient());

    CollectionAdminResponse rsp = CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .process(cluster.getSolrClient());
    assertTrue(rsp.isSuccess());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
    // use Solr-specific API to verify the expected placements
    ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
    DocCollection collection = clusterState.getCollectionOrNull(COLLECTION);
    assertNotNull(collection);
    Map<String, AtomicInteger> coresByNode = new HashMap<>();
    collection.forEachReplica((shard, replica) -> coresByNode.computeIfAbsent(replica.getNodeName(), n -> new AtomicInteger()).incrementAndGet());
    int maxCores = 0;
    int minCores = Integer.MAX_VALUE;
    for (Map.Entry<String, AtomicInteger> entry : coresByNode.entrySet()) {
      assertTrue("too few cores on node " + entry.getKey() + ": " + entry.getValue(),
          entry.getValue().get() > 0);
      if (entry.getValue().get() > maxCores) {
        maxCores = entry.getValue().get();
      }
      if (entry.getValue().get() < minCores) {
        minCores = entry.getValue().get();
      }
    }
    assertEquals("max cores too high", 2, maxCores);
    assertEquals("min cores too low", 1, minCores);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDynamicReconfiguration() throws Exception {
    PlacementPluginFactory<? extends PlacementPluginConfig> pluginFactory = cc.getPlacementPluginFactory();
    assertTrue("wrong type " + pluginFactory.getClass().getName(), pluginFactory instanceof DelegatingPlacementPluginFactory);
    DelegatingPlacementPluginFactory wrapper = (DelegatingPlacementPluginFactory) pluginFactory;

    int version = wrapper.getVersion();
    log.debug("--initial version={}", version);

    PluginMeta plugin = new PluginMeta();
    plugin.name = PlacementPluginFactory.PLUGIN_NAME;
    plugin.klass = MinimizeCoresPlacementFactory.class.getName();
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("add", plugin))
        .build();
    req.process(cluster.getSolrClient());

    version = waitForVersionChange(version, wrapper, 10);

    assertTrue("wrong version " + version, version > 0);
    PlacementPluginFactory<? extends PlacementPluginConfig> factory = wrapper.getDelegate();
    assertTrue("wrong type " + factory.getClass().getName(), factory instanceof MinimizeCoresPlacementFactory);

    // reconfigure
    plugin.klass = AffinityPlacementFactory.class.getName();
    plugin.config = new AffinityPlacementConfig(1, 2);
    req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("update", plugin))
        .build();
    req.process(cluster.getSolrClient());

    version = waitForVersionChange(version, wrapper, 10);

    factory = wrapper.getDelegate();
    assertTrue("wrong type " + factory.getClass().getName(), factory instanceof AffinityPlacementFactory);
    AffinityPlacementConfig config = ((AffinityPlacementFactory) factory).getConfig();
    assertEquals("minimalFreeDiskGB", 1, config.minimalFreeDiskGB);
    assertEquals("prioritizedFreeDiskGB", 2, config.prioritizedFreeDiskGB);

    // change plugin config
    plugin.config = new AffinityPlacementConfig(3, 4);
    req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("update", plugin))
        .build();
    req.process(cluster.getSolrClient());

    version = waitForVersionChange(version, wrapper, 10);
    factory = wrapper.getDelegate();
    assertTrue("wrong type " + factory.getClass().getName(), factory instanceof AffinityPlacementFactory);
    config = ((AffinityPlacementFactory) factory).getConfig();
    assertEquals("minimalFreeDiskGB", 3, config.minimalFreeDiskGB);
    assertEquals("prioritizedFreeDiskGB", 4, config.prioritizedFreeDiskGB);

    // add plugin of the right type but with the wrong name
    plugin.name = "myPlugin";
    req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("add", plugin))
        .build();
    req.process(cluster.getSolrClient());
    try {
      int newVersion = waitForVersionChange(version, wrapper, 5);
      if (newVersion != version) {
        fail("factory configuration updated but plugin name was wrong: " + plugin);
      }
    } catch (TimeoutException te) {
      // expected
    }
    // remove plugin
    req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload("{remove: '" + PlacementPluginFactory.PLUGIN_NAME + "'}")
        .build();
    req.process(cluster.getSolrClient());
    waitForVersionChange(version, wrapper, 10);
    factory = wrapper.getDelegate();
    assertNull("no factory should be present", factory);
  }

  @Test
  public void testAttributeFetcherImpl() throws Exception {
    CollectionAdminResponse rsp = CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .process(cluster.getSolrClient());
    assertTrue(rsp.isSuccess());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
    Cluster cluster = new SimpleClusterAbstractionsImpl.ClusterImpl(cloudManager);
    SolrCollection collection = cluster.getCollection(COLLECTION);
    AttributeFetcher attributeFetcher = new AttributeFetcherImpl(cloudManager);
    String someMetricKey = "solr.jvm:system.properties:user.name";
    String sysprop = "user.name";
    String sysenv = "PWD";
    attributeFetcher
        .fetchFrom(cluster.getLiveNodes())
        .requestNodeHeapUsage()
        .requestNodeSystemLoadAverage()
        .requestNodeMetric(someMetricKey)
        .requestNodeMetric(NodeMetric.FREE_DISK_GB)
        .requestNodeMetric(NodeMetric.TOTAL_DISK_GB)
        // XXX this doesn't work yet, it overwrites
        // the inserter in AttributeFetcherImpl for nodeCoresCount
        // .requestNodeMetric(NodeMetric.NUM_CORES)
        .requestNodeMetric(NodeMetric.SYSLOAD_AVG)
        .requestNodeMetric(NodeMetric.AVAILABLE_PROCESSORS)
        .requestNodeSystemProperty(sysprop)
        .requestNodeEnvironmentVariable(sysenv)
        .requestNodeTotalDisk()
        .requestNodeFreeDisk()
        .requestNodeCoresCount()
        .requestCollectionMetrics(collection, Set.of(ReplicaMetric.INDEX_SIZE_GB, ReplicaMetric.QUERY_RATE_1MIN, ReplicaMetric.UPDATE_RATE_1MIN));
    AttributeValues attributeValues = attributeFetcher.fetchAttributes();
    String userName = System.getProperty("user.name");
    String pwd = System.getenv("PWD");
    // node metrics
    for (Node node : cluster.getLiveNodes()) {
      assertTrue("heap usage", attributeValues.getHeapUsage(node).isPresent());
      assertTrue("total disk 1", attributeValues.getTotalDisk(node).isPresent());
      Optional<Double> spaceOpt = attributeValues.getNodeMetric(node, NodeMetric.TOTAL_DISK_GB);
      assertTrue("total disk 2", spaceOpt.isPresent());
      assertEquals("total disk 1 vs 2", attributeValues.getTotalDisk(node).get(), spaceOpt.get(), 0.0);
      assertTrue("free disk 1", attributeValues.getFreeDisk(node).isPresent());
      spaceOpt = attributeValues.getNodeMetric(node, NodeMetric.FREE_DISK_GB);
      assertTrue("free disk 2", spaceOpt.isPresent());
      assertEquals("free disk 1 vs 2", attributeValues.getFreeDisk(node).get(), spaceOpt.get(), 0.0);
      assertTrue("cores count 1", attributeValues.getCoresCount(node).isPresent());
//      Optional<Integer> coresOpt = attributeValues.getNodeMetric(node, NodeMetric.NUM_CORES);
//      assertTrue("cores 2", coresOpt.isPresent());
//      assertEquals("cores 1 vs 2", attributeValues.getCoresCount(node).get(), coresOpt.get());
      assertTrue("systemLoadAverage 1", attributeValues.getSystemLoadAverage(node).isPresent());
      assertTrue("systemLoadAverage 2", attributeValues.getNodeMetric(node, NodeMetric.SYSLOAD_AVG).isPresent());
      assertTrue("availableProcessors", attributeValues.getNodeMetric(node, NodeMetric.AVAILABLE_PROCESSORS).isPresent());
      Optional<Object> userNameOpt = attributeValues.getNodeMetric(node, someMetricKey);
      assertTrue("user.name", userNameOpt.isPresent());
      assertEquals("userName", userName, userNameOpt.get());
      Optional<String> syspropOpt = attributeValues.getSystemProperty(node, sysprop);
      assertTrue("sysprop", syspropOpt.isPresent());
      assertEquals("user.name sysprop", userName, syspropOpt.get());
      Optional<String> sysenvOpt = attributeValues.getEnvironmentVariable(node, sysenv);
      assertTrue("sysenv", sysenvOpt.isPresent());
      assertEquals("PWD sysenv", pwd, sysenvOpt.get());
    }
    assertTrue(attributeValues.getCollectionMetrics(COLLECTION).isPresent());
    CollectionMetrics collectionMetrics = attributeValues.getCollectionMetrics(COLLECTION).get();
    collection.shards().forEach(shard -> {
      Optional<ShardMetrics> shardMetricsOpt = collectionMetrics.getShardMetrics(shard.getShardName());
      assertTrue("shard metrics", shardMetricsOpt.isPresent());
      shard.replicas().forEach(replica -> {
        Optional<ReplicaMetrics> replicaMetricsOpt = shardMetricsOpt.get().getReplicaMetrics(replica.getReplicaName());
        assertTrue("replica metrics", replicaMetricsOpt.isPresent());
        ReplicaMetrics replicaMetrics = replicaMetricsOpt.get();
        Optional<Double> indexSizeOpt = replicaMetrics.getReplicaMetric(ReplicaMetric.INDEX_SIZE_GB);
        assertTrue("indexSize", indexSizeOpt.isPresent());
        assertTrue("wrong type, expected Double but was " + indexSizeOpt.get().getClass(), indexSizeOpt.get() instanceof Double);
        assertTrue("indexSize should be > 0 but was " + indexSizeOpt.get(), indexSizeOpt.get() > 0);
        assertTrue("indexSize should be < 0.01 but was " + indexSizeOpt.get(), indexSizeOpt.get() < 0.01);

        assertNotNull("queryRate", replicaMetrics.getReplicaMetric(ReplicaMetric.QUERY_RATE_1MIN));
        assertNotNull("updateRate", replicaMetrics.getReplicaMetric(ReplicaMetric.UPDATE_RATE_1MIN));
      });
    });
  }

  private int waitForVersionChange(int currentVersion, DelegatingPlacementPluginFactory wrapper, int timeoutSec) throws Exception {
    TimeOut timeout = new TimeOut(timeoutSec, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (!timeout.hasTimedOut()) {
      int newVersion = wrapper.getVersion();
      if (newVersion < currentVersion) {
        throw new Exception("Invalid version - went back! currentVersion=" + currentVersion +
            " newVersion=" + newVersion);
      } else if (currentVersion < newVersion) {
        log.debug("--current version was {}, new version is {}", currentVersion, newVersion);
        return newVersion;
      }
      timeout.sleep(200);
    }
    throw new TimeoutException("version didn't change in time, currentVersion=" + currentVersion);
  }
}
