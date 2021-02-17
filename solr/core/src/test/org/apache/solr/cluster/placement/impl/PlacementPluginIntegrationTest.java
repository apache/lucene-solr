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
import org.apache.solr.cluster.placement.ReplicaMetrics;
import org.apache.solr.cluster.placement.ShardMetrics;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementConfig;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cluster.placement.plugins.MinimizeCoresPlacementFactory;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.LogLevel;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonMap;

/**
 * Test for {@link MinimizeCoresPlacementFactory} using a {@link MiniSolrCloudCluster}.
 */
@LogLevel("org.apache.solr.cluster.placement.impl=DEBUG")
public class PlacementPluginIntegrationTest extends SolrCloudTestCase {
  private static final String COLLECTION = PlacementPluginIntegrationTest.class.getSimpleName() + "_collection";

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
  public void testDynamicReconfiguration() throws Exception {
    PlacementPluginFactory<? extends PlacementPluginConfig> pluginFactory = cc.getPlacementPluginFactory();
    assertTrue("wrong type " + pluginFactory.getClass().getName(), pluginFactory instanceof DelegatingPlacementPluginFactory);
    DelegatingPlacementPluginFactory wrapper = (DelegatingPlacementPluginFactory) pluginFactory;
    Phaser phaser = new Phaser();
    wrapper.setDelegationPhaser(phaser);

    int version = phaser.getPhase();
    assertTrue("wrong version " + version, version > -1);

    PluginMeta plugin = new PluginMeta();
    plugin.name = PlacementPluginFactory.PLUGIN_NAME;
    plugin.klass = MinimizeCoresPlacementFactory.class.getName();
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("add", plugin))
        .build();
    req.process(cluster.getSolrClient());

    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);
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

    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

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

    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);
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
    final int oldVersion = version;
    expectThrows(TimeoutException.class, () -> phaser.awaitAdvanceInterruptibly(oldVersion, 5, TimeUnit.SECONDS));
    // remove plugin
    req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload("{remove: '" + PlacementPluginFactory.PLUGIN_NAME + "'}")
        .build();
    req.process(cluster.getSolrClient());
    phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);
    factory = wrapper.getDelegate();
    assertNull("no factory should be present", factory);
  }

  @Test
  public void testWithCollectionIntegration() throws Exception {
    PlacementPluginFactory<? extends PlacementPluginConfig> pluginFactory = cc.getPlacementPluginFactory();
    assertTrue("wrong type " + pluginFactory.getClass().getName(), pluginFactory instanceof DelegatingPlacementPluginFactory);
    DelegatingPlacementPluginFactory wrapper = (DelegatingPlacementPluginFactory) pluginFactory;
    Phaser phaser = new Phaser();
    wrapper.setDelegationPhaser(phaser);

    int version = phaser.getPhase();

    Set<String> nodeSet = new HashSet<>();
    for (String node : cloudManager.getClusterStateProvider().getLiveNodes()) {
      if (nodeSet.size() > 1) {
        break;
      }
      nodeSet.add(node);
    }

    String SECONDARY_COLLECTION = COLLECTION + "_secondary";
    PluginMeta plugin = new PluginMeta();
    plugin.name = PlacementPluginFactory.PLUGIN_NAME;
    plugin.klass = AffinityPlacementFactory.class.getName();
    plugin.config = new AffinityPlacementConfig(1, 2, Map.of(COLLECTION, SECONDARY_COLLECTION));
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("add", plugin))
        .build();
    req.process(cluster.getSolrClient());

    phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    CollectionAdminResponse rsp = CollectionAdminRequest.createCollection(SECONDARY_COLLECTION, "conf", 1, 3)
        .process(cluster.getSolrClient());
    assertTrue(rsp.isSuccess());
    cluster.waitForActiveCollection(SECONDARY_COLLECTION, 1, 3);
    DocCollection secondary = cloudManager.getClusterStateProvider().getClusterState().getCollection(SECONDARY_COLLECTION);
    Set<String> secondaryNodes = new HashSet<>();
    secondary.forEachReplica((shard, replica) -> secondaryNodes.add(replica.getNodeName()));

    rsp = CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .setCreateNodeSet(String.join(",", nodeSet))
        .process(cluster.getSolrClient());
    assertTrue(rsp.isSuccess());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
    // make sure the primary replicas were placed on the nodeset
    DocCollection primary = cloudManager.getClusterStateProvider().getClusterState().getCollection(COLLECTION);
    primary.forEachReplica((shard, replica) ->
        assertTrue("primary replica not on secondary node!", nodeSet.contains(replica.getNodeName())));

    // try deleting secondary replica from node without the primary replica
    Optional<String> onlySecondaryReplica = secondary.getReplicas().stream()
        .filter(replica -> !nodeSet.contains(replica.getNodeName()))
        .map(replica -> replica.getName()).findFirst();
    assertTrue("no secondary node without primary replica", onlySecondaryReplica.isPresent());

    rsp = CollectionAdminRequest.deleteReplica(SECONDARY_COLLECTION, "shard1", onlySecondaryReplica.get())
        .process(cluster.getSolrClient());
    assertTrue("delete of a lone secondary replica should succeed", rsp.isSuccess());

    // try deleting secondary replica from node WITH the primary replica - should fail
    Optional<String> secondaryWithPrimaryReplica = secondary.getReplicas().stream()
        .filter(replica -> nodeSet.contains(replica.getNodeName()))
        .map(replica -> replica.getName()).findFirst();
    assertTrue("no secondary node with primary replica", secondaryWithPrimaryReplica.isPresent());
    try {
      rsp = CollectionAdminRequest.deleteReplica(SECONDARY_COLLECTION, "shard1", secondaryWithPrimaryReplica.get())
          .process(cluster.getSolrClient());
      fail("should have failed: " + rsp);
    } catch (Exception e) {
      assertTrue(e.toString(), e.toString().contains("co-located with replicas"));
    }

    // try deleting secondary collection
    try {
      rsp = CollectionAdminRequest.deleteCollection(SECONDARY_COLLECTION)
          .process(cluster.getSolrClient());
      fail("should have failed: " + rsp);
    } catch (Exception e) {
      assertTrue(e.toString(), e.toString().contains("colocated collection"));
    }
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
    NodeMetric<String> someMetricKey = new NodeMetricImpl<>("solr.jvm:system.properties:user.name");
    String sysprop = "user.name";
    Set<String> potentialEnvVars = Set.of("PWD", "TMPDIR", "TMP", "TEMP", "USER", "USERNAME", "HOME");

    String envVar = null;
    for (String env : potentialEnvVars) {
      if (System.getenv(env) != null) {
        envVar = env;
        break;
      }
    }
    if (envVar == null) {
      fail("None of the potential env vars exist? " + potentialEnvVars);
    }
    attributeFetcher
        .fetchFrom(cluster.getLiveNodes())
        .requestNodeMetric(NodeMetricImpl.HEAP_USAGE)
        .requestNodeMetric(NodeMetricImpl.SYSLOAD_AVG)
        .requestNodeMetric(NodeMetricImpl.NUM_CORES)
        .requestNodeMetric(NodeMetricImpl.FREE_DISK_GB)
        .requestNodeMetric(NodeMetricImpl.TOTAL_DISK_GB)
        .requestNodeMetric(NodeMetricImpl.AVAILABLE_PROCESSORS)
        .requestNodeMetric(someMetricKey)
        .requestNodeSystemProperty(sysprop)
        .requestNodeEnvironmentVariable(envVar)
        .requestCollectionMetrics(collection, Set.of(ReplicaMetricImpl.INDEX_SIZE_GB, ReplicaMetricImpl.QUERY_RATE_1MIN, ReplicaMetricImpl.UPDATE_RATE_1MIN));
    AttributeValues attributeValues = attributeFetcher.fetchAttributes();
    String userName = System.getProperty("user.name");
    String envVarValue = System.getenv(envVar);
    // node metrics
    for (Node node : cluster.getLiveNodes()) {
      Optional<Double> doubleOpt = attributeValues.getNodeMetric(node, NodeMetricImpl.HEAP_USAGE);
      assertTrue("heap usage", doubleOpt.isPresent());
      assertTrue("heap usage should be 0 < heapUsage < 100 but was " + doubleOpt, doubleOpt.get() > 0 && doubleOpt.get() < 100);
      doubleOpt = attributeValues.getNodeMetric(node, NodeMetricImpl.TOTAL_DISK_GB);
      assertTrue("total disk", doubleOpt.isPresent());
      assertTrue("total disk should be > 0 but was " + doubleOpt, doubleOpt.get() > 0);
      doubleOpt = attributeValues.getNodeMetric(node, NodeMetricImpl.FREE_DISK_GB);
      assertTrue("free disk", doubleOpt.isPresent());
      assertTrue("free disk should be > 0 but was " + doubleOpt, doubleOpt.get() > 0);
      Optional<Integer> intOpt = attributeValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES);
      assertTrue("cores", intOpt.isPresent());
      assertTrue("cores should be > 0", intOpt.get() > 0);
      assertTrue("systemLoadAverage 2", attributeValues.getNodeMetric(node, NodeMetricImpl.SYSLOAD_AVG).isPresent());
      assertTrue("availableProcessors", attributeValues.getNodeMetric(node, NodeMetricImpl.AVAILABLE_PROCESSORS).isPresent());
      Optional<String> userNameOpt = attributeValues.getNodeMetric(node, someMetricKey);
      assertTrue("user.name", userNameOpt.isPresent());
      assertEquals("userName", userName, userNameOpt.get());
      Optional<String> syspropOpt = attributeValues.getSystemProperty(node, sysprop);
      assertTrue("sysprop", syspropOpt.isPresent());
      assertEquals("user.name sysprop", userName, syspropOpt.get());
      Optional<String> envVarOpt = attributeValues.getEnvironmentVariable(node, envVar);
      assertTrue("envVar", envVarOpt.isPresent());
      assertEquals("envVar " + envVar, envVarValue, envVarOpt.get());
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
        Optional<Double> indexSizeOpt = replicaMetrics.getReplicaMetric(ReplicaMetricImpl.INDEX_SIZE_GB);
        assertTrue("indexSize", indexSizeOpt.isPresent());
        assertTrue("wrong type, expected Double but was " + indexSizeOpt.get().getClass(), indexSizeOpt.get() instanceof Double);
        assertTrue("indexSize should be > 0 but was " + indexSizeOpt.get(), indexSizeOpt.get() > 0);
        assertTrue("indexSize should be < 0.01 but was " + indexSizeOpt.get(), indexSizeOpt.get() < 0.01);

        assertNotNull("queryRate", replicaMetrics.getReplicaMetric(ReplicaMetricImpl.QUERY_RATE_1MIN));
        assertNotNull("updateRate", replicaMetrics.getReplicaMetric(ReplicaMetricImpl.UPDATE_RATE_1MIN));
      });
    });
  }
}
