package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementConfig;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory;
import org.apache.solr.cluster.placement.plugins.MinimizeCoresPlacementFactory;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonMap;

/**
 *
 */
public class PlacementPluginIntegrationTest extends SolrCloudTestCase {

  private static final String COLLECTION = PlacementPluginIntegrationTest.class.getName() + "_collection";

  private static ClusterProperties clusterProperties;
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
    clusterProperties = new ClusterProperties(cluster.getZkClient());
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
          .withPayload("{remove: " + PlacementPluginFactory.PLUGIN_NAME + "}")
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
    collection.forEachReplica((shard, replica) -> {
      coresByNode.computeIfAbsent(replica.getNodeName(), n -> new AtomicInteger()).incrementAndGet();
    });
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
    PluginMeta plugin = new PluginMeta();
    plugin.name = PlacementPluginFactory.PLUGIN_NAME;
    plugin.klass = MinimizeCoresPlacementFactory.class.getName();
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("add", plugin))
        .build();
    req.process(cluster.getSolrClient());

    PlacementPluginFactory pluginFactory = cc.getPlacementPluginFactory();
    assertTrue("wrong type " + pluginFactory.getClass().getName(), pluginFactory instanceof PlacementPluginFactoryLoader.DelegatingPlacementPluginFactory);
    PlacementPluginFactoryLoader.DelegatingPlacementPluginFactory wrapper = (PlacementPluginFactoryLoader.DelegatingPlacementPluginFactory) pluginFactory;
    // should already have some updates
    int version = wrapper.getVersion();
    assertTrue("wrong version " + version, version > 0);
    PlacementPluginFactory factory = wrapper.getDelegate();
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

    version = waitForVersionChange(version, wrapper);

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

    version = waitForVersionChange(version, wrapper);
    factory = wrapper.getDelegate();
    assertTrue("wrong type " + factory.getClass().getName(), factory instanceof AffinityPlacementFactory);
    config = ((AffinityPlacementFactory) factory).getConfig();
    assertEquals("minimalFreeDiskGB", 3, config.minimalFreeDiskGB);
    assertEquals("prioritizedFreeDiskGB", 4, config.prioritizedFreeDiskGB);

    // remove plugin
    req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload("{remove: " + PlacementPluginFactory.PLUGIN_NAME + "}")
        .build();
    req.process(cluster.getSolrClient());
    version = waitForVersionChange(version, wrapper);
    factory = wrapper.getDelegate();
    assertNull("no factory should be present", factory);
  }

  private int waitForVersionChange(int currentVersion, PlacementPluginFactoryLoader.DelegatingPlacementPluginFactory wrapper) throws Exception {
    TimeOut timeout = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (!timeout.hasTimedOut()) {
      int newVersion = wrapper.getVersion();
      if (newVersion < currentVersion) {
        throw new Exception("Invalid version - went back! currentVersion=" + currentVersion +
            " newVersion=" + newVersion);
      } else if (currentVersion < newVersion) {
        return newVersion;
      }
      timeout.sleep(200);
    }
    throw new TimeoutException("version didn't change in time, currentVersion=" + currentVersion);
  }
}
