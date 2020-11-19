package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.plugins.SamplePluginMinimizeCores;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonMap;

/**
 *
 */
public class PlacementPluginIntegrationTest extends SolrCloudTestCase {

  private static final String COLLECTION = PlacementPluginIntegrationTest.class.getName() + "_collection";

  private static ClusterProperties clusterProperties;
  private static SolrCloudManager cloudManager;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // placement plugins need metrics
    System.setProperty("metricsEnabled", "true");
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    clusterProperties = new ClusterProperties(cluster.getZkClient());
  }

  @After
  public void cleanup() throws Exception {
    cluster.deleteAllCollections();
    V2Request req = new V2Request.Builder("/cluster")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("set-placement-plugin", Map.of()))
        .build();
    req.process(cluster.getSolrClient());

  }

  @Test
  public void testMinimizeCores() throws Exception {
    Map<String, Object> config = Map.of(PlacementPluginConfig.FACTORY_CLASS, SamplePluginMinimizeCores.Factory.class.getName());
    V2Request req = new V2Request.Builder("/cluster")
        .forceV2(true)
        .POST()
        .withPayload(singletonMap("set-placement-plugin", config))
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

}
