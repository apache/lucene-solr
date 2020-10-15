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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.CommonTestInjection;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.component.TrackingShardHandlerFactory;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.rule.ImplicitSnitch.SYSPROP;

public class RoutingToNodesWithPropertiesTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PROP_NAME = SYSPROP + "zone";
  final static String COLLECTION = "coll";

  private final List<String> zone1Nodes = new ArrayList<>();
  private final List<String> zone2Nodes = new ArrayList<>();
  private final LinkedList<TrackingShardHandlerFactory.ShardRequestAndParams> zone1Queue = new LinkedList<>();
  private final LinkedList<TrackingShardHandlerFactory.ShardRequestAndParams> zone2Queue = new LinkedList<>();

  @Before
  public void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    CommonTestInjection.setAdditionalProps(ImmutableMap.of("zone", "us-west1"));
    configureCluster(2)
        .withSolrXml(TEST_PATH().resolve("solr-trackingshardhandler.xml"))
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    zone1Nodes.addAll(cluster.getJettySolrRunners().stream().map(JettySolrRunner::getNodeName).collect(Collectors.toSet()));
    CommonTestInjection.setAdditionalProps(ImmutableMap.of("zone", "us-west2"));
    zone2Nodes.add(cluster.startJettySolrRunner().getNodeName());
    zone2Nodes.add(cluster.startJettySolrRunner().getNodeName());

    String commands =  "{set-cluster-policy :[{" +
        "    'replica':'#EQUAL'," +
        "    'shard':'#EACH'," +
        "    'sysprop.zone':'#EACH'}]}";

    @SuppressWarnings({"rawtypes"})
    SolrRequest req = CloudTestUtils.AutoScalingRequest.create(SolrRequest.METHOD.POST, commands);
    cluster.getSolrClient().request(req);

    CollectionAdminRequest.createCollection(COLLECTION, 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);

    // Checking putting replicas
    for (Slice slice : getCollectionState(COLLECTION).getSlices()) {
      int numReplicaInZone1 = 0;
      int numReplicaInZone2 = 0;
      for (Replica replica : slice.getReplicas()) {
        if (zone1Nodes.contains(replica.getNodeName()))
          numReplicaInZone1++;
        if (zone2Nodes.contains(replica.getNodeName()))
          numReplicaInZone2++;
      }

      assertEquals(1, numReplicaInZone1);
      assertEquals(1, numReplicaInZone2);
    }

    // check inject props
    try (SolrCloudManager cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(cluster.getZkClient()),
        cluster.getSolrClient())) {
      for (String zone1Node: zone1Nodes) {
        NodeStateProvider nodeStateProvider = cloudManager.getNodeStateProvider();
        Map<String, Object> map  = nodeStateProvider.getNodeValues(zone1Node, Collections.singletonList(PROP_NAME));
        assertEquals("us-west1", map.get(PROP_NAME));
      }

      for (String zone2Node: zone2Nodes) {
        NodeStateProvider nodeStateProvider = cloudManager.getNodeStateProvider();
        Map<String, Object> map = nodeStateProvider.getNodeValues(zone2Node, Collections.singletonList(PROP_NAME));
        assertEquals("us-west2", map.get(PROP_NAME));
      }

      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        if (zone1Nodes.contains(jetty.getNodeName())) {
          ((TrackingShardHandlerFactory)jetty.getCoreContainer().getShardHandlerFactory()).setTrackingQueue(zone1Queue);
        } else {
          ((TrackingShardHandlerFactory)jetty.getCoreContainer().getShardHandlerFactory()).setTrackingQueue(zone2Queue);
        }
      }

      for (int i = 0; i < 20; i++) {
        new UpdateRequest()
            .add("id", String.valueOf(i))
            .process(cluster.getSolrClient(), COLLECTION);
      }

      new UpdateRequest()
          .commit(cluster.getSolrClient(), COLLECTION);
    }
  }

  @After
  public void after() {
    TestInjection.reset();
  }

  @Test
  public void test() throws Exception {
    final int NUM_TRY = 10;
    CollectionAdminRequest
        .setClusterProperty(ZkStateReader.DEFAULT_SHARD_PREFERENCES, ShardParams.SHARDS_PREFERENCE_NODE_WITH_SAME_SYSPROP +":"+PROP_NAME)
        .process(cluster.getSolrClient());
    {
      TimeOut timeOut = new TimeOut(20, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      timeOut.waitFor("Timeout waiting for sysprops are cached in all nodes", () -> {
        int total = 0;
        for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
          total += runner.getCoreContainer().getZkController().getSysPropsCacher().getCacheSize();
        }
        return total == cluster.getJettySolrRunners().size() * cluster.getJettySolrRunners().size();
      });
    }

    for (int i = 0; i <  NUM_TRY; i++) {
      SolrQuery qRequest = new SolrQuery("*:*");
      ModifiableSolrParams qParams = new ModifiableSolrParams();
      qParams.add(ShardParams.SHARDS_INFO, "true");
      qRequest.add(qParams);
      QueryResponse qResponse = cluster.getSolrClient().query(COLLECTION, qRequest);

      Object shardsInfo = qResponse.getResponse().get(ShardParams.SHARDS_INFO);
      assertNotNull("Unable to obtain "+ShardParams.SHARDS_INFO, shardsInfo);
      SimpleOrderedMap<?> shardsInfoMap = (SimpleOrderedMap<?>)shardsInfo;
      String firstReplicaAddr = ((SimpleOrderedMap) shardsInfoMap.getVal(0)).get("shardAddress").toString();
      String secondReplicaAddr = ((SimpleOrderedMap) shardsInfoMap.getVal(1)).get("shardAddress").toString();
      boolean firstReplicaInZone1 = false;
      boolean secondReplicaInZone1 = false;
      for (String zone1Node : zone1Nodes) {
        zone1Node = zone1Node.replace("_solr", "");
        firstReplicaInZone1 = firstReplicaInZone1 || firstReplicaAddr.contains(zone1Node);
        secondReplicaInZone1 = secondReplicaInZone1 || secondReplicaAddr.contains(zone1Node);
      }

      assertEquals(firstReplicaInZone1, secondReplicaInZone1);
    }

    // intense asserting using TrackingShardHandlerFactory
    assertRoutingToSameZone();

    // Cachers should be stop running
    CollectionAdminRequest
        .setClusterProperty(ZkStateReader.DEFAULT_SHARD_PREFERENCES, ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE+":PULL")
        .process(cluster.getSolrClient());
    {
      TimeOut timeOut = new TimeOut(20, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      timeOut.waitFor("Timeout waiting for sysPropsCache stop", () -> {
        int numNodeStillRunningCache = 0;
        for (JettySolrRunner runner: cluster.getJettySolrRunners()) {
          if (runner.getCoreContainer().getZkController().getSysPropsCacher().isRunning()) {
            numNodeStillRunningCache++;
          }
        }
        return numNodeStillRunningCache == 0;
      });
    }

    // Testing disable default shard preferences
    CollectionAdminRequest
        .setClusterProperty(ZkStateReader.DEFAULT_SHARD_PREFERENCES, null)
        .process(cluster.getSolrClient());
    {
      TimeOut timeOut = new TimeOut(20, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      timeOut.waitFor("Timeout waiting cluster properties get updated", () -> {
        int numNodeGetUpdatedPref = 0;
        int numNodeStillRunningCache = 0;
        for (JettySolrRunner runner: cluster.getJettySolrRunners()) {
          if (runner.getCoreContainer().getZkController()
              .getZkStateReader().getClusterProperties().containsKey(ZkStateReader.DEFAULT_SHARD_PREFERENCES)) {
            numNodeGetUpdatedPref++;
          }
          if (runner.getCoreContainer().getZkController().getSysPropsCacher().isRunning()) {
            numNodeStillRunningCache++;
          }
        }
        return numNodeGetUpdatedPref == 0 && numNodeStillRunningCache == 0;
      });
    }

  }

  private void assertRoutingToSameZone() {
    for (TrackingShardHandlerFactory.ShardRequestAndParams sreq: zone1Queue) {
      String firstNode = sreq.shard.split("\\|")[0];
      assertTrue(zone1Nodes.stream().anyMatch(s -> firstNode.contains(s.replace('_','/'))));
    }
    for (TrackingShardHandlerFactory.ShardRequestAndParams sreq: zone2Queue) {
      String firstNode = sreq.shard.split("\\|")[0];
      assertTrue(zone2Nodes.stream().anyMatch(s -> firstNode.contains(s.replace('_','/'))));
    }
  }
}
