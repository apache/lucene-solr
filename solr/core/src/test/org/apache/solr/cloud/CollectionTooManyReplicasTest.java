package org.apache.solr.cloud;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

@Slow
public class CollectionTooManyReplicasTest extends AbstractFullDistribZkTestBase {

  public CollectionTooManyReplicasTest() {
    sliceCount = 1;
  }

  @Test
  @ShardsFixed(num = 1)
  public void testAddTooManyReplicas() throws Exception {
    String collectionName = "TooManyReplicasInSeveralFlavors";
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create()
        .setCollectionName(collectionName)
        .setNumShards(2)
        .setReplicationFactor(1)
        .setMaxShardsPerNode(2)
        .setStateFormat(2);

    CollectionAdminResponse response = create.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    // Now I have the fixed Jetty plus the control instnace, I have two replicas, one for each shard

    // Curiously, I should be able to add a bunch of replicas if I specify the node, even more than maxShardsPerNode
    // Just get the first node any way we can.
    // Get a node to use for the "node" parameter.

    String nodeName = getAllNodeNames(collectionName).get(0);
    // Add a replica using the "node" parameter (no "too many replicas check")
    // this node should have 2 replicas on it
    CollectionAdminRequest.AddReplica addReplicaNode = new CollectionAdminRequest.AddReplica()
        .setCollectionName(collectionName)
        .setShardName("shard1")
        .setNode(nodeName);
    response = addReplicaNode.process(cloudClient);
    assertEquals(0, response.getStatus());

    // Three replicas so far, should be able to create another one "normally"
    CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica()
        .setCollectionName(collectionName)
        .setShardName("shard1");

    response = addReplica.process(cloudClient);
    assertEquals(0, response.getStatus());

    // This one should fail though, no "node" parameter specified
    try {
      addReplica.process(cloudClient);
      fail("Should have thrown an error because the nodes are full");
    } catch (HttpSolrClient.RemoteSolrException se) {
      assertTrue("Should have gotten the right error message back",
          se.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));
    }

    // Oddly, we should succeed next just because setting property.name will not check for nodes being "full up"
    Properties props = new Properties();
    props.setProperty("name", "bogus2");
    addReplicaNode.setProperties(props);
    response = addReplicaNode.process(cloudClient);
    assertEquals(0, response.getStatus());

    ZkStateReader zkStateReader = getCommonCloudSolrClient().getZkStateReader();
    zkStateReader.updateClusterState();
    Slice slice = zkStateReader.getClusterState().getSlicesMap(collectionName).get("shard1");

    Replica rep = null;
    for (Replica rep1 : slice.getReplicas()) { // Silly compiler
      if (rep1.get("core").equals("bogus2")) {
        rep = rep1;
        break;
      }
    }
    assertNotNull("Should have found a replica named 'bogus2'", rep);
    assertEquals("Replica should have been put on correct core", nodeName, rep.getNodeName());

    // Shard1 should have 4 replicas
    assertEquals("There should be 4 replicas for shard 1", 4, slice.getReplicas().size());

    // And let's fail one more time because to insure that the math doesn't do weird stuff it we have more replicas
    // than simple calcs would indicate.
    try {
      addReplica.process(cloudClient);
      fail("Should have thrown an error because the nodes are full");
    } catch (HttpSolrClient.RemoteSolrException se) {
      assertTrue("Should have gotten the right error message back",
          se.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));
    }
  }

  @Test
  @ShardsFixed(num = 2)
  public void testAddShard() throws Exception {
    String collectionName = "TooManyReplicasWhenAddingShards";
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create()
        .setCollectionName(collectionName)
        .setReplicationFactor(2)
        .setMaxShardsPerNode(2)
        .setStateFormat(2)
        .setRouterName("implicit")
        .setShards("shardstart");

    NamedList<Object> request = create.process(cloudClient).getResponse();

    assertTrue("Could not create the collection", request.get("success") != null);
    // We have two nodes, maxShardsPerNode is set to 2. Therefore, we should be able to add 2 shards each with
    // two replicas, but fail on the third.

    CollectionAdminRequest.CreateShard createShard = new CollectionAdminRequest.CreateShard()
        .setCollectionName(collectionName)
        .setShardName("shard1");
    CollectionAdminResponse resp = createShard.process(cloudClient);
    assertEquals(0, resp.getStatus());

    // Now we should have one replica on each Jetty, add another to reach maxShardsPerNode

    createShard = new CollectionAdminRequest.CreateShard()
        .setCollectionName(collectionName)
        .setShardName("shard2");
    resp = createShard.process(cloudClient);
    assertEquals(0, resp.getStatus());


    // Now fail to add the third as it should exceed maxShardsPerNode
    createShard = new CollectionAdminRequest.CreateShard()
        .setCollectionName(collectionName)
        .setShardName("shard3");
    try {
      createShard.process(cloudClient);
      fail("Should have exceeded the max number of replicas allowed");
    } catch (HttpSolrClient.RemoteSolrException se) {
      assertTrue("Should have gotten the right error message back",
          se.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));
    }

    // Hmmm, providing a nodeset also overrides the checks for max replicas, so prove it.
    List<String> nodes = getAllNodeNames(collectionName);

    createShard = new CollectionAdminRequest.CreateShard()
        .setCollectionName(collectionName)
        .setShardName("shard4")
        .setNodeSet(StringUtils.join(nodes, ","));
    resp = createShard.process(cloudClient);
    assertEquals(0, resp.getStatus());

    // And just for yucks, insure we fail the "regular" one again.
    createShard = new CollectionAdminRequest.CreateShard()
        .setCollectionName(collectionName)
        .setShardName("shard5");
    try {
      createShard.process(cloudClient);
      fail("Should have exceeded the max number of replicas allowed");
    } catch (HttpSolrClient.RemoteSolrException se) {
      assertTrue("Should have gotten the right error message back",
          se.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));
    }

    // And finally, insure that there are all the replcias we expect. We should have shards 1, 2 and 4 and each
    // should have exactly two replicas
    ZkStateReader zkStateReader = getCommonCloudSolrClient().getZkStateReader();
    zkStateReader.updateClusterState();
    Map<String, Slice> slices = zkStateReader.getClusterState().getSlicesMap(collectionName);
    assertEquals("There should be exaclty four slices", slices.size(), 4);
    assertNotNull("shardstart should exist", slices.get("shardstart"));
    assertNotNull("shard1 should exist", slices.get("shard1"));
    assertNotNull("shard2 should exist", slices.get("shard2"));
    assertNotNull("shard4 should exist", slices.get("shard4"));
    assertEquals("Shardstart should have exactly 2 replicas", 2, slices.get("shardstart").getReplicas().size());
    assertEquals("Shard1 should have exactly 2 replicas", 2, slices.get("shard1").getReplicas().size());
    assertEquals("Shard2 should have exactly 2 replicas", 2, slices.get("shard2").getReplicas().size());
    assertEquals("Shard4 should have exactly 2 replicas", 2, slices.get("shard4").getReplicas().size());

  }

  @Test
  @ShardsFixed(num = 2)
  public void testDownedShards() throws Exception {
    String collectionName = "TooManyReplicasWhenAddingDownedNode";
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create()
        .setCollectionName(collectionName)
        .setReplicationFactor(1)
        .setMaxShardsPerNode(2)
        .setStateFormat(2)
        .setRouterName("implicit")
        .setShards("shardstart");

    NamedList<Object> request = create.process(cloudClient).getResponse();

    assertTrue("Could not create the collection", request.get("success") != null);
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT)) {

      List<String> liveNodes = zkClient.getChildren("/live_nodes", null, true);

      // Shut down a Jetty, I really don't care which
      JettySolrRunner downJetty = jettys.get(r.nextInt(2));

      downJetty.stop();
      List<String> liveNodesNow = null;
      for (int idx = 0; idx < 150; ++idx) {
        liveNodesNow = zkClient.getChildren("/live_nodes", null, true);
        if (liveNodesNow.size() != liveNodes.size()) break;
        Thread.sleep(100);
      }
      List<String> deadNodes = new ArrayList<>(liveNodes);
      assertTrue("Should be a downed node", deadNodes.removeAll(liveNodesNow));
      liveNodes.removeAll(deadNodes);

      //OK, we've killed a node. Insure we get errors when we ask to create a replica or shard that involves it.
      // First try adding a  replica to the downed node.
      CollectionAdminRequest.AddReplica addReplicaNode = new CollectionAdminRequest.AddReplica()
          .setCollectionName(collectionName)
          .setShardName("shardstart")
          .setNode(deadNodes.get(0));

      try {
        addReplicaNode.process(cloudClient);
        fail("Should have gotten an exception");
      } catch (HttpSolrClient.RemoteSolrException se) {
        assertTrue("Should have gotten a message about shard not ",
            se.getMessage().contains("At least one of the node(s) specified are not currently active, no action taken."));
      }

      // Should also die if we just add a shard
      CollectionAdminRequest.CreateShard createShard = new CollectionAdminRequest.CreateShard()
          .setCollectionName(collectionName)
          .setShardName("shard1")
          .setNodeSet(deadNodes.get(0));
      try {
        createShard.process(cloudClient);
        fail("Should have gotten an exception");
      } catch (HttpSolrClient.RemoteSolrException se) {
        assertTrue("Should have gotten a message about shard not ",
            se.getMessage().contains("At least one of the node(s) specified are not currently active, no action taken."));
      }
      //downJetty.start();
    }
  }

  private List<String> getAllNodeNames(String collectionName) throws KeeperException, InterruptedException {
    ZkStateReader zkStateReader = getCommonCloudSolrClient().getZkStateReader();
    zkStateReader.updateClusterState();
    Slice slice = zkStateReader.getClusterState().getSlicesMap(collectionName).get("shard1");

    List<String> nodes = new ArrayList<>();
    for (Replica rep : slice.getReplicas()) {
      nodes.add(rep.getNodeName());
    }

    assertTrue("Should have some nodes!", nodes.size() > 0);
    return nodes;
  }

}
