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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
public class CollectionTooManyReplicasTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void deleteCollections() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  public void testAddTooManyReplicas() throws Exception {
    final String collectionName = "TooManyReplicasInSeveralFlavors";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());

    // I have two replicas, one for each shard

    // Curiously, I should be able to add a bunch of replicas if I specify the node, even more than maxShardsPerNode
    // Just get the first node any way we can.
    // Get a node to use for the "node" parameter.
    String nodeName = getAllNodeNames(collectionName).get(0);

    // Add a replica using the "node" parameter (no "too many replicas check")
    // this node should have 2 replicas on it
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(nodeName)
        .process(cluster.getSolrClient());

    // Three replicas so far, should be able to create another one "normally"
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .process(cluster.getSolrClient());

    // This one should fail though, no "node" parameter specified
    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
          .process(cluster.getSolrClient());
    });

    assertTrue("Should have gotten the right error message back",
          e.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));


    // Oddly, we should succeed next just because setting property.name will not check for nodes being "full up"
    // TODO: Isn't this a bug?
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .withProperty("name", "bogus2")
        .setNode(nodeName)
        .process(cluster.getSolrClient());

    DocCollection collectionState = getCollectionState(collectionName);
    Slice slice = collectionState.getSlice("shard1");
    Replica replica = getRandomReplica(slice, r -> r.getCoreName().equals("bogus2"));
    assertNotNull("Should have found a replica named 'bogus2'", replica);
    assertEquals("Replica should have been put on correct core", nodeName, replica.getNodeName());

    // Shard1 should have 4 replicas
    assertEquals("There should be 4 replicas for shard 1", 4, slice.getReplicas().size());

    // And let's fail one more time because to ensure that the math doesn't do weird stuff it we have more replicas
    // than simple calcs would indicate.
    Exception e2 = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
          .process(cluster.getSolrClient());
    });

    assertTrue("Should have gotten the right error message back",
        e2.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));

    // wait for recoveries to finish, for a clean shutdown - see SOLR-9645
    waitForState("Expected to see all replicas active", collectionName, (n, c) -> {
      for (Replica r : c.getReplicas()) {
        if (r.getState() != Replica.State.ACTIVE)
          return false;
      }
      return true;
    });
  }

  @Test
  public void testAddShard() throws Exception {

    String collectionName = "TooManyReplicasWhenAddingShards";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "shardstart", 2)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());

    // We have two nodes, maxShardsPerNode is set to 2. Therefore, we should be able to add 2 shards each with
    // two replicas, but fail on the third.
    CollectionAdminRequest.createShard(collectionName, "shard1")
        .process(cluster.getSolrClient());

    // Now we should have one replica on each Jetty, add another to reach maxShardsPerNode
    CollectionAdminRequest.createShard(collectionName, "shard2")
        .process(cluster.getSolrClient());

    // Now fail to add the third as it should exceed maxShardsPerNode
    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createShard(collectionName, "shard3")
          .process(cluster.getSolrClient());
    });
    assertTrue("Should have gotten the right error message back",
        e.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));

    // Hmmm, providing a nodeset also overrides the checks for max replicas, so prove it.
    List<String> nodes = getAllNodeNames(collectionName);

    CollectionAdminRequest.createShard(collectionName, "shard4")
        .setNodeSet(StringUtils.join(nodes, ","))
        .process(cluster.getSolrClient());

    // And just for yucks, insure we fail the "regular" one again.
    Exception e2 = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createShard(collectionName, "shard5")
          .process(cluster.getSolrClient());
    });
    assertTrue("Should have gotten the right error message back",
        e2.getMessage().contains("given the current number of live nodes and a maxShardsPerNode of"));

    // And finally, insure that there are all the replcias we expect. We should have shards 1, 2 and 4 and each
    // should have exactly two replicas
    waitForState("Expected shards shardstart, 1, 2 and 4, each with two active replicas", collectionName, (n, c) -> {
      return DocCollection.isFullyActive(n, c, 4, 2);
    });
    Map<String, Slice> slices = getCollectionState(collectionName).getSlicesMap();
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
  public void testDownedShards() throws Exception {
    String collectionName = "TooManyReplicasWhenAddingDownedNode";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "shardstart", 1)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());

    // Shut down a Jetty, I really don't care which
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    String deadNode = jetty.getBaseUrl().toString();
    cluster.stopJettySolrRunner(jetty);

    try {

      // Adding a replica on a dead node should fail
      Exception e1 = expectThrows(Exception.class, () -> {
        CollectionAdminRequest.addReplicaToShard(collectionName, "shardstart")
            .setNode(deadNode)
            .process(cluster.getSolrClient());
      });
      assertTrue("Should have gotten a message about shard not ",
          e1.getMessage().contains("At least one of the node(s) specified are not currently active, no action taken."));

      // Should also die if we just add a shard
      Exception e2 = expectThrows(Exception.class, () -> {
        CollectionAdminRequest.createShard(collectionName, "shard1")
            .setNodeSet(deadNode)
            .process(cluster.getSolrClient());
      });

      assertTrue("Should have gotten a message about shard not ",
          e2.getMessage().contains("At least one of the node(s) specified are not currently active, no action taken."));
    }
    finally {
      cluster.startJettySolrRunner(jetty);
    }
  }

  private List<String> getAllNodeNames(String collectionName) throws KeeperException, InterruptedException {
    DocCollection state = getCollectionState(collectionName);
    return state.getReplicas().stream().map(Replica::getNodeName).distinct().collect(Collectors.toList());
  }

}
