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
package org.apache.solr.cloud.api.collections;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
  @Ignore // Since maxShardsPerNode was removed in SOLR-12847 and autoscaling framework was removed in SOLR-14656, this test is broken
  public void testAddTooManyReplicas() throws Exception {

    final String collectionName = "TooManyReplicasInSeveralFlavors";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());

    // I have two replicas, one for each shard

    // Just get the first node any way we can.
    // Get a node to use for the "node" parameter.
    String nodeName = getAllNodeNames(collectionName).get(0);

    // Add a replica using the "node" parameter
    // this node should have 2 replicas on it
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(nodeName)
        .withProperty("name", "bogus2")
        .process(cluster.getSolrClient());

    // equivalent to maxShardsPerNode=1
    // String commands =  "{ set-cluster-policy: [ {replica: '<2', shard: '#ANY', node: '#ANY', strict: true} ] }";
    // cluster.getSolrClient().request(CloudTestUtils.AutoScalingRequest.create(SolrRequest.METHOD.POST, commands));

    // this should fail because the policy prevents it
    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
          .setNode(nodeName)
          .process(cluster.getSolrClient());
    });
    assertTrue(e.toString(), e.toString().contains("No node can satisfy"));

    // this should succeed because it places the replica on a different node
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .process(cluster.getSolrClient());


    DocCollection collectionState = getCollectionState(collectionName);
    Slice slice = collectionState.getSlice("shard1");
    Replica replica = getRandomReplica(slice, r -> r.getCoreName().equals("bogus2"));
    assertNotNull("Should have found a replica named 'bogus2'", replica);
    assertEquals("Replica should have been put on correct node", nodeName, replica.getNodeName());

    // Shard1 should have 2 replicas
    assertEquals("There should be 3 replicas for shard 1", 3, slice.getReplicas().size());

    // And let's fail one more time because to ensure that the math doesn't do weird stuff it we have more replicas
    // than simple calcs would indicate.
    Exception e2 = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
          .process(cluster.getSolrClient());
    });

    assertTrue("Should have gotten the right error message back",
        e2.getMessage().contains("No node can satisfy"));

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
  @Ignore // Since maxShardsPerNode was removed in SOLR-12847 and autoscaling framework was removed in SOLR-14656, this test is broken
  public void testAddShard() throws Exception {
    // equivalent to maxShardsPerNode=2
    // String commands =  "{ set-cluster-policy: [ {replica: '<3', shard: '#ANY', node: '#ANY', strict: true} ] }";
    // cluster.getSolrClient().request(CloudTestUtils.AutoScalingRequest.create(SolrRequest.METHOD.POST, commands));

    String collectionName = "TooManyReplicasWhenAddingShards";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "shardstart", 2)
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
        e.getMessage().contains("No node can satisfy the rules"));

    // Hmmm, providing a nodeset also overrides the checks for max replicas, so prove it.
    List<String> nodes = getAllNodeNames(collectionName);

    Exception e2 = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createShard(collectionName, "shard4")
          .setNodeSet(String.join(",", nodes))
          .process(cluster.getSolrClient());
    });
    assertTrue("Should have gotten the right error message back",
        e2.getMessage().contains("No node can satisfy the rules"));

//    // And just for yucks, insure we fail the "regular" one again.
    Exception e3 = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createShard(collectionName, "shard5")
          .process(cluster.getSolrClient());
    });
    assertTrue("Should have gotten the right error message back",
        e3.getMessage().contains("No node can satisfy the rules"));

    // And finally, ensure that there are all the replicas we expect. We should have shards 1, 2 and 4 and each
    // should have exactly two replicas
    waitForState("Expected shards shardstart, 1, 2, each with two active replicas", collectionName, (n, c) -> {
      return DocCollection.isFullyActive(n, c, 3, 2);
    });
    Map<String, Slice> slices = getCollectionState(collectionName).getSlicesMap();
    assertEquals("There should be exaclty three slices", slices.size(), 3);
    assertNotNull("shardstart should exist", slices.get("shardstart"));
    assertNotNull("shard1 should exist", slices.get("shard1"));
    assertNotNull("shard2 should exist", slices.get("shard2"));
    assertEquals("Shardstart should have exactly 2 replicas", 2, slices.get("shardstart").getReplicas().size());
    assertEquals("Shard1 should have exactly 2 replicas", 2, slices.get("shard1").getReplicas().size());
    assertEquals("Shard2 should have exactly 2 replicas", 2, slices.get("shard2").getReplicas().size());

  }

  @Test
  public void testDownedShards() throws Exception {
    String collectionName = "TooManyReplicasWhenAddingDownedNode";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "shardstart", 1)
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
      assertTrue("Should have gotten a message about shard not currently active: " + e1.toString(),
          e1.toString().contains("At least one of the node(s) specified [" + deadNode + "] are not currently active in"));

      // Should also die if we just add a shard
      Exception e2 = expectThrows(Exception.class, () -> {
        CollectionAdminRequest.createShard(collectionName, "shard1")
            .setNodeSet(deadNode)
            .process(cluster.getSolrClient());
      });

      assertTrue("Should have gotten a message about shard not currently active: " + e2.toString(),
          e2.toString().contains("At least one of the node(s) specified [" + deadNode + "] are not currently active in"));
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
