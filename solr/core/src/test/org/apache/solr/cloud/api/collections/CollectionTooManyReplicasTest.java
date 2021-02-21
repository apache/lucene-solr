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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slow
@LuceneTestCase.AwaitsFix(bugUrl = "MRM TODO: - relying on maxshardspernode enforcement, just delete this test")
public class CollectionTooManyReplicasTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
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
    Exception e = LuceneTestCase.expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createShard(collectionName, "shard3")
          .process(cluster.getSolrClient());
    });
    assertTrue("Should have gotten the right error message back",
        e.getMessage().contains("given the current number of eligible live nodes"));

    // Hmmm, providing a nodeset also overrides the checks for max replicas, so prove it.
    List<String> nodes = getAllNodeNames(collectionName);

    CollectionAdminRequest.createShard(collectionName, "shard4")
        .setNodeSet(String.join(",", nodes))
        .process(cluster.getSolrClient());

    // And just for yucks, insure we fail the "regular" one again.
    Exception e2 = LuceneTestCase.expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createShard(collectionName, "shard5")
          .process(cluster.getSolrClient());
    });
    assertTrue("Should have gotten the right error message back",
        e2.getMessage().contains("given the current number of eligible live nodes"));

    // And finally, ensure that there are all the replicas we expect. We should have shards 1, 2 and 4 and each
    // should have exactly two replicas
    cluster.waitForActiveCollection(collectionName, 4, 8);
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
  @LuceneTestCase.Nightly // TODO: investigate why this can be 15s +
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
      Exception e1 = LuceneTestCase.expectThrows(Exception.class, () -> {
        CollectionAdminRequest.addReplicaToShard(collectionName, "shardstart")
            .setNode(deadNode)
            .process(cluster.getSolrClient());
      });
      assertTrue("Should have gotten a message about shard not currently active: " + e1.toString(),
          e1.toString().contains("At least one of the node(s) specified [" + deadNode + "] are not currently active in"));

      // Should also die if we just add a shard
      Exception e2 = LuceneTestCase.expectThrows(Exception.class, () -> {
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
