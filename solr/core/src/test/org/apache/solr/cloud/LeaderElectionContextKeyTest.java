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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.hamcrest.CoreMatchers;
import org.junit.BeforeClass;
import org.junit.Test;

public class LeaderElectionContextKeyTest extends SolrCloudTestCase {

  private static final String TEST_COLLECTION_1 = "testCollection1";
  private static final String TEST_COLLECTION_2 = "testCollection2";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    for (int i = 1; i <= 2; i++) {
      // Create two collections with same order of requests, no parallel
      // therefore Assign.buildCoreNodeName will create same coreNodeName
      CollectionAdminRequest
          .createCollection("testCollection"+i, "config", 2, 1)
          .setMaxShardsPerNode(100)
          .setCreateNodeSet("")
          .process(cluster.getSolrClient());
      CollectionAdminRequest
          .addReplicaToShard("testCollection"+i, "shard1")
          .process(cluster.getSolrClient());
      CollectionAdminRequest
          .addReplicaToShard("testCollection"+i, "shard2")
          .process(cluster.getSolrClient());
    }

    AbstractDistribZkTestBase.waitForRecoveriesToFinish("testCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("testCollection2", cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
  }

  @Test
  public void test() throws KeeperException, InterruptedException, IOException, SolrServerException {
    ZkStateReader stateReader = cluster.getSolrClient().getZkStateReader();
    stateReader.forceUpdateCollection(TEST_COLLECTION_1);
    ClusterState clusterState = stateReader.getClusterState();
    // The test assume that TEST_COLLECTION_1 and TEST_COLLECTION_2 will have identical layout
    // ( same replica's name on every shard )
    for (int i = 1; i <= 2; i++) {
      String coll1ShardiLeader = clusterState.getCollection(TEST_COLLECTION_1).getLeader("shard"+i).getName();
      String coll2ShardiLeader = clusterState.getCollection(TEST_COLLECTION_2).getLeader("shard"+i).getName();
      String assertMss = String.format(Locale.ROOT, "Expect %s and %s each have a replica with same name on shard %s",
          coll1ShardiLeader, coll2ShardiLeader, "shard"+i);
      assertEquals(
          assertMss,
          coll1ShardiLeader,
          coll2ShardiLeader
      );
    }

    String shard = "shard" + String.valueOf(random().nextInt(2) + 1);
    Replica replica = clusterState.getCollection(TEST_COLLECTION_1).getLeader(shard);
    assertNotNull(replica);

    try (SolrClient shardLeaderClient = new HttpSolrClient.Builder(replica.get("base_url").toString()).build()) {
      assertEquals(1L, getElectionNodes(TEST_COLLECTION_1, shard, stateReader.getZkClient()).size());
      List<String> collection2Shard1Nodes = getElectionNodes(TEST_COLLECTION_2, "shard1", stateReader.getZkClient());
      List<String> collection2Shard2Nodes = getElectionNodes(TEST_COLLECTION_2, "shard2", stateReader.getZkClient());
      CoreAdminRequest.unloadCore(replica.getCoreName(), shardLeaderClient);
      // Waiting for leader election being kicked off
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
      boolean found = false;
      while (System.nanoTime() < timeout) {
        try {
          found = getElectionNodes(TEST_COLLECTION_1, shard, stateReader.getZkClient()).size() == 0;
          break;
        } catch (KeeperException.NoNodeException nne) {
          // ignore
        }
      }
      assertTrue(found);
      // There are no leader election was kicked off on testCollection2
      assertThat(collection2Shard1Nodes, CoreMatchers.is(getElectionNodes(TEST_COLLECTION_2, "shard1", stateReader.getZkClient())));
      assertThat(collection2Shard2Nodes, CoreMatchers.is(getElectionNodes(TEST_COLLECTION_2, "shard2", stateReader.getZkClient())));
    }
  }

  private List<String> getElectionNodes(String collection, String shard, SolrZkClient client) throws KeeperException, InterruptedException {
    return client.getChildren("/collections/"+collection+"/leader_elect/"+shard+LeaderElector.ELECTION_NODE, null, true);
  }
}
