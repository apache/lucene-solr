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
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
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

    CollectionAdminRequest
        .createCollection("testCollection1", "config", 2, 1)
        .setMaxShardsPerNode(1000)
        .process(cluster.getSolrClient());
    CollectionAdminRequest
        .createCollection("testCollection2", "config", 2, 1)
        .setMaxShardsPerNode(1000)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("testCollection1", cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish("testCollection2", cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
  }

  @Test
  public void test() throws KeeperException, InterruptedException, IOException, SolrServerException {
    ZkStateReader stateReader = cluster.getSolrClient().getZkStateReader();
    stateReader.forceUpdateCollection(TEST_COLLECTION_1);
    List<Replica> replicasOfCollection1 = stateReader.getClusterState().getCollection(TEST_COLLECTION_1).getReplicas();
    List<Replica> replicasOfCollection2 = stateReader.getClusterState().getCollection(TEST_COLLECTION_2).getReplicas();
    Replica replica = findLeaderReplicaWithDuplicatedName(replicasOfCollection1, replicasOfCollection2);
    assertNotNull(replica);

    SolrClient shardLeaderClient = new HttpSolrClient.Builder(replica.get("base_url").toString()).build();
    try {
      assertEquals(1L, getElectionNodes(TEST_COLLECTION_1, "shard1", stateReader.getZkClient()).size());
      List<String> collection2Shard1Nodes = getElectionNodes(TEST_COLLECTION_2, "shard1", stateReader.getZkClient());
      List<String> collection2Shard2Nodes = getElectionNodes(TEST_COLLECTION_2, "shard2", stateReader.getZkClient());
      CoreAdminRequest.unloadCore(replica.getCoreName(), shardLeaderClient);
      // Waiting for leader election being kicked off
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
      boolean found = false;
      while (System.nanoTime() < timeout) {
        try {
          found = getElectionNodes(TEST_COLLECTION_1, "shard1", stateReader.getZkClient()).size() == 0;
          break;
        } catch (KeeperException.NoNodeException nne) {
          // ignore
        }
      }
      assertTrue(found);
      // There are no leader election was kicked off on testCollection2
      assertThat(collection2Shard1Nodes, CoreMatchers.is(getElectionNodes(TEST_COLLECTION_2, "shard1", stateReader.getZkClient())));
      assertThat(collection2Shard2Nodes, CoreMatchers.is(getElectionNodes(TEST_COLLECTION_2, "shard2", stateReader.getZkClient())));
    } finally {
      shardLeaderClient.close();
    }
  }

  private Replica findLeaderReplicaWithDuplicatedName(List<Replica> replicas1, List<Replica> replicas2) {
    for (Replica replica1 : replicas1) {
      if (!replica1.containsKey("leader")) continue;
      for (Replica replica2 : replicas2) {
        if (replica1.getName().equals(replica2.getName())
            && replica1.get("base_url").equals(replica2.get("base_url"))
            && replica2.containsKey("leader")) {
          return replica1;
        }
      }
    }
    return null;
  }

  private List<String> getElectionNodes(String collection, String shard, SolrZkClient client) throws KeeperException, InterruptedException {
    return client.getChildren("/collections/"+collection+"/leader_elect/"+shard+LeaderElector.ELECTION_NODE, null, true);
  }
}
