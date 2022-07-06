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

package org.apache.solr.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;

@LogLevel("org.apache.solr.common.cloud.PerReplicaStatesOps=DEBUG")
public class FSPRSTest extends SolrCloudTestCase {

  public void testShardSplitWithNodeset() throws Exception {
    String COLL = "prs_shard_split_nodeset";
    MiniSolrCloudCluster cluster =
        configureCluster(2)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("conf2"))
            .configure();

    try {
      CollectionAdminRequest.createCollection(COLL, "conf", 2, 2)
          .setMaxShardsPerNode(4)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 2, 4);

      JettySolrRunner jetty = cluster.startJettySolrRunner();

      CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest
          .splitShard(COLL)
          .setCreateNodeSet(jetty.getNodeName())
          .setShardName("shard1");
      NamedList<Object> response = splitShard.process(cluster.getSolrClient()).getResponse();
      assertNotNull(response.get("success"));

      cluster.getSolrClient().getZkStateReader().waitForState(COLL, 10, TimeUnit.SECONDS,
          (liveNodes, collectionState) -> testColl(jetty, collectionState,  Arrays.asList("shard1_0", "shard1_1")));

      JettySolrRunner randomJetty = cluster.getRandomJetty(random());
      splitShard = CollectionAdminRequest
          .splitShard(COLL)
          .setCreateNodeSet(randomJetty.getNodeName())
          .setShardName("shard2");
      response = splitShard.process(cluster.getSolrClient()).getResponse();
      assertNotNull(response.get("success"));

      cluster.getSolrClient().getZkStateReader().waitForState(COLL, 10, TimeUnit.SECONDS,
          (liveNodes, collectionState) -> testColl(randomJetty, collectionState,  Arrays.asList("shard2_0", "shard2_1")));

    } finally {
      cluster.shutdown();
    }


  }

  private boolean testColl(JettySolrRunner jetty, DocCollection collectionState, Collection<String> sh) {
    Collection<String> set = new HashSet<>(sh);
    collectionState.forEachReplica((s, replica) -> {
      if (replica.getNodeName().equals(jetty.getNodeName()) &&
          !replica.isLeader() && set.contains(replica.slice)) {
        set.remove(replica.slice);
      }
    });

    return set.isEmpty();
  }

  public void testShardSplit() throws Exception {
    String COLL = "prs_shard_split";
    MiniSolrCloudCluster cluster =
        configureCluster(3)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("conf2"))
            .configure();
    int COLL_COUNT = 2;
    int SPLIT_COUNT = 1;
    
    try {
      for (int i = 0; i < COLL_COUNT; i++) {
        String C = COLL + i;
        CollectionAdminRequest.createCollection(C, "conf", 4, 1)
            .setMaxShardsPerNode(4)
            .process(cluster.getSolrClient());
        cluster.waitForActiveCollection(C, 4, 4);
      }
      for (int i = 0; i < COLL_COUNT; i++) {
        String C = COLL + i;
        CollectionAdminRequest.modifyCollection(C,
            Collections.singletonMap("perReplicaState", "true"))
            .process(cluster.getSolrClient());
        PerReplicaStates prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(C), cluster.getZkClient(), null);
        assertTrue(prs.states.size() >= 4);
      }

      for (int i=1; i<3; i++) {
        JettySolrRunner jetty = cluster.stopJettySolrRunner(i);
        cluster.startJettySolrRunner(jetty);
      }

      for (int i = 0; i < SPLIT_COUNT; i++) {
        String C = COLL + i;
        CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(C);
        splitShard.setShardName("shard1");
        NamedList<Object> response = splitShard.process(cluster.getSolrClient()).getResponse();
        assertNotNull(response.get("success"));
        for (int j = 0; j < 100; j++) {
          DocCollection docCollection = cluster.getSolrClient().getClusterStateProvider().getCollection(C);
          int shardCount = docCollection.getSlices().size();
          if (shardCount >= 5) break;
          else if (j >= 99) fail("split did not produce 2 shards shard count = " + shardCount);
          Thread.sleep(50);
        }
      }

      for (int i = 0; i < COLL_COUNT; i++) {
        String C = COLL + i;
        CollectionAdminRequest.modifyCollection(C,
            Collections.singletonMap("perReplicaState", "false"))
            .process(cluster.getSolrClient());
      }

      for (int i = 0; i < COLL_COUNT; i++) {
        String C = COLL + i;
        cluster.getSolrClient().getZkStateReader().waitForState(C, 10, TimeUnit.SECONDS, (liveNodes, c) -> {
          boolean[] result = new boolean[]{true};
          c.forEachReplica((s, replica) -> {
            if (!replica.isActive(liveNodes)) result[0] = false;
          });
          return result[0];
        });
      }

      for (int i = 0; i < COLL_COUNT; i++) {
        String C = COLL + i;
        PerReplicaStates prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(C), cluster.getZkClient(), null);
        assertEquals(0,prs.states.size());
      }
      for (int i = 0; i < COLL_COUNT; i++) {
        String C = COLL + i;
        CollectionAdminRequest.modifyCollection(C,
            Collections.singletonMap("perReplicaState", "true"))
            .process(cluster.getSolrClient());
      }


      for (int i = 0; i < COLL_COUNT; i++) {
        String C = COLL + i;
        cluster.getSolrClient().getZkStateReader().waitForState(C, 10, TimeUnit.SECONDS, (liveNodes, c) -> {
          boolean[] result = new boolean[]{true};
          c.forEachReplica((s, replica) -> {
            if (!replica.isActive(liveNodes)) result[0] = false;
          });
          return result[0];
        });
      }

    } finally {
      cluster.shutdown();
    }


  }

  public void testIntegration() throws Exception {
    final String SHARD1 = "shard1";
    final String SHARD1_0 = SHARD1 + "_0";
    final String SHARD1_1 = SHARD1 + "_1";
    String COLL = "prs_test_coll";
    MiniSolrCloudCluster cluster =
        configureCluster(3)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("conf2"))
            .configure();
    try {
      CollectionAdminRequest.createCollection(COLL, "conf", 1, 2)
          .setPerReplicaState(Boolean.FALSE)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 1, 2);

      UpdateRequest ur = new UpdateRequest();
      for(int i=0;i<10;i++) ur.add("id", ""+i);
      ur.commit(cluster.getSolrClient(), COLL);

      System.setProperty(CoreContainer.SOLR_QUERY_AGGREGATOR, "true");
      JettySolrRunner qaJetty = cluster.startJettySolrRunner();
      System.clearProperty(CoreContainer.SOLR_QUERY_AGGREGATOR);
      assertTrue(qaJetty.getCoreContainer().isQueryAggregator());

      try(HttpSolrClient client = (HttpSolrClient) qaJetty.newClient()){
        NavigableObject result = (NavigableObject) Utils.executeGET(client.getHttpClient(),
            qaJetty.getBaseUrl()+"/"+ COLL+"/select?q=*:*&wt=javabin", Utils.JAVABINCONSUMER
            );
        Collection<?> l = (Collection<?>) result._get("response", null);
        assertEquals(10, l.size());
      }
      CollectionAdminRequest.modifyCollection(COLL,
          Collections.singletonMap("perReplicaState", "true"))
          .process(cluster.getSolrClient());
      String collectionPath = ZkStateReader.getCollectionPath(COLL);
      PerReplicaStates prs = PerReplicaStates.fetch(collectionPath, cluster.getZkClient(), null);
      assertTrue(prs.states.size() > 0);


      DocCollection collection = cluster.getSolrClient().getClusterStateProvider().getCollection(COLL);
      Replica leader = collection.getReplica((s, replica) -> replica.isLeader());
      Replica r = collection.getReplica((s, replica) -> !replica.isLeader());
      CollectionAdminRequest.deleteReplica(COLL, SHARD1,
          r.getName())
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 1, 1);
      prs = PerReplicaStates.fetch(collectionPath, cluster.getZkClient(), null);
      assertEquals(1, prs.states.size());


      //MOVE in a replica by adding a replica first
      CollectionAdminRequest.addReplicaToShard(COLL, SHARD1)
          .setCreateNodeSet(r.getNodeName())
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 1, 2);

      // then delete the first replica replica
      CollectionAdminRequest.deleteReplica(COLL, SHARD1, leader.getName())
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 1, 1);

      cluster.getSolrClient().getZkStateReader().waitForState(COLL, 10, TimeUnit.SECONDS, (liveNodes, coll) -> {
        Replica newReplica = coll.getReplica(
            (s, replica) -> replica.getNodeName().equals(r.getNodeName()));

        assertNotNull(newReplica);
        PerReplicaStates prs1 = PerReplicaStates.fetch(collectionPath, cluster.getZkClient(), null);
        return prs1.states.size() == 1 && prs1.allActive() && prs1.get(newReplica.getName()).isLeader;
      });

      try (HttpSolrClient client = (HttpSolrClient) qaJetty.newClient()) {
        NavigableObject result = (NavigableObject) Utils.executeGET(client.getHttpClient(),
            qaJetty.getBaseUrl() + "/" + COLL + "/select?q=*:*&wt=javabin", Utils.JAVABINCONSUMER
        );
        Collection<?> l = (Collection<?>) result._get("response", null);
        assertEquals(10, l.size());
      }

      CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLL);
      splitShard.setShardName(SHARD1);
      NamedList<Object> response = splitShard.process(cluster.getSolrClient()).getResponse();
      assertNotNull(response.get("success"));
      for (int i = 0; i < 100; i++) {
        DocCollection docCollection = cluster.getSolrClient().getClusterStateProvider().getCollection(COLL);
        int shardCount = docCollection.getSlices().size();
        if (shardCount >= 2) break;
        else if (i >= 99) fail("split did not produce 2 shards shard count = " + shardCount);
        Thread.sleep(50);
      }
      CountDownLatch latch = new CountDownLatch(1);
      cluster.getSolrClient().getZkStateReader().registerCollectionStateWatcher(COLL, (liveNodes, collectionState) -> {
        Slice parent = collectionState.getSlice(SHARD1);
        Slice slice10 = collectionState.getSlice(SHARD1_0);
        Slice slice11 = collectionState.getSlice(SHARD1_1);
        if (slice10 != null && slice11 != null &&
            parent.getState() == Slice.State.INACTIVE &&
            slice10.getState() == Slice.State.ACTIVE &&
            slice11.getState() == Slice.State.ACTIVE) {
          latch.countDown();
          return true; // removes the watch
        }
        return false;
      });

      // then delete the parent replica replica
      CollectionAdminRequest.deleteShard(COLL, SHARD1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 2, 2);

      try (HttpSolrClient client = (HttpSolrClient) qaJetty.newClient()) {
        NavigableObject result = (NavigableObject) Utils.executeGET(client.getHttpClient(),
            qaJetty.getBaseUrl() + "/" + COLL + "/select?q=*:*&wt=javabin", Utils.JAVABINCONSUMER
        );
        Collection<?> l = (Collection<?>) result._get("response", null);
        assertEquals(10, l.size());
      }

      latch.await(1, TimeUnit.MINUTES);
      if (latch.getCount() != 0) {
        // sanity check
        fail("Sub-shards did not become active even after waiting for 1 minute");
      }

    } finally {
      cluster.shutdown();
    }
  }

}
