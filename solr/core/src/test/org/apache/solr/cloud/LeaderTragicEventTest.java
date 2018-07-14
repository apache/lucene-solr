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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderTragicEventTest extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.mscheduler", "org.apache.solr.core.MockConcurrentMergeScheduler");

    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    cluster.getSolrClient().setDefaultCollection(COLLECTION);
  }

  @AfterClass
  public static void cleanup() {
    System.clearProperty("solr.mscheduler");
  }


  @Test
  public void test() throws Exception {
    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 1, 2)
        .process(cluster.getSolrClient());
    ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), COLLECTION, 120000);

    List<String> addedIds = new ArrayList<>();
    Replica oldLeader = corruptLeader(addedIds);

    waitForState("Timeout waiting for new replica become leader", COLLECTION, (liveNodes, collectionState) -> {
      Slice slice = collectionState.getSlice("shard1");

      if (slice.getReplicas().size() != 2) return false;
      if (slice.getLeader().getName().equals(oldLeader.getName())) return false;

      return true;
    });
    ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), COLLECTION, 120000);
    Slice shard = getCollectionState(COLLECTION).getSlice("shard1");
    assertNotSame(shard.getLeader().getNodeName(), oldLeader.getNodeName());
    assertEquals(getNonLeader(shard).getNodeName(), oldLeader.getNodeName());

    for (String id : addedIds) {
      assertNotNull(cluster.getSolrClient().getById(COLLECTION,id));
    }
    log.info("The test success oldLeader:{} currentState:{}", oldLeader, getCollectionState(COLLECTION));

    CollectionAdminRequest.deleteCollection(COLLECTION).process(cluster.getSolrClient());
  }

  private Replica corruptLeader(List<String> addedIds) throws IOException {
    DocCollection dc = getCollectionState(COLLECTION);
    Replica oldLeader = dc.getLeader("shard1");
    CoreContainer leaderCC = cluster.getReplicaJetty(oldLeader).getCoreContainer();
    SolrCore leaderCore = leaderCC.getCores().iterator().next();
    MockDirectoryWrapper dir = (MockDirectoryWrapper) leaderCore.getDirectoryFactory().get(leaderCore.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, leaderCore.getSolrConfig().indexConfig.lockType);
    leaderCore.getDirectoryFactory().release(dir);

    try (HttpSolrClient solrClient = new HttpSolrClient.Builder(dc.getLeader("shard1").getCoreUrl()).build()) {
      for (int i = 0; i < 100; i++) {
        new UpdateRequest()
            .add("id", i + "")
            .process(solrClient);
        solrClient.commit();
        addedIds.add(i + "");

        for (String file : dir.listAll()) {
          if (file.contains("segments_")) continue;
          if (file.endsWith("si")) continue;
          if (file.endsWith("fnm")) continue;
          if (random().nextBoolean()) continue;

          dir.corruptFiles(Collections.singleton(file));
        }
      }
    } catch (Exception e) {
      // Expected
    }
    return oldLeader;
  }

  private Replica getNonLeader(Slice slice) {
    if (slice.getReplicas().size() <= 1) return null;
    return slice.getReplicas(rep -> !rep.getName().equals(slice.getLeader().getName())).get(0);
  }

  @Test
  public void testOtherReplicasAreNotActive() throws Exception {
    int numReplicas = random().nextInt(2) + 1;
    // won't do anything if leader is the only one active replica in the shard
    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 1, numReplicas)
        .process(cluster.getSolrClient());
    ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), COLLECTION, 120000);

    JettySolrRunner otherReplicaJetty = null;
    if (numReplicas == 2) {
      Slice shard = getCollectionState(COLLECTION).getSlice("shard1");
      otherReplicaJetty = cluster.getReplicaJetty(getNonLeader(shard));
      otherReplicaJetty.stop();
      waitForState("Timeout waiting for replica get down", COLLECTION, (liveNodes, collectionState) -> getNonLeader(collectionState.getSlice("shard1")).getState() != Replica.State.ACTIVE);
    }

    Replica oldLeader = corruptLeader(new ArrayList<>());

    //TODO better way to test this
    Thread.sleep(5000);
    Replica leader = getCollectionState(COLLECTION).getSlice("shard1").getLeader();
    assertEquals(leader.getName(), oldLeader.getName());

    if (otherReplicaJetty != null) {
      // won't be able to do anything here, since this replica can't recovery from the leader
      otherReplicaJetty.start();
    }

    CollectionAdminRequest.deleteCollection(COLLECTION).process(cluster.getSolrClient());
  }


}
