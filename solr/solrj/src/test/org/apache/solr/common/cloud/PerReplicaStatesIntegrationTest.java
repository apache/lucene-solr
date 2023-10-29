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

package org.apache.solr.common.cloud;


import java.lang.invoke.MethodHandles;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.cloud.DocCollection.PER_REPLICA_STATE;


/**
 * This test would be faster if we simulated the zk state instead.
 */
@Slow
@LogLevel("org.apache.solr.common.cloud.PerReplicaStatesOps=DEBUG;org.apache.solr.cloud.Overseer=INFO;org.apache.solr.common.cloud=INFO;org.apache.solr.cloud.api.collections=INFO;org.apache.solr.cloud.overseer=INFO;org.apache.solr.handler.admin=DEBUG")
public class PerReplicaStatesIntegrationTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public void testPerReplicaStateCollection() throws Exception {

    String testCollection = "perReplicaState_test";

    MiniSolrCloudCluster cluster =
        configureCluster(3)
            .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .configure();
    try {
      int liveNodes = cluster.getJettySolrRunners().size();
      CollectionAdminRequest.createCollection(testCollection, "conf", 2, 2)
          .setMaxShardsPerNode(liveNodes)
          .setPerReplicaState(Boolean.TRUE)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 2, 4);
      final SolrClient clientUnderTest = cluster.getSolrClient();
      final SolrPingResponse response = clientUnderTest.ping(testCollection);
      assertEquals("This should be OK", 0, response.getStatus());
      DocCollection c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      c.forEachReplica((s, replica) -> assertNotNull(replica.getReplicaState()));
      PerReplicaStates prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(testCollection), cluster.getZkClient(), null);
      assertEquals(4, prs.states.size());
      JettySolrRunner jsr = cluster.startJettySolrRunner();
      // Now let's do an add replica
      CollectionAdminRequest
          .addReplicaToShard(testCollection, "shard1")
          .process(cluster.getSolrClient());
      prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(testCollection), cluster.getZkClient(), null);
      assertEquals(5, prs.states.size());

      testCollection = "perReplicaState_testv2";
      new V2Request.Builder("/collections")
          .withMethod(POST)
          .withPayload("{create: {name: perReplicaState_testv2, config : conf, numShards : 2, nrtReplicas : 2, perReplicaState : true, maxShardsPerNode : 5}}")
          .build()
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 2, 4);
      c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      c.forEachReplica((s, replica) -> assertNotNull(replica.getReplicaState()));
      prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(testCollection), cluster.getZkClient(), null);
      assertEquals(4, prs.states.size());
    }finally {
      cluster.shutdown();
    }


  }

  public void testRestart() throws Exception {
    String testCollection = "prs_restart_test";
    MiniSolrCloudCluster cluster =
        configureCluster(1)
            .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .configure();
    try {
      CollectionAdminRequest.createCollection(testCollection, "conf", 1, 1)
          .setPerReplicaState(Boolean.TRUE)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 1, 1);

      DocCollection c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      c.forEachReplica((s, replica) -> assertNotNull(replica.getReplicaState()));
      String collectionPath = ZkStateReader.getCollectionPath(testCollection);
      PerReplicaStates prs = PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
      assertEquals(1, prs.states.size());

      JettySolrRunner jsr = cluster.startJettySolrRunner();
      assertEquals(2,cluster.getJettySolrRunners().size());

      // Now let's do an add replica
      CollectionAdminRequest
          .addReplicaToShard(testCollection, "shard1")
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 1, 2);
      prs = PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
      assertEquals(2, prs.states.size());
      c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      prs.states.forEachEntry((s, state) -> assertEquals(Replica.State.ACTIVE, state.state));

      String replicaName = null;
      for (Replica r : c.getSlice("shard1").getReplicas()) {
        if(r.getNodeName() .equals(jsr.getNodeName())) {
          replicaName = r.getName();
        }
      }

      if(replicaName != null) {
        log.info("restarting the node : {}, state.json v: {} downreplica :{}", jsr.getNodeName(), c.getZNodeVersion(), replicaName);
        jsr.stop();
        c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
        log.info("after down node, state.json v: {}", c.getZNodeVersion());
        prs =  PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
        PerReplicaStates.State st = prs.get(replicaName);
        assertNotEquals(Replica.State.ACTIVE, st.state);
        jsr.start();
        cluster.waitForActiveCollection(testCollection, 1, 2);
        prs =  PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
        prs.states.forEachEntry((s, state) -> assertEquals(Replica.State.ACTIVE, state.state));
      }

    } finally {
      cluster.shutdown();
    }

  }

  public void testMultipleTransitions() throws Exception {
    String COLL = "prs_modify_op_coll";
    MiniSolrCloudCluster cluster =
        configureCluster(2)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
            .configure();
    PerReplicaStates original = null;
    try {
      CollectionAdminRequest.createCollection(COLL, "conf", 3, 1)
          .setPerReplicaState(Boolean.TRUE)
          .setMaxShardsPerNode(2)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLL, 3, 3);

      PerReplicaStates prs1 = original =  PerReplicaStates.fetch(ZkStateReader.getCollectionPath(COLL), cluster.getZkClient(), null);
      log.info("prs1 : {}", prs1);

      CollectionAdminRequest.modifyCollection(COLL,
          Collections.singletonMap(PER_REPLICA_STATE, "false"))
          .process(cluster.getSolrClient());
      cluster.getSolrClient().getZkStateReader().waitForState(COLL, 5, TimeUnit.SECONDS,
          (liveNodes, collectionState) -> "false".equals(collectionState.getProperties().get(PER_REPLICA_STATE)));
      CollectionAdminRequest.modifyCollection(COLL,
          Collections.singletonMap(PER_REPLICA_STATE, "true"))
          .process(cluster.getSolrClient());
      cluster.getSolrClient().getZkStateReader().waitForState(COLL, 5, TimeUnit.SECONDS, (liveNodes, collectionState) -> {
        AtomicBoolean anyFail = new AtomicBoolean(false);
        PerReplicaStates prs2 = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(COLL), cluster.getZkClient(), null);
        prs2.states.forEachEntry((r, newState) -> {
          if(newState.getDuplicate() !=null) anyFail.set(true);
        });
        return !anyFail.get();
      });

    } finally {
      cluster.shutdown();

    }

  }
  public void testZkNodeVersions() throws Exception {
    String NONPRS_COLL = "non_prs_test_coll1";
    String PRS_COLL = "prs_test_coll2";
    MiniSolrCloudCluster cluster =
        configureCluster(3)
            .addConfig(
                "conf",
                getFile("solrj")
                    .toPath()
                    .resolve("solr")
                    .resolve("configsets")
                    .resolve("streaming")
                    .resolve("conf"))
            .configure();
    try {
      Stat stat = null;
      CollectionAdminRequest.createCollection(NONPRS_COLL, "conf", 10, 1)
          .setMaxShardsPerNode(10)
          .process(cluster.getSolrClient());
      stat = cluster.getZkClient().exists(ZkStateReader.getCollectionPath(NONPRS_COLL), null, true);
      log.info("");
      // the actual number can vary depending on batching
      assertTrue(stat.getVersion() >= 2);
      assertEquals(0, stat.getCversion());

      CollectionAdminRequest.createCollection(PRS_COLL, "conf", 10, 1)
          .setMaxShardsPerNode(10)
          .setPerReplicaState(Boolean.TRUE)
          .process(cluster.getSolrClient());
      stat = cluster.getZkClient().exists(ZkStateReader.getCollectionPath(PRS_COLL), null, true);
      // +1 after all replica are added with on state.json write to CreateCollectionCmd.setData()
      assertEquals(1, stat.getVersion());
      // For each replica:
      // +1 for ZkController#preRegister, in ZkController#publish, direct write PRS to down
      // +2 for runLeaderProcess, flip the replica to leader
      // +2 for ZkController#register, in ZkController#publish, direct write PRS to active
      // Hence 5 * 10 = 50. Take note that +1 for ADD, and +2 for all the UPDATE (remove the old PRS
      // and add new PRS entry)
      assertEquals(50, stat.getCversion());

      CollectionAdminResponse response =
          CollectionAdminRequest.addReplicaToShard(PRS_COLL, "shard1")
              .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(PRS_COLL, 10, 11);
      stat = cluster.getZkClient().exists(ZkStateReader.getCollectionPath(PRS_COLL), null, true);
      // For the new replica:
      // +2 for state.json overseer writes, even though there's no longer PRS updates from
      // overseer, current code would still do a "TOUCH" on the PRS entry
      // +1 for ZkController#preRegister, in ZkController#publish, direct write PRS to down
      // +2 for RecoveryStrategy#doRecovery, since this is no longer a new collection, new replica
      // will go through recovery, direct write PRS to RECOVERING
      assertEquals(57, stat.getCversion());

      String addedCore = response.getCollectionCoresStatus().entrySet().iterator().next().getKey();
      Replica addedReplica =
          cluster
              .getSolrClient()
              .getZkStateReader()
              .getCollection(PRS_COLL)
              .getSlice("shard1")
              .getReplicas(replica -> addedCore.equals(replica.getCoreName()))
              .get(0);
      CollectionAdminRequest.deleteReplica(PRS_COLL, "shard1", addedReplica.getName())
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(PRS_COLL, 10, 10);
      stat = cluster.getZkClient().exists(ZkStateReader.getCollectionPath(PRS_COLL), null, true);
      // For replica deletion
      // +1 for ZkController#unregister, which delete the PRS entry from data node
      // overseer, current code would still do a "TOUCH" on the PRS entry
      assertEquals(59, stat.getCversion());

      for (JettySolrRunner j : cluster.getJettySolrRunners()) {
        j.stop();
        j.start(true);
        stat = cluster.getZkClient().exists(ZkStateReader.getCollectionPath(PRS_COLL), null, true);
        // ensure restart does not update the state.json, after addReplica/deleteReplica, 2 more
        // updates hence at version 3 on state.json version
        assertEquals(3, stat.getVersion());
      }
    } finally {
      cluster.shutdown();
    }
  }
}
