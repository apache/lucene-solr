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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test split phase that occurs when a Collection API split call is made.
 */
@Slow
@Ignore("SOLR-4944")
public class ChaosMonkeyShardSplitTest extends ShardSplitTest {

  static final int TIMEOUT = 10000;
  private AtomicInteger killCounter = new AtomicInteger();

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  public void doTest() throws Exception {
    waitForThingsToLevelOut(15);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    final DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
    final List<DocRouter.Range> ranges = router.partitionRange(2, shard1Range);
    final int[] docCounts = new int[ranges.size()];
    int numReplicas = shard1.getReplicas().size();

    Thread indexThread = null;
    OverseerRestarter killer = null;
    Thread killerThread = null;
    final SolrServer solrServer = clients.get(0);

    try {
      del("*:*");
      for (int id = 0; id < 100; id++) {
        indexAndUpdateCount(router, ranges, docCounts, String.valueOf(id), id);
      }
      commit();

      indexThread = new Thread() {
        @Override
        public void run() {
          int max = atLeast(401);
          for (int id = 101; id < max; id++) {
            try {
              indexAndUpdateCount(router, ranges, docCounts, String.valueOf(id), id);
              Thread.sleep(atLeast(25));
            } catch (Exception e) {
              log.error("Exception while adding doc", e);
            }
          }
        }
      };
      indexThread.start();

      // kill the leader
      CloudJettyRunner leaderJetty = shardToLeaderJetty.get("shard1");
      chaosMonkey.killJetty(leaderJetty);

      Thread.sleep(2000);

      waitForThingsToLevelOut(90);

      Thread.sleep(1000);
      checkShardConsistency(false, true);

      CloudJettyRunner deadJetty = leaderJetty;

      // TODO: Check total docs ?
      // long cloudClientDocs = cloudClient.query(new
      // SolrQuery("*:*")).getResults().getNumFound();

      // Wait until new leader is elected
      while (deadJetty == leaderJetty) {
        updateMappingsFromZk(this.jettys, this.clients);
        leaderJetty = shardToLeaderJetty.get("shard1");
      }

      // bring back dead node
      ChaosMonkey.start(deadJetty.jetty); // he is not the leader anymore

      waitTillRecovered();

      // Kill the overseer
      // TODO: Actually kill the Overseer instance
      killer = new OverseerRestarter(zkServer.getZkAddress());
      killerThread = new Thread(killer);
      killerThread.start();
      killCounter.incrementAndGet();

      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, null, null);

      log.info("Layout after split: \n");
      printLayout();

      // distributed commit on all shards
    } finally {
      if (indexThread != null)
        indexThread.join();
      if (solrServer != null)
        solrServer.commit();
      if (killer != null) {
        killer.run = false;
        if (killerThread != null) {
          killerThread.join();
        }
      }
    }

    checkDocCountsAndShardStates(docCounts, numReplicas);

    // todo - can't call waitForThingsToLevelOut because it looks for
    // jettys of all shards
    // and the new sub-shards don't have any.
    waitForRecoveriesToFinish(true);
    // waitForThingsToLevelOut(15);
  }

  private class OverseerRestarter implements Runnable {
    SolrZkClient overseerClient = null;
    public volatile boolean run = true;
    private final String zkAddress;

    public OverseerRestarter(String zkAddress) {
      this.zkAddress = zkAddress;
    }

    @Override
    public void run() {
      try {
        overseerClient = electNewOverseer(zkAddress);
        while (run) {
          if (killCounter.get() > 0) {
            try {
              killCounter.decrementAndGet();
              log.info("Killing overseer after 800ms");
              Thread.sleep(800);
              overseerClient.close();
              overseerClient = electNewOverseer(zkAddress);
            } catch (Throwable e) {
              // e.printStackTrace();
            }
          }
          try {
            Thread.sleep(100);
          } catch (Throwable e) {
            // e.printStackTrace();
          }
        }
      } catch (Throwable t) {
        // ignore
      } finally {
        if (overseerClient != null) {
          try {
            overseerClient.close();
          } catch (Throwable t) {
            // ignore
          }
        }
      }
    }
  }

  private void waitTillRecovered() throws Exception {
    for (int i = 0; i < 30; i++) {
      Thread.sleep(3000);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection1 = clusterState.getCollection("collection1");
      Slice slice = collection1.getSlice("shard1");
      Collection<Replica> replicas = slice.getReplicas();
      boolean allActive = true;
      for (Replica replica : replicas) {
        if (!clusterState.liveNodesContain(replica.getNodeName())
            || !replica.get(ZkStateReader.STATE_PROP).equals(
            ZkStateReader.ACTIVE)) {
          allActive = false;
          break;
        }
      }
      if (allActive) {
        return;
      }
    }
    printLayout();
    fail("timeout waiting to see recovered node");
  }

  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

  /**
   * Elects a new overseer
   *
   * @return SolrZkClient
   */
  private SolrZkClient electNewOverseer(String address) throws KeeperException,
      InterruptedException, IOException {
    SolrZkClient zkClient = new SolrZkClient(address, TIMEOUT);
    ZkStateReader reader = new ZkStateReader(zkClient);
    LeaderElector overseerElector = new LeaderElector(zkClient);

    // TODO: close Overseer
    Overseer overseer = new Overseer(
        new HttpShardHandlerFactory().getShardHandler(), "/admin/cores", reader);
    overseer.close();
    ElectionContext ec = new OverseerElectionContext(zkClient, overseer,
        address.replaceAll("/", "_"));
    overseerElector.setup(ec);
    overseerElector.joinElection(ec, false);
    return zkClient;
  }

}
