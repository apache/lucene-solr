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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.cloud.api.collections.ShardSplitTest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test split phase that occurs when a Collection API split call is made.
 */
@Slow
@Ignore("SOLR-4944")
public class ChaosMonkeyShardSplitTest extends ShardSplitTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int TIMEOUT = 10000;
  private AtomicInteger killCounter = new AtomicInteger();
  
  @BeforeClass
  public static void beforeSuperClass() {
    System.clearProperty("solr.httpclient.retries");
    System.clearProperty("solr.retries.on.forward");
    System.clearProperty("solr.retries.to.followers"); 
  }

  @Test
  public void test() throws Exception {
    waitForThingsToLevelOut(15);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    final DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getSlice(SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
    final List<DocRouter.Range> ranges = router.partitionRange(2, shard1Range);
    final int[] docCounts = new int[ranges.size()];
    int numReplicas = shard1.getReplicas().size();
    final Set<String> documentIds = ConcurrentHashMap.newKeySet(1024);

    Thread indexThread = null;
    OverseerRestarter killer = null;
    Thread killerThread = null;
    final SolrClient solrClient = clients.get(0);

    try {
      del("*:*");
      for (int id = 0; id < 100; id++) {
        indexAndUpdateCount(router, ranges, docCounts, String.valueOf(id), id, documentIds);
      }
      commit();

      indexThread = new Thread() {
        @Override
        public void run() {
          int max = atLeast(401);
          for (int id = 101; id < max; id++) {
            try {
              indexAndUpdateCount(router, ranges, docCounts, String.valueOf(id), id, documentIds);
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
      leaderJetty.jetty.stop();

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
      deadJetty.jetty.start(); // he is not the leader anymore

      waitTillRecovered();

      // Kill the overseer
      // TODO: Actually kill the Overseer instance
      killer = new OverseerRestarter(zkServer.getZkAddress());
      killerThread = new Thread(killer);
      killerThread.start();
      killCounter.incrementAndGet();

      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, null, null, false);

      log.info("Layout after split: \n");
      printLayout();

      // distributed commit on all shards
    } finally {
      if (indexThread != null)
        indexThread.join();
      if (solrClient != null)
        solrClient.commit();
      if (killer != null) {
        killer.run = false;
        if (killerThread != null) {
          killerThread.join();
        }
      }
    }

    checkDocCountsAndShardStates(docCounts, numReplicas, documentIds);

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
            } catch (Exception e) {
              // e.printStackTrace();
            }
          }
          try {
            Thread.sleep(100);
          } catch (Exception e) {
            // e.printStackTrace();
          }
        }
      } catch (Exception t) {
        // ignore
      } finally {
        if (overseerClient != null) {
          try {
            overseerClient.close();
          } catch (Exception t) {
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
      zkStateReader.forceUpdateCollection("collection1");
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection1 = clusterState.getCollection("collection1");
      Slice slice = collection1.getSlice("shard1");
      Collection<Replica> replicas = slice.getReplicas();
      boolean allActive = true;
      for (Replica replica : replicas) {
        if (!clusterState.liveNodesContain(replica.getNodeName()) || replica.getState() != Replica.State.ACTIVE) {
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
    UpdateShardHandler updateShardHandler = new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);
    try (HttpShardHandlerFactory hshf = new HttpShardHandlerFactory()) {
      Overseer overseer = new Overseer((HttpShardHandler) hshf.getShardHandler(), updateShardHandler, "/admin/cores",
          reader, null, new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build());
      overseer.close();
      ElectionContext ec = new OverseerElectionContext(zkClient, overseer,
          address.replaceAll("/", "_"));
      overseerElector.setup(ec);
      overseerElector.joinElection(ec, false);
    }
    reader.close();
    return zkClient;
  }

}
