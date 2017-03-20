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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

public class OnlyLeaderIndexesTest extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(3)
        .addConfig("config", TEST_PATH().resolve("configsets")
        .resolve("cloud-minimal-inplace-updates").resolve("conf"))
        .configure();

    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 1, 3)
        .setRealtimeReplicas(1)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
  }

  @Test
  public void test() throws Exception {
    basicTest();
    recoveryTest();
    dbiTest();
    basicLeaderElectionTest();
    outOfOrderDBQWithInPlaceUpdatesTest();
  }

  public void basicTest() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .add(sdoc("id", "1"))
        .add(sdoc("id", "2"))
        .add(sdoc("id", "3"))
        .add(sdoc("id", "4"))
        .process(cloudClient, COLLECTION);

    {
      UpdateHandler updateHandler = getSolrCore(true).get(0).getUpdateHandler();
      RefCounted<IndexWriter> iwRef = updateHandler.getSolrCoreState().getIndexWriter(null);
      assertTrue("IndexWriter at leader must see updates ", iwRef.get().hasUncommittedChanges());
      iwRef.decref();
    }

    for (SolrCore solrCore : getSolrCore(false)) {
      RefCounted<IndexWriter> iwRef = solrCore.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
      assertFalse("IndexWriter at replicas must not see updates ", iwRef.get().hasUncommittedChanges());
      iwRef.decref();
    }

    checkRTG(1, 4, cluster.getJettySolrRunners());

    new UpdateRequest()
        .deleteById("1")
        .deleteByQuery("id:2")
        .process(cloudClient, COLLECTION);

    // The DBQ is not processed at replicas, so we still can get doc2 and other docs by RTG
    checkRTG(2,4, getSolrRunner(false));

    new UpdateRequest()
        .commit(cloudClient, COLLECTION);

    checkShardConsistency(2, 1);

    // Update log roll over
    for (SolrCore solrCore : getSolrCore(false)) {
      UpdateLog updateLog = solrCore.getUpdateHandler().getUpdateLog();
      assertFalse(updateLog.hasUncommittedChanges());
    }

    // UpdateLog copy over old updates
    for (int i = 15; i <= 150; i++) {
      cloudClient.add(COLLECTION, sdoc("id",String.valueOf(i)));
      if (random().nextInt(100) < 15 & i != 150) {
        cloudClient.commit(COLLECTION);
      }
    }
    checkRTG(120,150, cluster.getJettySolrRunners());
    waitForReplicasCatchUp(20);
  }

  public void recoveryTest() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTION);
    new UpdateRequest()
        .add(sdoc("id", "3"))
        .add(sdoc("id", "4"))
        .commit(cloudClient, COLLECTION);
    // Replica recovery
    new UpdateRequest()
        .add(sdoc("id", "5"))
        .process(cloudClient, COLLECTION);
    JettySolrRunner solrRunner = getSolrRunner(false).get(0);
    ChaosMonkey.stop(solrRunner);
    new UpdateRequest()
        .add(sdoc("id", "6"))
        .process(cloudClient, COLLECTION);
    ChaosMonkey.start(solrRunner);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    // We skip peerSync, so replica will always trigger commit on leader
    checkShardConsistency(4, 20);

    // LTR can be kicked off, so waiting for replicas recovery
    new UpdateRequest()
        .add(sdoc("id", "7"))
        .commit(cloudClient, COLLECTION);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    checkShardConsistency(5, 20);

    // More Replica recovery testing
    new UpdateRequest()
        .add(sdoc("id", "8"))
        .process(cloudClient, COLLECTION);
    checkRTG(3,8, cluster.getJettySolrRunners());
    DirectUpdateHandler2.commitOnClose = false;
    ChaosMonkey.stop(solrRunner);
    DirectUpdateHandler2.commitOnClose = true;
    ChaosMonkey.start(solrRunner);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    checkRTG(3,8, cluster.getJettySolrRunners());
    checkShardConsistency(6, 20);

    // Test replica recovery apply buffer updates
    Semaphore waitingForBufferUpdates = new Semaphore(0);
    Semaphore waitingForReplay = new Semaphore(0);
    RecoveryStrategy.testing_beforeReplayBufferingUpdates = () -> {
      try {
        waitingForReplay.release();
        waitingForBufferUpdates.acquire();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    };
    ChaosMonkey.stop(solrRunner);
    ChaosMonkey.start(solrRunner);
    waitingForReplay.acquire();
    new UpdateRequest()
        .add(sdoc("id", "9"))
        .add(sdoc("id", "10"))
        .process(cloudClient, COLLECTION);
    waitingForBufferUpdates.release();
    RecoveryStrategy.testing_beforeReplayBufferingUpdates = null;
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    checkRTG(3,10, cluster.getJettySolrRunners());
    checkShardConsistency(6, 20);
    for (SolrCore solrCore : getSolrCore(false)) {
      RefCounted<IndexWriter> iwRef = solrCore.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
      assertFalse("IndexWriter at replicas must not see updates ", iwRef.get().hasUncommittedChanges());
      iwRef.decref();
    }
  }

  public void dbiTest() throws Exception{
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTION);
    new UpdateRequest()
        .add(sdoc("id", "1"))
        .commit(cloudClient, COLLECTION);
    checkShardConsistency(1, 1);
    new UpdateRequest()
        .deleteById("1")
        .process(cloudClient, COLLECTION);
    try {
      checkRTG(1, 1, cluster.getJettySolrRunners());
    } catch (AssertionError e) {
      return;
    }
    fail("Doc1 is deleted but it's still exist");
  }

  public void basicLeaderElectionTest() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTION);
    new UpdateRequest()
        .add(sdoc("id", "1"))
        .add(sdoc("id", "2"))
        .process(cloudClient, COLLECTION);
    String oldLeader = getLeader();
    JettySolrRunner oldLeaderJetty = getSolrRunner(true).get(0);
    ChaosMonkey.kill(oldLeaderJetty);
    for (int i = 0; i < 60; i++) { // wait till leader is changed
      if (!oldLeader.equals(getLeader())) {
        break;
      }
      Thread.sleep(100);
    }
    new UpdateRequest()
        .add(sdoc("id", "3"))
        .add(sdoc("id", "4"))
        .process(cloudClient, COLLECTION);
    ChaosMonkey.start(oldLeaderJetty);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 60);
    checkRTG(1,4, cluster.getJettySolrRunners());
    new UpdateRequest()
        .commit(cloudClient, COLLECTION);
    checkShardConsistency(4,1);
  }

  private String getLeader() throws InterruptedException {
    ZkNodeProps props = cluster.getSolrClient().getZkStateReader().getLeaderRetry("collection1", "shard1", 30000);
    return props.getStr(ZkStateReader.NODE_NAME_PROP);
  }

  public void outOfOrderDBQWithInPlaceUpdatesTest() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTION);
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 1, "title_s", "title0_new", "inplace_updatable_int", 5, "_version_", Long.MAX_VALUE-100)); // full update
    updates.add(simulatedDBQ("inplace_updatable_int:5", Long.MAX_VALUE-98));
    updates.add(simulatedUpdateRequest(Long.MAX_VALUE-100, "id", 1, "inplace_updatable_int", 6, "_version_", Long.MAX_VALUE-99));
    for (JettySolrRunner solrRunner: getSolrRunner(false)) {
      try (SolrClient client = solrRunner.newClient()) {
        for (UpdateRequest up : updates) {
          up.process(client, COLLECTION);
        }
      }
    }
    JettySolrRunner oldLeaderJetty = getSolrRunner(true).get(0);
    ChaosMonkey.kill(oldLeaderJetty);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    ChaosMonkey.start(oldLeaderJetty);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
    new UpdateRequest()
        .add(sdoc("id", "2"))
        .commit(cluster.getSolrClient(), COLLECTION);
    checkShardConsistency(2,20);
    SolrDocument doc = cluster.getSolrClient().getById(COLLECTION,"1");
    assertNotNull(doc.get("title_s"));
  }

  private UpdateRequest simulatedUpdateRequest(Long prevVersion, Object... fields) throws SolrServerException, IOException {
    SolrInputDocument doc = sdoc(fields);

    // get baseUrl of the leader
    String baseUrl = getBaseUrl();

    UpdateRequest ur = new UpdateRequest();
    ur.add(doc);
    ur.setParam("update.distrib", "FROMLEADER");
    if (prevVersion != null) {
      ur.setParam("distrib.inplace.prevversion", String.valueOf(prevVersion));
      ur.setParam("distrib.inplace.update", "true");
    }
    ur.setParam("distrib.from", baseUrl);
    return ur;
  }

  private UpdateRequest simulatedDBQ(String query, long version) throws SolrServerException, IOException {
    String baseUrl = getBaseUrl();

    UpdateRequest ur = new UpdateRequest();
    ur.deleteByQuery(query);
    ur.setParam("_version_", ""+version);
    ur.setParam("update.distrib", "FROMLEADER");
    ur.setParam("distrib.from", baseUrl);
    return ur;
  }

  private String getBaseUrl() {
    DocCollection collection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION);
    Slice slice = collection.getSlice("shard1");
    return slice.getLeader().getCoreUrl();
  }

  private void checkRTG(int from, int to, List<JettySolrRunner> solrRunners) throws Exception{

    for (JettySolrRunner solrRunner: solrRunners) {
      try (SolrClient client = solrRunner.newClient()) {
        for (int i = from; i <= to; i++) {
          SolrQuery query = new SolrQuery("*:*");
          query.set("distrib", false);
          query.setRequestHandler("/get");
          query.set("id",i);
          QueryResponse res = client.query(COLLECTION, query);
          assertNotNull("Can not find doc "+ i + " in " + solrRunner.getBaseUrl(),res.getResponse().get("doc"));
        }
      }
    }

  }

  private void checkShardConsistency(int expected, int numTry) throws Exception{

    for (int i = 0; i < numTry; i++) {
      boolean inSync = true;
      for (JettySolrRunner solrRunner: cluster.getJettySolrRunners()) {
        try (SolrClient client = solrRunner.newClient()) {
          SolrQuery query = new SolrQuery("*:*");
          query.set("distrib", false);
          long results = client.query(COLLECTION, query).getResults().getNumFound();
          if (expected != results) {
            inSync = false;
            Thread.sleep(500);
            break;
          }
        }
      }
      if (inSync) return;
    }

    fail("Some replicas are not in sync with leader");
  }

  private void waitForReplicasCatchUp(int numTry) throws IOException, InterruptedException {
    String leaderTimeCommit = getSolrCore(true).get(0).getDeletionPolicy().getLatestCommit().getUserData().get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
    if (leaderTimeCommit == null) return;
    for (int i = 0; i < numTry; i++) {
      boolean inSync = true;
      for (SolrCore solrCore : getSolrCore(false)) {
        String replicateTimeCommit = solrCore.getDeletionPolicy().getLatestCommit().getUserData().get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
        if (!leaderTimeCommit.equals(replicateTimeCommit)) {
          inSync = false;
          Thread.sleep(500);
          break;
        }
      }
      if (inSync) return;
    }

    fail("Some replicas are not in sync with leader");

  }

  private List<SolrCore> getSolrCore(boolean isLeader) {
    List<SolrCore> rs = new ArrayList<>();

    CloudSolrClient cloudClient = cluster.getSolrClient();
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(COLLECTION);

    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      if (solrRunner.getCoreContainer() == null) continue;
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        CloudDescriptor cloudDescriptor = solrCore.getCoreDescriptor().getCloudDescriptor();
        Slice slice = docCollection.getSlice(cloudDescriptor.getShardId());
        Replica replica = docCollection.getReplica(cloudDescriptor.getCoreNodeName());
        if (slice.getLeader() == replica && isLeader) {
          rs.add(solrCore);
        } else if (slice.getLeader() != replica && !isLeader) {
          rs.add(solrCore);
        }
      }
    }
    return rs;
  }

  private List<JettySolrRunner> getSolrRunner(boolean isLeader) {
    List<JettySolrRunner> rs = new ArrayList<>();

    CloudSolrClient cloudClient = cluster.getSolrClient();
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(COLLECTION);

    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      if (solrRunner.getCoreContainer() == null) continue;
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        CloudDescriptor cloudDescriptor = solrCore.getCoreDescriptor().getCloudDescriptor();
        Slice slice = docCollection.getSlice(cloudDescriptor.getShardId());
        Replica replica = docCollection.getReplica(cloudDescriptor.getCoreNodeName());
        if (slice.getLeader() == replica && isLeader) {
          rs.add(solrRunner);
        } else if (slice.getLeader() != replica && !isLeader) {
          rs.add(solrRunner);
        }
      }
    }
    return rs;
  }

}