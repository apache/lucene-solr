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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.client.HttpClient;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

@Slow
public class TestAppendReplica extends SolrCloudTestCase {
  
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private String collectionName = null;
  private final static int REPLICATION_TIMEOUT_SECS = 10;
  
  private String suggestedCollectionName() {
    return (getTestClass().getSimpleName().replace("Test", "") + "_" + getTestName().split(" ")[0]).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase(Locale.ROOT);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    TestInjection.waitForReplicasInSync = null; // We'll be explicit about this in this test
    configureCluster(2) // 2 + random().nextInt(3) 
        .addConfig("conf", configset("cloud-minimal-inplace-updates"))
        .configure();
    Boolean useLegacyCloud = rarely();
    LOG.info("Using legacyCloud?: {}", useLegacyCloud);
    CollectionAdminRequest.ClusterProp clusterPropRequest = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, String.valueOf(useLegacyCloud));
    CollectionAdminResponse response = clusterPropRequest.process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
  }
  
  @AfterClass
  public static void tearDownCluster() {
    TestInjection.reset();
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionName = suggestedCollectionName();
    expectThrows(SolrException.class, () -> getCollectionState(collectionName));
  }

  @Override
  public void tearDown() throws Exception {
    for (JettySolrRunner jetty:cluster.getJettySolrRunners()) {
      if (!jetty.isRunning()) {
        LOG.warn("Jetty {} not running, probably some bad test. Starting it", jetty.getLocalPort());
        ChaosMonkey.start(jetty);
      }
    }
    if (cluster.getSolrClient().getZkStateReader().getClusterState().getCollectionOrNull(collectionName) != null) {
      LOG.info("tearDown deleting collection");
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
      waitForDeletion(collectionName);
    }
    super.tearDown();
  }
  
  /**
   * Asserts that Update logs exist for replicas of type {@link org.apache.solr.common.cloud.Replica.Type#REALTIME}, but not
   * for replicas of type {@link org.apache.solr.common.cloud.Replica.Type#PASSIVE}
   */
  private void assertUlogPresence(DocCollection collection) {
    for (Slice s:collection.getSlices()) {
      for (Replica r:s.getReplicas()) {
        SolrCore core = null;
        try {
          core = cluster.getReplicaJetty(r).getCoreContainer().getCore(r.getCoreName());
          assertNotNull(core);
          assertTrue("Update log should exist for replicas of type Append", 
              new java.io.File(core.getUlogDir()).exists());
        } finally {
          core.close();
        }
      }
    }
  }
  
  @Repeat(iterations=2) // 2 times to make sure cleanup is complete and we can create the same collection
  public void testCreateDelete() throws Exception {
    try {
      CollectionAdminRequest.createCollection(collectionName, "conf", 2, 0, 4, 0)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
      DocCollection docCollection = getCollectionState(collectionName);
      assertNotNull(docCollection);
      assertEquals("Expecting 2 shards",
          2, docCollection.getSlices().size());
      assertEquals("Expecting 4 relpicas per shard",
          8, docCollection.getReplicas().size());
      assertEquals("Expecting 8 append replicas, 4 per shard",
          8, docCollection.getReplicas(EnumSet.of(Replica.Type.APPEND)).size());
      assertEquals("Expecting no realtime replicas",
          0, docCollection.getReplicas(EnumSet.of(Replica.Type.REALTIME)).size());
      assertEquals("Expecting no passive replicas",
          0, docCollection.getReplicas(EnumSet.of(Replica.Type.PASSIVE)).size());
      for (Slice s:docCollection.getSlices()) {
        assertTrue(s.getLeader().getType() == Replica.Type.APPEND);
        List<String> shardElectionNodes = cluster.getZkClient().getChildren(ZkStateReader.getShardLeadersElectPath(collectionName, s.getName()), null, true);
        assertEquals("Unexpected election nodes for Shard: " + s.getName() + ": " + Arrays.toString(shardElectionNodes.toArray()), 
            4, shardElectionNodes.size());
      }
      assertUlogPresence(docCollection);
    } finally {
      zkClient().printLayoutToStdOut();
    }
  }
  
  @SuppressWarnings("unchecked")
  public void testAddDocs() throws Exception {
    int numAppendReplicas = 1 + random().nextInt(3);
    DocCollection docCollection = createAndWaitForCollection(1, 0, numAppendReplicas, 0);
    assertEquals(1, docCollection.getSlices().size());
    
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    
    Slice s = docCollection.getSlices().iterator().next();
    try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
      assertEquals(1, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    TimeOut t = new TimeOut(REPLICATION_TIMEOUT_SECS, TimeUnit.SECONDS);
    for (Replica r:s.getReplicas(EnumSet.of(Replica.Type.APPEND))) {
      //TODO: assert replication < REPLICATION_TIMEOUT_SECS
      try (HttpSolrClient appendReplicaClient = getHttpSolrClient(r.getCoreUrl())) {
        while (true) {
          try {
            assertEquals("Replica " + r.getName() + " not up to date after 10 seconds",
                1, appendReplicaClient.query(new SolrQuery("*:*")).getResults().getNumFound());
            // Append replicas process all updates
            SolrQuery req = new SolrQuery(
                "qt", "/admin/plugins",
                "stats", "true");
            QueryResponse statsResponse = appendReplicaClient.query(req);
            assertEquals("Append replicas should recive all updates. Replica: " + r + ", response: " + statsResponse, 
                1L, ((Map<String, Object>)((NamedList<Object>)statsResponse.getResponse()).findRecursive("plugins", "UPDATE", "updateHandler", "stats")).get("UPDATE.updateHandler.cumulativeAdds.count"));
            break;
          } catch (AssertionError e) {
            if (t.hasTimedOut()) {
              throw e;
            } else {
              Thread.sleep(100);
            }
          }
        }
      }
    }
    assertUlogPresence(docCollection);
  }
  
  public void testAddRemoveAppendReplica() throws Exception {
    DocCollection docCollection = createAndWaitForCollection(2, 0, 1, 0);
    assertEquals(2, docCollection.getSlices().size());
    
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.APPEND).process(cluster.getSolrClient());
    docCollection = assertNumberOfReplicas(0, 3, 0, true, false);
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard2", Replica.Type.APPEND).process(cluster.getSolrClient());    
    docCollection = assertNumberOfReplicas(0, 4, 0, true, false);
    
    waitForState("Expecting collection to have 2 shards and 2 replica each", collectionName, clusterShape(2, 2));
    
    //Delete passive replica from shard1
    CollectionAdminRequest.deleteReplica(
        collectionName, 
        "shard1", 
        docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.APPEND)).get(0).getName())
    .process(cluster.getSolrClient());
    assertNumberOfReplicas(0, 3, 0, true, true);
  }
  
  public void testRemoveLeader() throws Exception {
    doReplaceLeader(true);
  }
  
  public void testKillLeader() throws Exception {
    doReplaceLeader(false);
  }
  
  public void testRealTimeGet() throws SolrServerException, IOException, KeeperException, InterruptedException {
    // should be redirected to Replica.Type.REALTIME
    int numReplicas = random().nextBoolean()?1:2;
    int numRealtimeReplicas = random().nextBoolean()?0:2;
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, numRealtimeReplicas, numReplicas, 0)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
    waitForState("Unexpected replica count", collectionName, activeReplicaCount(numRealtimeReplicas, numReplicas, 0));
    DocCollection docCollection = assertNumberOfReplicas(numRealtimeReplicas, numReplicas, 0, false, true);
    HttpClient httpClient = cluster.getSolrClient().getHttpClient();
    int id = 0;
    Slice slice = docCollection.getSlice("shard1");
    List<String> ids = new ArrayList<>(slice.getReplicas().size());
    for (Replica rAdd:slice.getReplicas()) {
      try (HttpSolrClient client = getHttpSolrClient(rAdd.getCoreUrl(), httpClient)) {
        client.add(new SolrInputDocument("id", String.valueOf(id), "foo_s", "bar"));
      }
      SolrDocument docCloudClient = cluster.getSolrClient().getById(collectionName, String.valueOf(id));
      assertEquals("bar", docCloudClient.getFieldValue("foo_s"));
      for (Replica rGet:slice.getReplicas()) {
        try (HttpSolrClient client = getHttpSolrClient(rGet.getCoreUrl(), httpClient)) {
          SolrDocument doc = client.getById(String.valueOf(id));
          assertEquals("bar", doc.getFieldValue("foo_s"));
        }
      }
      ids.add(String.valueOf(id));
      id++;
    }
    SolrDocumentList previousAllIdsResult = null;
    for (Replica rAdd:slice.getReplicas()) {
      try (HttpSolrClient client = getHttpSolrClient(rAdd.getCoreUrl(), httpClient)) {
        SolrDocumentList allIdsResult = client.getById(ids);
        if (previousAllIdsResult != null) {
          assertTrue(compareSolrDocumentList(previousAllIdsResult, allIdsResult));
        } else {
          // set the first response here
          previousAllIdsResult = allIdsResult;
          assertEquals("Unexpected number of documents", ids.size(), allIdsResult.getNumFound());
        }
      }
      id++;
    }
  }
  
  /*
   * validate leader election and that replication still happens on a new leader
   */
  private void doReplaceLeader(boolean removeReplica) throws Exception {
    DocCollection docCollection = createAndWaitForCollection(1, 0, 2, 0);
    
    // Add a document and commit
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    Slice s = docCollection.getSlices().iterator().next();
    try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
      assertEquals(1, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.APPEND)), REPLICATION_TIMEOUT_SECS);
    
    // Delete leader replica from shard1
    JettySolrRunner leaderJetty = null;
    if (removeReplica) {
      CollectionAdminRequest.deleteReplica(
          collectionName, 
          "shard1", 
          s.getLeader().getName())
      .process(cluster.getSolrClient());
    } else {
      leaderJetty = cluster.getReplicaJetty(s.getLeader());
      ChaosMonkey.kill(leaderJetty);
      waitForState("Leader replica not removed", collectionName, clusterShape(1, 1));
      // Wait for cluster state to be updated
      waitForState("Replica state not updated in cluster state", 
          collectionName, clusterStateReflectsActiveAndDownReplicas());
    }
    docCollection = assertNumberOfReplicas(0, 1, 0, true, true);
    
    // Wait until a new leader is elected
    TimeOut t = new TimeOut(30, TimeUnit.SECONDS);
    while (!t.hasTimedOut()) {
      docCollection = getCollectionState(collectionName);
      Replica leader = docCollection.getSlice("shard1").getLeader();
      if (leader != null && leader.isActive(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes())) {
        break;
      }
      Thread.sleep(500);
    }
    assertFalse("Timeout waiting for a new leader to be elected", t.hasTimedOut());
    
    // There is a new leader, I should be able to add and commit
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "zoo"));
    cluster.getSolrClient().commit(collectionName);
    
    // Queries should still work
    waitForNumDocsInAllReplicas(2, docCollection.getReplicas(EnumSet.of(Replica.Type.APPEND)), REPLICATION_TIMEOUT_SECS);
    // Start back the node
    if (removeReplica) {
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.APPEND).process(cluster.getSolrClient());
    } else {
      ChaosMonkey.start(leaderJetty);
    }
    waitForState("Expected collection to be 1x2", collectionName, clusterShape(1, 2));
    // added replica should replicate from the leader
    waitForNumDocsInAllReplicas(2, docCollection.getReplicas(EnumSet.of(Replica.Type.APPEND)), REPLICATION_TIMEOUT_SECS);
  }
  
  public void testKillAppendReplica() throws Exception {
    DocCollection docCollection = createAndWaitForCollection(1, 0, 2, 0);
    
    waitForNumDocsInAllActiveReplicas(0);
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(1);
    
    JettySolrRunner passiveReplicaJetty = cluster.getReplicaJetty(docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.APPEND)).get(0));
    ChaosMonkey.kill(passiveReplicaJetty);
    waitForState("Replica not removed", collectionName, activeReplicaCount(0, 1, 0));
    // Also wait for the replica to be placed in state="down"
    waitForState("Didn't update state", collectionName, clusterStateReflectsActiveAndDownReplicas());
    
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(2);
    
    ChaosMonkey.start(passiveReplicaJetty);
    waitForState("Replica not added", collectionName, activeReplicaCount(0, 2, 0));
    waitForNumDocsInAllActiveReplicas(2);
  }
  
  public void testSearchWhileReplicationHappens() {
      
  }
  
  public void testReplication() {
    // Validate incremental replication
  }
  
  public void testOnlyLeaderIndexes() throws Exception {
    createAndWaitForCollection(1, 0, 2, 0);
    
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .add(sdoc("id", "1"))
        .add(sdoc("id", "2"))
        .add(sdoc("id", "3"))
        .add(sdoc("id", "4"))
        .process(cloudClient, collectionName);

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
        .process(cloudClient, collectionName);

    // The DBQ is not processed at replicas, so we still can get doc2 and other docs by RTG
    checkRTG(2,4, getSolrRunner(false));

    new UpdateRequest()
        .commit(cloudClient, collectionName);

    waitForNumDocsInAllActiveReplicas(2);

    // Update log roll over
    for (SolrCore solrCore : getSolrCore(false)) {
      UpdateLog updateLog = solrCore.getUpdateHandler().getUpdateLog();
      assertFalse(updateLog.hasUncommittedChanges());
    }

    // UpdateLog copy over old updates
    for (int i = 15; i <= 150; i++) {
      cloudClient.add(collectionName, sdoc("id",String.valueOf(i)));
      if (random().nextInt(100) < 15 & i != 150) {
        cloudClient.commit(collectionName);
      }
    }
    checkRTG(120,150, cluster.getJettySolrRunners());
    waitForReplicasCatchUp(20);
  }
  
  public void testRecovery() throws Exception {
    boolean useKill = random().nextBoolean();
    createAndWaitForCollection(1, 0, 2, 0);
    
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .add(sdoc("id", "3"))
        .add(sdoc("id", "4"))
        .commit(cloudClient, collectionName);
    // Replica recovery
    new UpdateRequest()
        .add(sdoc("id", "5"))
        .process(cloudClient, collectionName);
    JettySolrRunner solrRunner = getSolrRunner(false).get(0);
    if (useKill) { 
      ChaosMonkey.kill(solrRunner);
    } else {
      ChaosMonkey.stop(solrRunner);
    }
    new UpdateRequest()
        .add(sdoc("id", "6"))
        .process(cloudClient, collectionName);
    ChaosMonkey.start(solrRunner);
    waitForState("Replica didn't recover", collectionName, activeReplicaCount(0,2,0));
    // We skip peerSync, so replica will always trigger commit on leader
    waitForNumDocsInAllActiveReplicas(4);
    
    // If I add the doc immediately, the leader fails to communicate with the follower with broken pipe. Related to SOLR-9555 I believe
    //nocommit
    Thread.sleep(10000);
    
    // More Replica recovery testing
    new UpdateRequest()
        .add(sdoc("id", "7"))
        .process(cloudClient, collectionName);
    checkRTG(3,7, cluster.getJettySolrRunners());
    DirectUpdateHandler2.commitOnClose = false;
    ChaosMonkey.stop(solrRunner);
    DirectUpdateHandler2.commitOnClose = true;
    ChaosMonkey.start(solrRunner);
    waitForState("Replica didn't recover", collectionName, activeReplicaCount(0,2,0));
    checkRTG(3,7, cluster.getJettySolrRunners());
    waitForNumDocsInAllActiveReplicas(5, 0);

    // Test replica recovery apply buffer updates
    Semaphore waitingForBufferUpdates = new Semaphore(0);
    Semaphore waitingForReplay = new Semaphore(0);
    RecoveryStrategy.testing_beforeReplayBufferingUpdates = () -> {
      try {
        waitingForReplay.release();
        waitingForBufferUpdates.acquire();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Test interrupted: " + e.getMessage());
      }
    };
    if (useKill) { 
      ChaosMonkey.kill(solrRunner);
    } else {
      ChaosMonkey.stop(solrRunner);
    }
    ChaosMonkey.start(solrRunner);
    waitingForReplay.acquire();
    new UpdateRequest()
        .add(sdoc("id", "8"))
        .add(sdoc("id", "9"))
        .process(cloudClient, collectionName);
    waitingForBufferUpdates.release();
    RecoveryStrategy.testing_beforeReplayBufferingUpdates = null;
    waitForState("Replica didn't recover", collectionName, activeReplicaCount(0,2,0));
    checkRTG(3,9, cluster.getJettySolrRunners());
    waitForNumDocsInAllActiveReplicas(5, 0);
    for (SolrCore solrCore : getSolrCore(false)) {
      RefCounted<IndexWriter> iwRef = solrCore.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
      assertFalse("IndexWriter at replicas must not see updates ", iwRef.get().hasUncommittedChanges());
      iwRef.decref();
    }
  }
  
  public void testDeleteById() throws Exception{
    createAndWaitForCollection(1,0,2,0);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), collectionName);
    new UpdateRequest()
        .add(sdoc("id", "1"))
        .commit(cloudClient, collectionName);
    waitForNumDocsInAllActiveReplicas(1);
    new UpdateRequest()
        .deleteById("1")
        .process(cloudClient, collectionName);
    boolean successs = false;
    try {
      checkRTG(1, 1, cluster.getJettySolrRunners());
      successs = true;
    } catch (AssertionError e) {
      //expected
    }
    assertFalse("Doc1 is deleted but it's still exist", successs);
  }
  
  public void testBasicLeaderElection() throws Exception {
    createAndWaitForCollection(1,0,2,0);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), collectionName);
    new UpdateRequest()
        .add(sdoc("id", "1"))
        .add(sdoc("id", "2"))
        .process(cloudClient, collectionName);
    JettySolrRunner oldLeaderJetty = getSolrRunner(true).get(0);
    ChaosMonkey.kill(oldLeaderJetty);
    waitForState("Replica not removed", collectionName, activeReplicaCount(0, 1, 0));
    new UpdateRequest()
        .add(sdoc("id", "3"))
        .add(sdoc("id", "4"))
        .process(cloudClient, collectionName);
    ChaosMonkey.start(oldLeaderJetty);
    waitForState("Replica not added", collectionName, activeReplicaCount(0, 2, 0));
    checkRTG(1,4, cluster.getJettySolrRunners());
    new UpdateRequest()
        .commit(cloudClient, collectionName);
    waitForNumDocsInAllActiveReplicas(4, 0);
  }
  
  public void testOutOfOrderDBQWithInPlaceUpdates() throws Exception {
    createAndWaitForCollection(1,0,2,0);
    assertFalse(getSolrCore(true).get(0).getLatestSchema().getField("inplace_updatable_int").indexed());
    assertFalse(getSolrCore(true).get(0).getLatestSchema().getField("inplace_updatable_int").stored());
    assertTrue(getSolrCore(true).get(0).getLatestSchema().getField("inplace_updatable_int").hasDocValues());
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 1, "title_s", "title0_new", "inplace_updatable_int", 5, "_version_", 1L)); // full update
    updates.add(simulatedDBQ("inplace_updatable_int:5", 3L));
    updates.add(simulatedUpdateRequest(1L, "id", 1, "inplace_updatable_int", 6, "_version_", 2L));
    for (JettySolrRunner solrRunner: getSolrRunner(false)) {
      try (SolrClient client = solrRunner.newClient()) {
        for (UpdateRequest up : updates) {
          up.process(client, collectionName);
        }
      }
    }
    JettySolrRunner oldLeaderJetty = getSolrRunner(true).get(0);
    ChaosMonkey.kill(oldLeaderJetty);
    waitForState("Replica not removed", collectionName, activeReplicaCount(0, 1, 0));
    ChaosMonkey.start(oldLeaderJetty);
    waitForState("Replica not added", collectionName, activeReplicaCount(0, 2, 0));
    checkRTG(1,1, cluster.getJettySolrRunners());
    SolrDocument doc = cluster.getSolrClient().getById(collectionName,"1");
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
    DocCollection collection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName);
    Slice slice = collection.getSlice("shard1");
    return slice.getLeader().getCoreUrl();
  }

  private DocCollection createAndWaitForCollection(int numShards, int numRealtimeReplicas, int numAppendReplicas, int numPassiveReplicas) throws SolrServerException, IOException, KeeperException, InterruptedException {
    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, numRealtimeReplicas, numAppendReplicas, numPassiveReplicas)
    .setMaxShardsPerNode(100)
    .process(cluster.getSolrClient());
    int numReplicasPerShard = numRealtimeReplicas + numAppendReplicas + numPassiveReplicas;
    cluster.getSolrClient().getZkStateReader().registerCore(collectionName); //TODO: Is this needed? 
    waitForState("Expected collection to be created with " + numShards + " shards and  " + numReplicasPerShard + " replicas", 
        collectionName, clusterShape(numShards, numReplicasPerShard));
    return assertNumberOfReplicas(numRealtimeReplicas*numShards, numAppendReplicas*numShards, numPassiveReplicas*numShards, false, true);
  }
  
  private void waitForNumDocsInAllActiveReplicas(int numDocs) throws IOException, SolrServerException, InterruptedException {
    waitForNumDocsInAllActiveReplicas(numDocs, REPLICATION_TIMEOUT_SECS);
  }
  
  private void waitForNumDocsInAllActiveReplicas(int numDocs, int timeout) throws IOException, SolrServerException, InterruptedException {
    DocCollection docCollection = getCollectionState(collectionName);
    waitForNumDocsInAllReplicas(numDocs, docCollection.getReplicas().stream().filter(r -> r.getState() == Replica.State.ACTIVE).collect(Collectors.toList()), timeout);
  }
    
  private void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas, int timeout) throws IOException, SolrServerException, InterruptedException {
    waitForNumDocsInAllReplicas(numDocs, replicas, "*:*", timeout);
  }
  
  private void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas, String query, int timeout) throws IOException, SolrServerException, InterruptedException {
    TimeOut t = new TimeOut(timeout, TimeUnit.SECONDS);
    for (Replica r:replicas) {
      if (!r.isActive(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes())) {
        continue;
      }
      try (HttpSolrClient replicaClient = getHttpSolrClient(r.getCoreUrl())) {
        while (true) {
          try {
            assertEquals("Replica " + r.getName() + " not up to date after " + REPLICATION_TIMEOUT_SECS + " seconds",
                numDocs, replicaClient.query(new SolrQuery(query)).getResults().getNumFound());
            break;
          } catch (AssertionError e) {
            if (t.hasTimedOut()) {
              throw e;
            } else {
              Thread.sleep(100);
            }
          }
        }
      }
    }
  }
  
  private void waitForDeletion(String collection) throws InterruptedException, KeeperException {
    TimeOut t = new TimeOut(10, TimeUnit.SECONDS);
    while (cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection(collection)) {
      try {
        Thread.sleep(100);
        if (t.hasTimedOut()) {
          fail("Timed out waiting for collection " + collection + " to be deleted.");
        }
        cluster.getSolrClient().getZkStateReader().forceUpdateCollection(collection);
      } catch(SolrException e) {
        return;
      }
      
    }
  }
  
  private DocCollection assertNumberOfReplicas(int numWriter, int numActive, int numPassive, boolean updateCollection, boolean activeOnly) throws KeeperException, InterruptedException {
    if (updateCollection) {
      cluster.getSolrClient().getZkStateReader().forceUpdateCollection(collectionName);
    }
    DocCollection docCollection = getCollectionState(collectionName);
    assertNotNull(docCollection);
    assertEquals("Unexpected number of writer replicas: " + docCollection, numWriter, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.REALTIME)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of passive replicas: " + docCollection, numPassive, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.PASSIVE)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of active replicas: " + docCollection, numActive, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.APPEND)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    return docCollection;
  }
  
  /*
   * passes only if all replicas are active or down, and the "liveNodes" reflect the same status
   */
  private CollectionStatePredicate clusterStateReflectsActiveAndDownReplicas() {
    return (liveNodes, collectionState) -> {
      for (Replica r:collectionState.getReplicas()) {
        if (r.getState() != Replica.State.DOWN && r.getState() != Replica.State.ACTIVE) {
          return false;
        }
        if (r.getState() == Replica.State.DOWN && liveNodes.contains(r.getNodeName())) {
          return false;
        }
        if (r.getState() == Replica.State.ACTIVE && !liveNodes.contains(r.getNodeName())) {
          return false;
        }
      }
      return true;
    };
  }
  
  
  private CollectionStatePredicate activeReplicaCount(int numWriter, int numActive, int numPassive) {
    return (liveNodes, collectionState) -> {
      int writersFound = 0, activesFound = 0, passivesFound = 0;
      if (collectionState == null)
        return false;
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            switch (replica.getType()) {
              case APPEND:
                activesFound++;
                break;
              case PASSIVE:
                passivesFound++;
                break;
              case REALTIME:
                writersFound++;
                break;
              default:
                throw new AssertionError("Unexpected replica type");
            }
        }
      }
      return numWriter == writersFound && numActive == activesFound && numPassive == passivesFound;
    };
  }
  
  private List<SolrCore> getSolrCore(boolean isLeader) {
    List<SolrCore> rs = new ArrayList<>();

    CloudSolrClient cloudClient = cluster.getSolrClient();
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);

    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      if (solrRunner.getCoreContainer() == null) continue;
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        CloudDescriptor cloudDescriptor = solrCore.getCoreDescriptor().getCloudDescriptor();
        Slice slice = docCollection.getSlice(cloudDescriptor.getShardId());
        Replica replica = docCollection.getReplica(cloudDescriptor.getCoreNodeName());
        if (slice.getLeader().equals(replica) && isLeader) {
          rs.add(solrCore);
        } else if (!slice.getLeader().equals(replica) && !isLeader) {
          rs.add(solrCore);
        }
      }
    }
    return rs;
  }
  
  private void checkRTG(int from, int to, List<JettySolrRunner> solrRunners) throws Exception{
    for (JettySolrRunner solrRunner: solrRunners) {
      try (SolrClient client = solrRunner.newClient()) {
        for (int i = from; i <= to; i++) {
          SolrQuery query = new SolrQuery();
          query.set("distrib", false);
          query.setRequestHandler("/get");
          query.set("id",i);
          QueryResponse res = client.query(collectionName, query);
          assertNotNull("Can not find doc "+ i + " in " + solrRunner.getBaseUrl(),res.getResponse().get("doc"));
        }
      }
    }
  }
  
  private List<JettySolrRunner> getSolrRunner(boolean isLeader) {
    List<JettySolrRunner> rs = new ArrayList<>();
    CloudSolrClient cloudClient = cluster.getSolrClient();
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
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
}
