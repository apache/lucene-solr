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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.codahale.metrics.Meter;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
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
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class TestTlogReplica extends SolrCloudTestCase {
  
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
   * Asserts that Update logs exist for replicas of type {@link org.apache.solr.common.cloud.Replica.Type#NRT}, but not
   * for replicas of type {@link org.apache.solr.common.cloud.Replica.Type#PULL}
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
      switch (random().nextInt(3)) {
        case 0:
          CollectionAdminRequest.createCollection(collectionName, "conf", 2, 0, 4, 0)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());
          break;
        case 1:
          // Sometimes don't use SolrJ
          String url = String.format(Locale.ROOT, "%s/admin/collections?action=CREATE&name=%s&collection.configName=%s&numShards=%s&tlogReplicas=%s&maxShardsPerNode=%s",
              cluster.getRandomJetty(random()).getBaseUrl(),
              collectionName, "conf",
              2,    // numShards
              4,    // tlogReplicas
              100); // maxShardsPerNode
          HttpGet createCollectionGet = new HttpGet(url);
          HttpResponse httpResponse = cluster.getSolrClient().getHttpClient().execute(createCollectionGet);
          assertEquals(200, httpResponse.getStatusLine().getStatusCode());
          break;
        case 2:
          // Sometimes use V2 API
          url = cluster.getRandomJetty(random()).getBaseUrl().toString() + "/____v2/c";
          String requestBody = String.format(Locale.ROOT, "{create:{name:%s, config:%s, numShards:%s, tlogReplicas:%s, maxShardsPerNode:%s}}",
              collectionName, "conf",
              2,    // numShards
              4,    // tlogReplicas
              100); // maxShardsPerNode
          HttpPost createCollectionPost = new HttpPost(url);
          createCollectionPost.setHeader("Content-type", "application/json");
          createCollectionPost.setEntity(new StringEntity(requestBody));
          httpResponse = cluster.getSolrClient().getHttpClient().execute(createCollectionPost);
          assertEquals(200, httpResponse.getStatusLine().getStatusCode());
          break;
      }
      
      boolean reloaded = false;
      while (true) {
        DocCollection docCollection = getCollectionState(collectionName);
        assertNotNull(docCollection);
        assertEquals("Expecting 2 shards",
            2, docCollection.getSlices().size());
        assertEquals("Expecting 4 relpicas per shard",
            8, docCollection.getReplicas().size());
        assertEquals("Expecting 8 tlog replicas, 4 per shard",
            8, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
        assertEquals("Expecting no nrt replicas",
            0, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
        assertEquals("Expecting no pull replicas",
            0, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
        for (Slice s:docCollection.getSlices()) {
          assertTrue(s.getLeader().getType() == Replica.Type.TLOG);
          List<String> shardElectionNodes = cluster.getZkClient().getChildren(ZkStateReader.getShardLeadersElectPath(collectionName, s.getName()), null, true);
          assertEquals("Unexpected election nodes for Shard: " + s.getName() + ": " + Arrays.toString(shardElectionNodes.toArray()), 
              4, shardElectionNodes.size());
        }
        assertUlogPresence(docCollection);
        if (reloaded) {
          break;
        } else {
          // reload
          CollectionAdminResponse response = CollectionAdminRequest.reloadCollection(collectionName)
          .process(cluster.getSolrClient());
          assertEquals(0, response.getStatus());
          reloaded = true;
        }
      }
    } finally {
      zkClient().printLayoutToStdOut();
    }
  }
  
  @SuppressWarnings("unchecked")
  public void testAddDocs() throws Exception {
    int numTlogReplicas = 1 + random().nextInt(3);
    DocCollection docCollection = createAndWaitForCollection(1, 0, numTlogReplicas, 0);
    assertEquals(1, docCollection.getSlices().size());
    
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    
    Slice s = docCollection.getSlices().iterator().next();
    try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
      assertEquals(1, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    TimeOut t = new TimeOut(REPLICATION_TIMEOUT_SECS, TimeUnit.SECONDS);
    for (Replica r:s.getReplicas(EnumSet.of(Replica.Type.TLOG))) {
      //TODO: assert replication < REPLICATION_TIMEOUT_SECS
      try (HttpSolrClient tlogReplicaClient = getHttpSolrClient(r.getCoreUrl())) {
        while (true) {
          try {
            assertEquals("Replica " + r.getName() + " not up to date after 10 seconds",
                1, tlogReplicaClient.query(new SolrQuery("*:*")).getResults().getNumFound());
            // Append replicas process all updates
            SolrQuery req = new SolrQuery(
                "qt", "/admin/plugins",
                "stats", "true");
            QueryResponse statsResponse = tlogReplicaClient.query(req);
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
  
  public void testAddRemoveTlogReplica() throws Exception {
    DocCollection docCollection = createAndWaitForCollection(2, 0, 1, 0);
    assertEquals(2, docCollection.getSlices().size());
    
    addReplicaToShard("shard1", Replica.Type.TLOG);
    docCollection = assertNumberOfReplicas(0, 3, 0, true, false);
    addReplicaToShard("shard2", Replica.Type.TLOG);
    docCollection = assertNumberOfReplicas(0, 4, 0, true, false);
    
    waitForState("Expecting collection to have 2 shards and 2 replica each", collectionName, clusterShape(2, 2));
    
    //Delete tlog replica from shard1
    CollectionAdminRequest.deleteReplica(
        collectionName, 
        "shard1", 
        docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.TLOG)).get(0).getName())
    .process(cluster.getSolrClient());
    assertNumberOfReplicas(0, 3, 0, true, true);
  }
  
  private void addReplicaToShard(String shardName, Replica.Type type) throws ClientProtocolException, IOException, SolrServerException {
    switch (random().nextInt(3)) {
      case 0: // Add replica with SolrJ
        CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(collectionName, shardName, type).process(cluster.getSolrClient());
        assertEquals("Unexpected response status: " + response.getStatus(), 0, response.getStatus());
        break;
      case 1: // Add replica with V1 API
        String url = String.format(Locale.ROOT, "%s/admin/collections?action=ADDREPLICA&collection=%s&shard=%s&type=%s", 
            cluster.getRandomJetty(random()).getBaseUrl(), 
            collectionName,
            shardName,
            type);
        HttpGet addReplicaGet = new HttpGet(url);
        HttpResponse httpResponse = cluster.getSolrClient().getHttpClient().execute(addReplicaGet);
        assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        break;
      case 2:// Add replica with V2 API
        url = String.format(Locale.ROOT, "%s/____v2/c/%s/shards", 
            cluster.getRandomJetty(random()).getBaseUrl(), 
            collectionName);
        String requestBody = String.format(Locale.ROOT, "{add-replica:{shard:%s, type:%s}}", 
            shardName,
            type);
        HttpPost addReplicaPost = new HttpPost(url);
        addReplicaPost.setHeader("Content-type", "application/json");
        addReplicaPost.setEntity(new StringEntity(requestBody));
        httpResponse = cluster.getSolrClient().getHttpClient().execute(addReplicaPost);
        assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        break;
    }
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
    int numNrtReplicas = random().nextBoolean()?0:2;
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, numNrtReplicas, numReplicas, 0)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
    waitForState("Unexpected replica count", collectionName, activeReplicaCount(numNrtReplicas, numReplicas, 0));
    DocCollection docCollection = assertNumberOfReplicas(numNrtReplicas, numReplicas, 0, false, true);
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
    
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)), REPLICATION_TIMEOUT_SECS);
    
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
    waitForNumDocsInAllReplicas(2, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)), REPLICATION_TIMEOUT_SECS);
    // Start back the node
    if (removeReplica) {
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.TLOG).process(cluster.getSolrClient());
    } else {
      ChaosMonkey.start(leaderJetty);
    }
    waitForState("Expected collection to be 1x2", collectionName, clusterShape(1, 2));
    // added replica should replicate from the leader
    waitForNumDocsInAllReplicas(2, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)), REPLICATION_TIMEOUT_SECS);
  }
  
  public void testKillTlogReplica() throws Exception {
    DocCollection docCollection = createAndWaitForCollection(1, 0, 2, 0);
    
    waitForNumDocsInAllActiveReplicas(0);
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(1);
    
    JettySolrRunner pullReplicaJetty = cluster.getReplicaJetty(docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.TLOG)).get(0));
    ChaosMonkey.kill(pullReplicaJetty);
    waitForState("Replica not removed", collectionName, activeReplicaCount(0, 1, 0));
//    // Also wait for the replica to be placed in state="down"
//    waitForState("Didn't update state", collectionName, clusterStateReflectsActiveAndDownReplicas());
    
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(2);
    
    ChaosMonkey.start(pullReplicaJetty);
    waitForState("Replica not added", collectionName, activeReplicaCount(0, 2, 0));
    waitForNumDocsInAllActiveReplicas(2);
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
      long docsPending = (long) getSolrCore(true).get(0).getMetricRegistry().getGauges().get("UPDATE.updateHandler.docsPending").getValue();
      assertEquals("Expected 4 docs are pending in core " + getSolrCore(true).get(0).getCoreDescriptor(),4, docsPending);
    }

    for (SolrCore solrCore : getSolrCore(false)) {
      long docsPending = (long) solrCore.getMetricRegistry().getGauges().get("UPDATE.updateHandler.docsPending").getValue();
      assertEquals("Expected non docs are pending in core " + solrCore.getCoreDescriptor(),0, docsPending);
    }

    checkRTG(1, 4, cluster.getJettySolrRunners());

    new UpdateRequest()
        .deleteById("1")
        .deleteByQuery("id:2")
        .process(cloudClient, collectionName);

    // The DBQ is not processed at replicas, so we still can get doc2 and other docs by RTG
    checkRTG(2,4, getSolrRunner(false));

    Map<SolrCore, Long> timeCopyOverPerCores = getTimesCopyOverOldUpdates(getSolrCore(false));
    new UpdateRequest()
        .commit(cloudClient, collectionName);

    waitForNumDocsInAllActiveReplicas(2);
    // There are a small delay between new searcher and copy over old updates operation
    TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS);
    while (!timeOut.hasTimedOut()) {
      if (assertCopyOverOldUpdates(1, timeCopyOverPerCores)) {
        break;
      } else {
        Thread.sleep(500);
      }
    }
    assertTrue("Expect only one copy over updates per cores", assertCopyOverOldUpdates(1, timeCopyOverPerCores));

    boolean firstCommit = true;
    // UpdateLog copy over old updates
    for (int i = 15; i <= 150; i++) {
      cloudClient.add(collectionName, sdoc("id",String.valueOf(i)));
      if (random().nextInt(100) < 15 & i != 150) {
        if (firstCommit) {
          // because tlog replicas periodically ask leader for new segments,
          // therefore the copy over old updates action must not be triggered until
          // tlog replicas actually get new segments
          assertTrue("Expect only one copy over updates per cores", assertCopyOverOldUpdates(1, timeCopyOverPerCores));
          firstCommit = false;
        }
        cloudClient.commit(collectionName);
      }
    }
    checkRTG(120,150, cluster.getJettySolrRunners());
    waitForReplicasCatchUp(20);
  }
  
  @SuppressWarnings("unchecked")
  public void testRecovery() throws Exception {
    boolean useKill = random().nextBoolean();
    createAndWaitForCollection(1, 0, 2, 0);
    
    CloudSolrClient cloudClient = cluster.getSolrClient();
    new UpdateRequest()
        .add(sdoc("id", "3"))
        .add(sdoc("id", "4"))
        .commit(cloudClient, collectionName);
    new UpdateRequest()
        .add(sdoc("id", "5"))
        .process(cloudClient, collectionName);
    JettySolrRunner solrRunner = getSolrRunner(false).get(0);
    if (useKill) { 
      ChaosMonkey.kill(solrRunner);
    } else {
      ChaosMonkey.stop(solrRunner);
    }
    waitForState("Replica still up", collectionName, activeReplicaCount(0,1,0));
    new UpdateRequest()
        .add(sdoc("id", "6"))
        .process(cloudClient, collectionName);
    ChaosMonkey.start(solrRunner);
    waitForState("Replica didn't recover", collectionName, activeReplicaCount(0,2,0));
    // We skip peerSync, so replica will always trigger commit on leader
    // We query only the non-leader replicas, since we haven't opened a new searcher on the leader yet
    waitForNumDocsInAllReplicas(4, getNonLeaderReplias(collectionName), 10); //timeout for stale collection state
    
    // If I add the doc immediately, the leader fails to communicate with the follower with broken pipe.
    // Options are, wait or retry...
    for (int i = 0; i < 3; i++) {
      UpdateRequest ureq = new UpdateRequest().add(sdoc("id", "7"));
      ureq.setParam("collection", collectionName);
      ureq.setParam(UpdateRequest.MIN_REPFACT, "2");
      NamedList<Object> response = cloudClient.request(ureq);
      if ((Integer)((NamedList<Object>)response.get("responseHeader")).get(UpdateRequest.REPFACT) >= 2) {
        break;
      }
      LOG.info("Min RF not achieved yet. retrying");
    }
    checkRTG(3,7, cluster.getJettySolrRunners());
    DirectUpdateHandler2.commitOnClose = false;
    ChaosMonkey.stop(solrRunner);
    waitForState("Replica still up", collectionName, activeReplicaCount(0,1,0));
    DirectUpdateHandler2.commitOnClose = true;
    ChaosMonkey.start(solrRunner);
    waitForState("Replica didn't recover", collectionName, activeReplicaCount(0,2,0));
    waitForNumDocsInAllReplicas(5, getNonLeaderReplias(collectionName), 10); //timeout for stale collection state
    checkRTG(3,7, cluster.getJettySolrRunners());
    cluster.getSolrClient().commit(collectionName);

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
    for (SolrCore solrCore : getSolrCore(false)) {
      RefCounted<IndexWriter> iwRef = solrCore.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
      assertFalse("IndexWriter at replicas must not see updates ", iwRef.get().hasUncommittedChanges());
      iwRef.decref();
    }
  }
  
  private List<Replica> getNonLeaderReplias(String collectionName) {
    return getCollectionState(collectionName).getReplicas().stream().filter(
        (r)-> !r.getBool("leader", false)).collect(Collectors.toList());
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
    String oldLeaderNodeName = oldLeaderJetty.getNodeName();
    ChaosMonkey.kill(oldLeaderJetty);
    waitForState("Replica not removed", collectionName, activeReplicaCount(0, 1, 0));
    waitForState("Expect new leader", collectionName,
        (liveNodes, collectionState) -> {
          Replica leader = collectionState.getLeader("shard1");
          if (leader == null) return false;
          return !leader.getNodeName().equals(oldLeaderNodeName);
        }
    );
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

  private DocCollection createAndWaitForCollection(int numShards, int numNrtReplicas, int numTlogReplicas, int numPullReplicas) throws SolrServerException, IOException, KeeperException, InterruptedException {
    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, numNrtReplicas, numTlogReplicas, numPullReplicas)
    .setMaxShardsPerNode(100)
    .process(cluster.getSolrClient());
    int numReplicasPerShard = numNrtReplicas + numTlogReplicas + numPullReplicas;
    waitForState("Expected collection to be created with " + numShards + " shards and  " + numReplicasPerShard + " replicas",
        collectionName, clusterShape(numShards, numReplicasPerShard));
    return assertNumberOfReplicas(numNrtReplicas*numShards, numTlogReplicas*numShards, numPullReplicas*numShards, false, true);
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
            assertEquals("Replica " + r.getName() + " not up to date after " + timeout + " seconds",
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
  
  private DocCollection assertNumberOfReplicas(int numNrtReplicas, int numTlogReplicas, int numPullReplicas, boolean updateCollection, boolean activeOnly) throws KeeperException, InterruptedException {
    if (updateCollection) {
      cluster.getSolrClient().getZkStateReader().forceUpdateCollection(collectionName);
    }
    DocCollection docCollection = getCollectionState(collectionName);
    assertNotNull(docCollection);
    assertEquals("Unexpected number of nrt replicas: " + docCollection, numNrtReplicas, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of pull replicas: " + docCollection, numPullReplicas, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of tlog replicas: " + docCollection, numTlogReplicas, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
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
  
  
  private CollectionStatePredicate activeReplicaCount(int numNrtReplicas, int numTlogReplicas, int numPullReplicas) {
    return (liveNodes, collectionState) -> {
      int nrtFound = 0, tlogFound = 0, pullFound = 0;
      if (collectionState == null)
        return false;
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            switch (replica.getType()) {
              case TLOG:
                tlogFound++;
                break;
              case PULL:
                pullFound++;
                break;
              case NRT:
                nrtFound++;
                break;
              default:
                throw new AssertionError("Unexpected replica type");
            }
        }
      }
      return numNrtReplicas == nrtFound && numTlogReplicas == tlogFound && numPullReplicas == pullFound;
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

  private boolean assertCopyOverOldUpdates(long delta, Map<SolrCore, Long> timesPerCore) {
    for (SolrCore core : timesPerCore.keySet()) {
      if (timesPerCore.get(core) + delta != getTimesCopyOverOldUpdates(core)) return false;
    }
    return true;
  }

  private Map<SolrCore, Long> getTimesCopyOverOldUpdates(List<SolrCore> cores) {
    Map<SolrCore, Long> timesPerCore = new HashMap<>();
    for (SolrCore core : cores) {
      long times = getTimesCopyOverOldUpdates(core);
      timesPerCore.put(core, times);
    }
    return timesPerCore;
  }

  private long getTimesCopyOverOldUpdates(SolrCore core) {
    return ((Meter)core.getMetricRegistry().getMetrics().get("TLOG.copyOverOldUpdates.ops")).getCount();
  }
}
