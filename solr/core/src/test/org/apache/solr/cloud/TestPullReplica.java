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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
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
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

@Slow
@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
public class TestPullReplica extends SolrCloudTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private String collectionName = null;
  private final static int REPLICATION_TIMEOUT_SECS = 30;
  
  private String suggestedCollectionName() {
    return (getTestClass().getSimpleName().replace("Test", "") + "_" + getSaferTestName().split(" ")[0]).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase(Locale.ROOT);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    TestInjection.waitForReplicasInSync = null; // We'll be explicit about this in this test
   //  cloudSolrClientMaxStaleRetries
   System.setProperty("cloudSolrClientMaxStaleRetries", "1");
   System.setProperty("zkReaderGetLeaderRetryTimeoutMs", "1000");
   
   configureCluster(2) // 2 + random().nextInt(3) 
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    Boolean useLegacyCloud = rarely();
    log.info("Using legacyCloud?: {}", useLegacyCloud);
    CollectionAdminRequest.ClusterProp clusterPropRequest = CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, String.valueOf(useLegacyCloud));
    CollectionAdminResponse response = clusterPropRequest.process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
  }
  
  @AfterClass
  public static void tearDownCluster() {
    System.clearProperty("cloudSolrClientMaxStaleRetries");
    System.clearProperty("zkReaderGetLeaderRetryTimeoutMs");
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
        log.warn("Jetty {} not running, probably some bad test. Starting it", jetty.getLocalPort());
        jetty.start();
      }
    }
    if (cluster.getSolrClient().getZkStateReader().getClusterState().getCollectionOrNull(collectionName) != null) {
      log.info("tearDown deleting collection");
      CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
      log.info("Collection deleted");
      waitForDeletion(collectionName);
    }
    super.tearDown();
  }
  
  @Repeat(iterations=2) // 2 times to make sure cleanup is complete and we can create the same collection
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  public void testCreateDelete() throws Exception {
    try {
      switch (random().nextInt(3)) {
        case 0:
          // Sometimes use SolrJ
          CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1, 0, 3)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());
          break;
        case 1:
          // Sometimes use v1 API
          String url = String.format(Locale.ROOT, "%s/admin/collections?action=CREATE&name=%s&collection.configName=%s&numShards=%s&pullReplicas=%s&maxShardsPerNode=%s",
              cluster.getRandomJetty(random()).getBaseUrl(),
              collectionName, "conf",
              2,    // numShards
              3,    // pullReplicas
              100); // maxShardsPerNode
          url = url + pickRandom("", "&nrtReplicas=1", "&replicationFactor=1"); // These options should all mean the same
          HttpGet createCollectionGet = new HttpGet(url);
          cluster.getSolrClient().getHttpClient().execute(createCollectionGet);
          break;
        case 2:
          // Sometimes use V2 API
          url = cluster.getRandomJetty(random()).getBaseUrl().toString() + "/____v2/c";
          String requestBody = String.format(Locale.ROOT, "{create:{name:%s, config:%s, numShards:%s, pullReplicas:%s, maxShardsPerNode:%s %s}}",
              collectionName, "conf",
              2,    // numShards
              3,    // pullReplicas
              100, // maxShardsPerNode
              pickRandom("", ", nrtReplicas:1", ", replicationFactor:1")); // These options should all mean the same
          HttpPost createCollectionPost = new HttpPost(url);
          createCollectionPost.setHeader("Content-type", "application/json");
          createCollectionPost.setEntity(new StringEntity(requestBody));
          HttpResponse httpResponse = cluster.getSolrClient().getHttpClient().execute(createCollectionPost);
          assertEquals(200, httpResponse.getStatusLine().getStatusCode());
          break;
      }
      boolean reloaded = false;
      while (true) {
        DocCollection docCollection = getCollectionState(collectionName);
        assertNotNull(docCollection);
        assertEquals("Expecting 4 relpicas per shard",
            8, docCollection.getReplicas().size());
        assertEquals("Expecting 6 pull replicas, 3 per shard",
            6, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
        assertEquals("Expecting 2 writer replicas, one per shard",
            2, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
        for (Slice s:docCollection.getSlices()) {
          // read-only replicas can never become leaders
          assertFalse(s.getLeader().getType() == Replica.Type.PULL);
          List<String> shardElectionNodes = cluster.getZkClient().getChildren(ZkStateReader.getShardLeadersElectPath(collectionName, s.getName()), null, true);
          assertEquals("Unexpected election nodes for Shard: " + s.getName() + ": " + Arrays.toString(shardElectionNodes.toArray()), 
              1, shardElectionNodes.size());
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
  
  /**
   * Asserts that Update logs don't exist for replicas of type {@link org.apache.solr.common.cloud.Replica.Type#PULL}
   */
  private void assertUlogPresence(DocCollection collection) {
    for (Slice s:collection.getSlices()) {
      for (Replica r:s.getReplicas()) {
        if (r.getType() == Replica.Type.NRT) {
          continue;
        }
        SolrCore core = null;
        try {
          core = cluster.getReplicaJetty(r).getCoreContainer().getCore(r.getCoreName());
          assertNotNull(core);
          assertFalse("Update log should not exist for replicas of type Passive but file is present: " + core.getUlogDir(),
              new java.io.File(core.getUlogDir()).exists());
        } finally {
          core.close();
        }
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testAddDocs() throws Exception {
    int numPullReplicas = 1 + random().nextInt(3);
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, numPullReplicas)
    .setMaxShardsPerNode(100)
    .process(cluster.getSolrClient());
    waitForState("Expected collection to be created with 1 shard and " + (numPullReplicas + 1) + " replicas", collectionName, clusterShape(1, numPullReplicas + 1));
    DocCollection docCollection = assertNumberOfReplicas(1, 0, numPullReplicas, false, true);
    assertEquals(1, docCollection.getSlices().size());
    
    boolean reloaded = false;
    int numDocs = 0;
    while (true) {
      numDocs++;
      cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", String.valueOf(numDocs), "foo", "bar"));
      cluster.getSolrClient().commit(collectionName);
      
      Slice s = docCollection.getSlices().iterator().next();
      try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
        assertEquals(numDocs, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
      }
      
      TimeOut t = new TimeOut(REPLICATION_TIMEOUT_SECS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      for (Replica r:s.getReplicas(EnumSet.of(Replica.Type.PULL))) {
        //TODO: assert replication < REPLICATION_TIMEOUT_SECS
        try (HttpSolrClient pullReplicaClient = getHttpSolrClient(r.getCoreUrl())) {
          while (true) {
            try {
              assertEquals("Replica " + r.getName() + " not up to date after 10 seconds",
                  numDocs, pullReplicaClient.query(new SolrQuery("*:*")).getResults().getNumFound());
              break;
            } catch (AssertionError e) {
              if (t.hasTimedOut()) {
                throw e;
              } else {
                Thread.sleep(100);
              }
            }
          }
          SolrQuery req = new SolrQuery(
              "qt", "/admin/plugins",
              "stats", "true");
          QueryResponse statsResponse = pullReplicaClient.query(req);
          assertEquals("Replicas shouldn't process the add document request: " + statsResponse, 
              0L, ((Map<String, Object>)((NamedList<Object>)statsResponse.getResponse()).findRecursive("plugins", "UPDATE", "updateHandler", "stats")).get("UPDATE.updateHandler.adds"));
        }
      }
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
    assertUlogPresence(docCollection);
  }
  
  public void testAddRemovePullReplica() throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1, 0, 0)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
    waitForState("Expected collection to be created with 2 shards and 1 replica each", collectionName, clusterShape(2, 2));
    DocCollection docCollection = assertNumberOfReplicas(2, 0, 0, false, true);
    assertEquals(2, docCollection.getSlices().size());
    
    addReplicaToShard("shard1", Replica.Type.PULL);
    docCollection = assertNumberOfReplicas(2, 0, 1, true, false);
    addReplicaToShard("shard2", Replica.Type.PULL);
    docCollection = assertNumberOfReplicas(2, 0, 2, true, false);
    
    waitForState("Expecting collection to have 2 shards and 2 replica each", collectionName, clusterShape(2, 4));
    
    //Delete pull replica from shard1
    CollectionAdminRequest.deleteReplica(
        collectionName, 
        "shard1", 
        docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getName())
    .process(cluster.getSolrClient());
    assertNumberOfReplicas(2, 0, 1, true, true);
  }
  
  public void testRemoveAllWriterReplicas() throws Exception {
    doTestNoLeader(true);
  }

  @Test
  //2018-06-18 (commented) @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  public void testKillLeader() throws Exception {
    doTestNoLeader(false);
  }
  
  @Ignore("Ignore until I figure out a way to reliably record state transitions")
  public void testPullReplicaStates() throws Exception {
    // Validate that pull replicas go through the correct states when starting, stopping, reconnecting
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 0)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
//    cluster.getSolrClient().getZkStateReader().registerCore(collectionName); //TODO: Is this needed? 
    waitForState("Replica not added", collectionName, activeReplicaCount(1, 0, 0));
    addDocs(500);
    List<Replica.State> statesSeen = new ArrayList<>(3);
    cluster.getSolrClient().registerCollectionStateWatcher(collectionName, (liveNodes, collectionState) -> {
      Replica r = collectionState.getSlice("shard1").getReplica("core_node2");
      log.info("CollectionStateWatcher state change: {}", r);
      if (r == null) {
        return false;
      }
      statesSeen.add(r.getState());
      log.info("CollectionStateWatcher saw state: {}", r.getState());
      return r.getState() == Replica.State.ACTIVE;
    });
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.PULL).process(cluster.getSolrClient());
    waitForState("Replica not added", collectionName, activeReplicaCount(1, 0, 1));
    zkClient().printLayoutToStdOut();
    log.info("Saw states: " + Arrays.toString(statesSeen.toArray()));
    assertEquals("Expecting DOWN->RECOVERING->ACTIVE but saw: " + Arrays.toString(statesSeen.toArray()), 3, statesSeen.size());
    assertEquals("Expecting DOWN->RECOVERING->ACTIVE but saw: " + Arrays.toString(statesSeen.toArray()), Replica.State.DOWN, statesSeen.get(0));
    assertEquals("Expecting DOWN->RECOVERING->ACTIVE but saw: " + Arrays.toString(statesSeen.toArray()), Replica.State.RECOVERING, statesSeen.get(0));
    assertEquals("Expecting DOWN->RECOVERING->ACTIVE but saw: " + Arrays.toString(statesSeen.toArray()), Replica.State.ACTIVE, statesSeen.get(0));
  }
  
  public void testRealTimeGet() throws SolrServerException, IOException, KeeperException, InterruptedException {
    // should be redirected to Replica.Type.NRT
    int numReplicas = random().nextBoolean()?1:2;
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, numReplicas, 0, numReplicas)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
    waitForState("Unexpected replica count", collectionName, activeReplicaCount(numReplicas, 0, numReplicas));
    DocCollection docCollection = assertNumberOfReplicas(numReplicas, 0, numReplicas, false, true);
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
   * validate that replication still happens on a new leader
   */
  private void doTestNoLeader(boolean removeReplica) throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 1)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
    waitForState("Expected collection to be created with 1 shard and 2 replicas", collectionName, clusterShape(1, 2));
    DocCollection docCollection = assertNumberOfReplicas(1, 0, 1, false, true);
    
    // Add a document and commit
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    Slice s = docCollection.getSlices().iterator().next();
    try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
      assertEquals(1, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));
    
    // Delete leader replica from shard1
    ignoreException("No registered leader was found"); //These are expected
    JettySolrRunner leaderJetty = null;
    if (removeReplica) {
      CollectionAdminRequest.deleteReplica(
          collectionName, 
          "shard1", 
          s.getLeader().getName())
      .process(cluster.getSolrClient());
    } else {
      leaderJetty = cluster.getReplicaJetty(s.getLeader());
      leaderJetty.stop();
      waitForState("Leader replica not removed", collectionName, clusterShape(1, 1));
      // Wait for cluster state to be updated
      waitForState("Replica state not updated in cluster state", 
          collectionName, clusterStateReflectsActiveAndDownReplicas());
    }
    docCollection = assertNumberOfReplicas(0, 0, 1, true, true);
    
    // Check that there is no leader for the shard
    Replica leader = docCollection.getSlice("shard1").getLeader();
    assertTrue(leader == null || !leader.isActive(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes()));
    
    // Pull replica on the other hand should be active
    Replica pullReplica = docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.PULL)).get(0);
    assertTrue(pullReplica.isActive(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes()));

    long highestTerm = 0L;
    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "shard1", zkClient())) {
      highestTerm = zkShardTerms.getHighestTerm();
    }
    // add document, this should fail since there is no leader. Pull replica should not accept the update
    expectThrows(SolrException.class, () -> 
      cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "zoo"))
    );
    if (removeReplica) {
      try(ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "shard1", zkClient())) {
        assertEquals(highestTerm, zkShardTerms.getHighestTerm());
      }
    }
    
    // Also fails if I send the update to the pull replica explicitly
    try (HttpSolrClient pullReplicaClient = getHttpSolrClient(docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
      expectThrows(SolrException.class, () -> 
        cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "zoo"))
      );
    }
    if (removeReplica) {
      try(ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "shard1", zkClient())) {
        assertEquals(highestTerm, zkShardTerms.getHighestTerm());
      }
    }
    
    // Queries should still work
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));
    // Add nrt replica back. Since there is no nrt now, new nrt will have no docs. There will be data loss, since the it will become the leader
    // and pull replicas will replicate from it. Maybe we want to change this. Replicate from pull replicas is not a good idea, since they
    // are by definition out of date.
    if (removeReplica) {
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.NRT).process(cluster.getSolrClient());
    } else {
      leaderJetty.stop();
    }
    waitForState("Expected collection to be 1x2", collectionName, clusterShape(1, 2));
    unIgnoreException("No registered leader was found"); // Should have a leader from now on

    // Validate that the new nrt replica is the leader now
    cluster.getSolrClient().getZkStateReader().forceUpdateCollection(collectionName);
    docCollection = getCollectionState(collectionName);
    leader = docCollection.getSlice("shard1").getLeader();
    assertTrue(leader != null && leader.isActive(cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes()));

    // If jetty is restarted, the replication is not forced, and replica doesn't replicate from leader until new docs are added. Is this the correct behavior? Why should these two cases be different?
    if (removeReplica) {
      // Pull replicas will replicate the empty index if a new replica was added and becomes leader
      waitForNumDocsInAllReplicas(0, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));
    }
    
    // add docs agin
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "zoo"));
    s = docCollection.getSlices().iterator().next();
    try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
      leaderClient.commit();
      assertEquals(1, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)), "id:2");
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));
  }
  
  public void testKillPullReplica() throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 1)
      .setMaxShardsPerNode(100)
      .process(cluster.getSolrClient());
//    cluster.getSolrClient().getZkStateReader().registerCore(collectionName); //TODO: Is this needed? 
    waitForState("Expected collection to be created with 1 shard and 2 replicas", collectionName, clusterShape(1, 2));
    DocCollection docCollection = assertNumberOfReplicas(1, 0, 1, false, true);
    assertEquals(1, docCollection.getSlices().size());
    
    waitForNumDocsInAllActiveReplicas(0);
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(1);
    
    JettySolrRunner pullReplicaJetty = cluster.getReplicaJetty(docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.PULL)).get(0));
    pullReplicaJetty.stop();
    waitForState("Replica not removed", collectionName, activeReplicaCount(1, 0, 0));
    // Also wait for the replica to be placed in state="down"
    waitForState("Didn't update state", collectionName, clusterStateReflectsActiveAndDownReplicas());
    
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(2);
    
    pullReplicaJetty.start();
    waitForState("Replica not added", collectionName, activeReplicaCount(1, 0, 1));
    waitForNumDocsInAllActiveReplicas(2);
  }
  
  public void testSearchWhileReplicationHappens() {
      
  }
  
  private void waitForNumDocsInAllActiveReplicas(int numDocs) throws IOException, SolrServerException, InterruptedException {
    DocCollection docCollection = getCollectionState(collectionName);
    waitForNumDocsInAllReplicas(numDocs, docCollection.getReplicas().stream().filter(r -> r.getState() == Replica.State.ACTIVE).collect(Collectors.toList()));
  }
    
  private void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas) throws IOException, SolrServerException, InterruptedException {
    waitForNumDocsInAllReplicas(numDocs, replicas, "*:*");
  }
  
  private void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas, String query) throws IOException, SolrServerException, InterruptedException {
    TimeOut t = new TimeOut(REPLICATION_TIMEOUT_SECS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    for (Replica r:replicas) {
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
    TimeOut t = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection(collection)) {
      log.info("Collection not yet deleted");
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
    assertEquals("Unexpected number of writer replicas: " + docCollection, numNrtReplicas, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of pull replicas: " + docCollection, numPullReplicas, 
        docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).stream().filter(r->!activeOnly || r.getState() == Replica.State.ACTIVE).count());
    assertEquals("Unexpected number of active replicas: " + docCollection, numTlogReplicas, 
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
  
  private void addDocs(int numDocs) throws SolrServerException, IOException {
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "fieldName_s", String.valueOf(i)));
    }
    cluster.getSolrClient().add(collectionName, docs);
    cluster.getSolrClient().commit(collectionName);
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
}
