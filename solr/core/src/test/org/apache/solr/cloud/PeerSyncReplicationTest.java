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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;

/**
 * Test PeerSync when a node restarts and documents are indexed when node was down.
 *
 * This test is modeled after SyncSliceTest
 */
@Slow
@LuceneTestCase.Nightly
@Ignore // MRM TODO:
public class PeerSyncReplicationTest extends SolrCloudBridgeTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean success = false;
  int docId = 0;

  List<JettySolrRunner> nodesDown = new ArrayList<>();

  @BeforeClass
  public static void beforePeerSyncReplicationTest() throws Exception {
    // set socket timeout small, so replica won't be put into LIR state when they restart
    System.setProperty("distribUpdateSoTimeout", "3000");
    // tlog gets deleted after node restarts if we use CachingDirectoryFactory.
    // make sure that tlog stays intact after we restart a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
  }

  @AfterClass
  public static void afterPeerSyncReplicationTest() throws Exception {

  }

  public PeerSyncReplicationTest() {
    super();
    sliceCount = 1;
    replicationFactor = 3;
    numJettys = 3;
    solrconfigString = "solrconfig-tlog.xml";
    schemaString = "schema.xml";
  }

  @Test
  //commented 2-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    // index enough docs and commit to establish frame of reference for PeerSync
    for (int i = 0; i < 100; i++) {
      indexDoc(id, docId, i1, 50, tlong, 50, t1,
          "document number " + docId++);
    }
    commit();

    try {
      //checkShardConsistency(false, true);

      long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
      assertEquals(docId, cloudClientDocs);

      Replica initialLeaderInfo = getShardLeader(COLLECTION, "s1", 10000);
      JettySolrRunner initialLeaderJetty = getJettyOnPort(getReplicaPort(initialLeaderInfo));
      List<JettySolrRunner> otherJetties = getOtherAvailableJetties(initialLeaderJetty);

      assertTrue(otherJetties.size() > 0);


      JettySolrRunner neverLeader = otherJetties.get(otherJetties.size() - 1);
      otherJetties.remove(neverLeader) ;

      // first shutdown a node that will never be a leader
      forceNodeFailures(Collections.singletonList(neverLeader));

      // node failure and recovery via PeerSync
      log.info("Forcing PeerSync");
      JettySolrRunner nodePeerSynced = forceNodeFailureAndDoPeerSync(false);

      // add a few more docs
      indexDoc(id, docId, i1, 50, tlong, 50, t1,
          "document number " + docId++);
      indexDoc(id, docId, i1, 50, tlong, 50, t1,
          "document number " + docId++);
      commit();

      cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
      assertEquals(docId, cloudClientDocs);

      // now shutdown all other nodes except for 'nodeShutDownForFailure'
      otherJetties.remove(nodePeerSynced);
      forceNodeFailures(otherJetties);
      //waitForThingsToLevelOut(30, TimeUnit.SECONDS);
     // checkShardConsistency(false, true);

      // now shutdown the original leader
      log.info("Now shutting down initial leader");
      forceNodeFailures(singletonList(initialLeaderJetty));
      log.info("Updating mappings from zk");
      AbstractDistribZkTestBase.waitForNewLeader(cloudClient, COLLECTION, "s1", initialLeaderInfo, new TimeOut(15, TimeUnit.SECONDS, TimeSource.NANO_TIME));

      JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(getShardLeader(COLLECTION, "s1", 10000)));

      assertEquals("PeerSynced node did not become leader", nodePeerSynced, leaderJetty);

      // bring up node that was down all along, and let it PeerSync from the node that was forced to PeerSynce  
      bringUpDeadNodeAndEnsureNoReplication(neverLeader, false);
      //waitTillNodesActive();

      //checkShardConsistency(false, true);

      
      // bring back all the nodes including initial leader 
      // (commented as reports Maximum concurrent create/delete watches above limit violation and reports thread leaks)
      /*for(int i = 0 ; i < nodesDown.size(); i++) {
        bringUpDeadNodeAndEnsureNoReplication(shardToLeaderJetty.get("s1"), neverLeader, false);
      }
      checkShardConsistency(false, true);*/

      // make sure leader has not changed after bringing initial leader back
      assertEquals(nodePeerSynced, getJettyOnPort(getReplicaPort(getShardLeader(COLLECTION, "s1", 10000))));

      // assert metrics
      SolrMetricManager manager = nodePeerSynced.getCoreContainer().getMetricManager();
      MetricRegistry registry = null;
      for (String name : manager.registryNames()) {
        if (name.startsWith("solr.core.collection1")) {
          registry = manager.registry(name);
          break;
        }
      }
      assertNotNull(registry);
      Map<String, Metric> metrics = registry.getMetrics();
      assertTrue("REPLICATION.peerSync.time present", metrics.containsKey("REPLICATION.peerSync.time"));
      assertTrue("REPLICATION.peerSync.errors present", metrics.containsKey("REPLICATION.peerSync.errors"));

      Counter counter = (Counter)metrics.get("REPLICATION.peerSync.errors");
      // MRM TODO:
      //assertEquals(0L, counter.getCount());
      success = true;
    } finally {
      System.clearProperty("solr.disableFingerprint");
    }
  }

  class IndexInBackGround extends Thread {
    private int numDocs;
    private JettySolrRunner runner;

    public IndexInBackGround(int numDocs, JettySolrRunner nodeToBringUp) {
      super(SolrTestCaseJ4.getClassName());
      this.numDocs = numDocs;
      this.runner = nodeToBringUp;
    }
    
    public void run() {
      Random random = LuceneTestCase.random();
      try {
        // If we don't wait for cores get loaded, the leader may put this replica into LIR state
        for (int i = 0; i < numDocs; i++) {
          indexDoc(random, id, docId, i1, 50, tlong, 50, t1, "document number " + docId);
          docId++;
          // slow down adds, to get documents indexed while in PeerSync
          Thread.sleep(20);
        }
      } catch (Exception e) {
        log.error("Error indexing doc in background", e);
        //Throwing an error here will kill the thread
      }
    }

  }
   

  private void forceNodeFailures(List<JettySolrRunner> replicasToShutDown) throws Exception {
    try (ParWork worker = new ParWork("stop_jetties")) {

      for (JettySolrRunner replicaToShutDown : replicasToShutDown) {
        worker.collect("shutdownReplicas", () -> {
          try {
            replicaToShutDown.stop();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
    }

    for (JettySolrRunner jetty : replicasToShutDown) {
      cluster.waitForJettyToStop(jetty);
    }


    int totalDown = 0;

    List<JettySolrRunner> jetties = getJettysForShard("s1");

    if (replicasToShutDown != null) {
      jetties.removeAll(replicasToShutDown);
      totalDown += replicasToShutDown.size();
    }

    jetties.removeAll(nodesDown);
    totalDown += nodesDown.size();

    assertEquals(getShardCount() - totalDown, jetties.size());

    nodesDown.addAll(replicasToShutDown);
  }
  
  

  private JettySolrRunner forceNodeFailureAndDoPeerSync(boolean disableFingerprint)
      throws Exception {
    // kill non leader - new leader could have all the docs or be missing one
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(getShardLeader(COLLECTION, "s1", 10000)));
    List<JettySolrRunner> nonLeaderJetties = getOtherAvailableJetties(leaderJetty);
    JettySolrRunner replicaToShutDown = nonLeaderJetties.get(random().nextInt(nonLeaderJetties.size())); // random non leader node

    forceNodeFailures(Arrays.asList(replicaToShutDown));

    // two docs need to be sync'd back when replica restarts
    indexDoc(id, docId, i1, 50, tlong, 50, t1,
        "document number " + docId++);
    indexDoc(id, docId, i1, 50, tlong, 50, t1,
        "document number " + docId++);
    commit();

    bringUpDeadNodeAndEnsureNoReplication(replicaToShutDown, disableFingerprint);

    return replicaToShutDown;
  }


  private void bringUpDeadNodeAndEnsureNoReplication(JettySolrRunner nodeToBringUp, boolean disableFingerprint)
      throws Exception {
    // disable fingerprint check if needed
    System.setProperty("solr.disableFingerprint", String.valueOf(disableFingerprint));
    // we wait a little bit, so socket between leader -> replica will be timeout
    Thread.sleep(500);
    IndexInBackGround iib = new IndexInBackGround(50, nodeToBringUp);
    iib.start();
    
    // bring back dead node and ensure it recovers
    nodeToBringUp.start();
    
    nodesDown.remove(nodeToBringUp);

    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    List<JettySolrRunner> jetties = getJettysForShard("s1");
    jetties.removeAll(nodesDown);
    assertEquals(getShardCount() - nodesDown.size(), jetties.size());
    
    iib.join();
    
    cloudClient.commit();
    
    //checkShardConsistency(false, false);
    
    long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(docId, cloudClientDocs);

    // if there was no replication, we should not have replication.properties file
    String replicationProperties = nodeToBringUp.getSolrHome() + "/cores/" + SolrTestCaseJ4.DEFAULT_TEST_COLLECTION_NAME + "/data/replication.properties";
    assertTrue("PeerSync failed. Had to fail back to replication", Files.notExists(Paths.get(replicationProperties)));
  }

  private List<JettySolrRunner> getOtherAvailableJetties(JettySolrRunner leader) {
    List<JettySolrRunner> candidates = getJettysForShard("s1");

    if (leader != null) {
      candidates.remove(leader);
    }

    candidates.removeAll(nodesDown);

    return candidates;
  }

  private List<JettySolrRunner> getJettysForShard(String shard) {
    List<JettySolrRunner> candidates = new ArrayList<>();

    Slice slice = cloudClient.getZkStateReader().getClusterState().getCollection(COLLECTION).getSlice(shard);
    for (Replica replica : slice) {
      int port = getReplicaPort(replica);
      candidates.add(getJettyOnPort(port));
    }
    return candidates;
  }

  protected void indexDoc(Object... fields) throws IOException,
          SolrServerException {
    indexDoc(random(), fields);
  }

  protected void indexDoc(Random random, Object... fields) throws IOException,
      SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();

    addFields(doc, fields);
    addFields(doc, "rnd_s", RandomStringUtils.random(random.nextInt(100) + 100));

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ModifiableSolrParams params = new ModifiableSolrParams();
    ureq.setParams(params);
    ureq.process(cloudClient);
  }

  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

}
