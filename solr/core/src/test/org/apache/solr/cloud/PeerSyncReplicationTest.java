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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.ZkTestServer.LimitViolationAction;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Test PeerSync when a node restarts and documents are indexed when node was down.
 *
 * This test is modeled after SyncSliceTest
 */
@Slow
public class PeerSyncReplicationTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean success = false;
  int docId = 0;

  List<CloudJettyRunner> nodesDown = new ArrayList<>();

  @Override
  public void distribTearDown() throws Exception {
    if (!success) {
      printLayoutOnTearDown = true;
    }
    System.clearProperty("distribUpdateSoTimeout");
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
    System.clearProperty("tests.zk.violationReportAction");
    super.distribTearDown();
  }

  public PeerSyncReplicationTest() {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }

  protected String getCloudSolrConfig() {
    return "solrconfig-tlog.xml";
  }

  @Override
  public void distribSetUp() throws Exception {
    // set socket timeout small, so replica won't be put into LIR state when they restart
    System.setProperty("distribUpdateSoTimeout", "3000");
    // tlog gets deleted after node restarts if we use CachingDirectoryFactory.
    // make sure that tlog stays intact after we restart a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    System.setProperty("tests.zk.violationReportAction", LimitViolationAction.IGNORE.toString());
    super.distribSetUp();
  }

  @Test
  //commented 2-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    waitForThingsToLevelOut(30);

    del("*:*");

    // index enough docs and commit to establish frame of reference for PeerSync
    for (int i = 0; i < 100; i++) {
      indexDoc(id, docId, i1, 50, tlong, 50, t1,
          "document number " + docId++);
    }
    commit();
    waitForThingsToLevelOut(30);

    try {
      checkShardConsistency(false, true);

      long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
      assertEquals(docId, cloudClientDocs);

      CloudJettyRunner initialLeaderJetty = shardToLeaderJetty.get("shard1");
      List<CloudJettyRunner> otherJetties = getOtherAvailableJetties(initialLeaderJetty);
      CloudJettyRunner neverLeader = otherJetties.get(otherJetties.size() - 1);
      otherJetties.remove(neverLeader) ;

      // first shutdown a node that will never be a leader
      forceNodeFailures(singletonList(neverLeader));

      // node failure and recovery via PeerSync
      log.info("Forcing PeerSync");
      CloudJettyRunner nodePeerSynced = forceNodeFailureAndDoPeerSync(false);

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
      waitForThingsToLevelOut(30);
      checkShardConsistency(false, true);

      // now shutdown the original leader
      log.info("Now shutting down initial leader");
      forceNodeFailures(singletonList(initialLeaderJetty));
      log.info("Updating mappings from zk");
      waitForNewLeader(cloudClient, "shard1", (Replica) initialLeaderJetty.client.info, new TimeOut(15, SECONDS, TimeSource.NANO_TIME));
      updateMappingsFromZk(jettys, clients, true);
      assertEquals("PeerSynced node did not become leader", nodePeerSynced, shardToLeaderJetty.get("shard1"));

      // bring up node that was down all along, and let it PeerSync from the node that was forced to PeerSynce  
      bringUpDeadNodeAndEnsureNoReplication(neverLeader, false);
      waitTillNodesActive();

      checkShardConsistency(false, true);

      
      // bring back all the nodes including initial leader 
      // (commented as reports Maximum concurrent create/delete watches above limit violation and reports thread leaks)
      /*for(int i = 0 ; i < nodesDown.size(); i++) {
        bringUpDeadNodeAndEnsureNoReplication(shardToLeaderJetty.get("shard1"), neverLeader, false);
      }
      checkShardConsistency(false, true);*/

      // make sure leader has not changed after bringing initial leader back
      assertEquals(nodePeerSynced, shardToLeaderJetty.get("shard1"));

      // assert metrics
      SolrMetricManager manager = nodePeerSynced.jetty.getCoreContainer().getMetricManager();
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
      assertEquals(0L, counter.getCount());
      success = true;
    } finally {
      System.clearProperty("solr.disableFingerprint");
    }
  }

  class IndexInBackGround extends Thread {
    private int numDocs;
    private CloudJettyRunner runner;

    public IndexInBackGround(int numDocs, CloudJettyRunner nodeToBringUp) {
      super(getClassName());
      this.numDocs = numDocs;
      this.runner = nodeToBringUp;
    }
    
    public void run() {
      try {
        // If we don't wait for cores get loaded, the leader may put this replica into LIR state
        waitForCoreLoading();
        for (int i = 0; i < numDocs; i++) {
          indexDoc(id, docId, i1, 50, tlong, 50, t1, "document number " + docId);
          docId++;
          // slow down adds, to get documents indexed while in PeerSync
          Thread.sleep(100);
        }
      } catch (Exception e) {
        log.error("Error indexing doc in background", e);
        //Throwing an error here will kill the thread
      }
    }

    private void waitForCoreLoading() throws InterruptedException {
      while (true) {
        if (runner.jetty.getCoreContainer() != null) {
          CoreContainer cc = runner.jetty.getCoreContainer();
          cc.waitForLoadingCoresToFinish(20000);
          break;
        }
        Thread.sleep(100);
      }
    }
  }
   

  private void forceNodeFailures(List<CloudJettyRunner> replicasToShutDown) throws Exception {
    for (CloudJettyRunner replicaToShutDown : replicasToShutDown) {
      replicaToShutDown.jetty.stop();
    }

    int totalDown = 0;

    Set<CloudJettyRunner> jetties = new HashSet<>();
    jetties.addAll(shardToJetty.get("shard1"));

    if (replicasToShutDown != null) {
      jetties.removeAll(replicasToShutDown);
      totalDown += replicasToShutDown.size();
    }

    jetties.removeAll(nodesDown);
    totalDown += nodesDown.size();

    assertEquals(getShardCount() - totalDown, jetties.size());

    nodesDown.addAll(replicasToShutDown);
  }
  
  

  private CloudJettyRunner forceNodeFailureAndDoPeerSync(boolean disableFingerprint)
      throws Exception {
    // kill non leader - new leader could have all the docs or be missing one
    CloudJettyRunner leaderJetty = shardToLeaderJetty.get("shard1");

    List<CloudJettyRunner> nonLeaderJetties = getOtherAvailableJetties(leaderJetty);
    CloudJettyRunner replicaToShutDown = nonLeaderJetties.get(random().nextInt(nonLeaderJetties.size())); // random non leader node

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


  private void bringUpDeadNodeAndEnsureNoReplication(CloudJettyRunner nodeToBringUp, boolean disableFingerprint)
      throws Exception {
    // disable fingerprint check if needed
    System.setProperty("solr.disableFingerprint", String.valueOf(disableFingerprint));
    // we wait a little bit, so socket between leader -> replica will be timeout
    Thread.sleep(3000);
    IndexInBackGround iib = new IndexInBackGround(50, nodeToBringUp);
    iib.start();
    
    // bring back dead node and ensure it recovers
    nodeToBringUp.jetty.start();
    
    nodesDown.remove(nodeToBringUp);

    waitTillNodesActive();
    waitForThingsToLevelOut(30);

    Set<CloudJettyRunner> jetties = new HashSet<>();
    jetties.addAll(shardToJetty.get("shard1"));
    jetties.removeAll(nodesDown);
    assertEquals(getShardCount() - nodesDown.size(), jetties.size());

    waitForThingsToLevelOut(30);
    
    iib.join();
    
    cloudClient.commit();
    
    checkShardConsistency(false, false);
    
    long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(docId, cloudClientDocs);

    // if there was no replication, we should not have replication.properties file
    String replicationProperties = nodeToBringUp.jetty.getSolrHome() + "/cores/" + DEFAULT_TEST_COLLECTION_NAME + "/data/replication.properties";
    assertTrue("PeerSync failed. Had to fail back to replication", Files.notExists(Paths.get(replicationProperties)));
  }

  
  
  private void waitTillNodesActive() throws Exception {
    for (int i = 0; i < 60; i++) {
      Thread.sleep(3000);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection1 = clusterState.getCollection("collection1");
      Slice slice = collection1.getSlice("shard1");
      Collection<Replica> replicas = slice.getReplicas();
      boolean allActive = true;

      Collection<String> nodesDownNames =
          nodesDown.stream()
              .map(n -> n.coreNodeName)
              .collect(Collectors.toList());

      Collection<Replica> replicasToCheck =
          replicas.stream()
              .filter(r -> !nodesDownNames.contains(r.getName()))
              .collect(Collectors.toList());

      for (Replica replica : replicasToCheck) {
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
    fail("timeout waiting to see all nodes active");
  }
  
  

  private List<CloudJettyRunner> getOtherAvailableJetties(CloudJettyRunner leader) {
    List<CloudJettyRunner> candidates = new ArrayList<>();
    candidates.addAll(shardToJetty.get("shard1"));

    if (leader != null) {
      candidates.remove(leader);
    }

    candidates.removeAll(nodesDown);

    return candidates;
  }

  
  
  protected void indexDoc(Object... fields) throws IOException,
      SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();

    addFields(doc, fields);
    addFields(doc, "rnd_s", RandomStringUtils.random(random().nextInt(100) + 100));

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
