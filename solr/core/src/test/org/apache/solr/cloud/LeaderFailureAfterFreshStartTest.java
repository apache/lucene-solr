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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
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
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * 
 * Test for SOLR-9446
 *
 * This test is modeled after SyncSliceTest
 */
@Slow
public class LeaderFailureAfterFreshStartTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean success = false;
  int docId = 0;

  List<CloudJettyRunner> nodesDown = new ArrayList<>();

  @Override
  public void distribTearDown() throws Exception {
    if (!success) {
      printLayoutOnTearDown = true;
    }
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
    System.clearProperty("tests.zk.violationReportAction");
    super.distribTearDown();
  }

  public LeaderFailureAfterFreshStartTest() {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }

  protected String getCloudSolrConfig() {
    return "solrconfig-tlog.xml";
  }

  @Override
  public void distribSetUp() throws Exception {
    // tlog gets deleted after node restarts if we use CachingDirectoryFactory.
    // make sure that tlog stays intact after we restart a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    System.setProperty("tests.zk.violationReportAction", LimitViolationAction.IGNORE.toString());
    super.distribSetUp();
  }

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    try {
      CloudJettyRunner initialLeaderJetty = shardToLeaderJetty.get("shard1");
      List<CloudJettyRunner> otherJetties = getOtherAvailableJetties(initialLeaderJetty);
      
      log.info("Leader node_name: {},  url: {}", initialLeaderJetty.coreNodeName, initialLeaderJetty.url);
      for (CloudJettyRunner cloudJettyRunner : otherJetties) {
        log.info("Nonleader node_name: {},  url: {}", cloudJettyRunner.coreNodeName, cloudJettyRunner.url);
      }
      
      CloudJettyRunner secondNode = otherJetties.get(0);
      CloudJettyRunner freshNode = otherJetties.get(1);
      
      // shutdown a node to simulate fresh start
      otherJetties.remove(freshNode);
      forceNodeFailures(singletonList(freshNode));

      del("*:*");
      waitForThingsToLevelOut(30);

      checkShardConsistency(false, true);

      // index a few docs and commit
      for (int i = 0; i < 100; i++) {
        indexDoc(id, docId, i1, 50, tlong, 50, t1,
            "document number " + docId++);
      }
      commit();
      waitForThingsToLevelOut(30);

      checkShardConsistency(false, true);
      
      // bring down the other node and index a few docs; so the leader and other node segments diverge
      forceNodeFailures(singletonList(secondNode));
      for (int i = 0; i < 10; i++) {
        indexDoc(id, docId, i1, 50, tlong, 50, t1,
            "document number " + docId++);
        if(i % 2 == 0) {
          commit();
        }
      }
      commit();
      restartNodes(singletonList(secondNode));

      // start the freshNode 
      restartNodes(singletonList(freshNode));
      String coreName = freshNode.jetty.getCoreContainer().getCores().iterator().next().getName();
      String replicationProperties = freshNode.jetty.getSolrHome() + "/cores/" +  coreName + "/data/replication.properties";
      String md5 = DigestUtils.md5Hex(Files.readAllBytes(Paths.get(replicationProperties)));
        
      // shutdown the original leader
      log.info("Now shutting down initial leader");
      forceNodeFailures(singletonList(initialLeaderJetty));
      waitForNewLeader(cloudClient, "shard1", (Replica)initialLeaderJetty.client.info  , new TimeOut(15, SECONDS, TimeSource.NANO_TIME));
      waitTillNodesActive();
      log.info("Updating mappings from zk");
      updateMappingsFromZk(jettys, clients, true);
      assertEquals("Node went into replication", md5, DigestUtils.md5Hex(Files.readAllBytes(Paths.get(replicationProperties))));
      
      success = true;
    } finally {
      System.clearProperty("solr.disableFingerprint");
    }
  }
  
  private void restartNodes(List<CloudJettyRunner> nodesToRestart) throws Exception {
    for (CloudJettyRunner node : nodesToRestart) {
      node.jetty.start();
      nodesDown.remove(node);
    }
    waitTillNodesActive();
    checkShardConsistency(false, true);
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

  

  private void waitTillNodesActive() throws Exception {
    for (int i = 0; i < 60; i++) {
      Thread.sleep(3000);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection1 = clusterState.getCollection("collection1");
      Slice slice = collection1.getSlice("shard1");
      Collection<Replica> replicas = slice.getReplicas();
      boolean allActive = true;

      Collection<String> nodesDownNames = nodesDown.stream()
          .map(n -> n.coreNodeName)
          .collect(Collectors.toList());
      
      Collection<Replica> replicasToCheck = null;
      replicasToCheck = replicas.stream()
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
