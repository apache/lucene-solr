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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Nightly
@LuceneTestCase.Slow
@Deprecated
public class LIROnShardRestartTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
  }

  public void testAllReplicasInLIR() throws Exception {
    String collection = "allReplicasInLIR";
    CollectionAdminRequest.createCollection(collection, 1, 3)
        .process(cluster.getSolrClient());
    cluster.getSolrClient().add(collection, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().add(collection, new SolrInputDocument("id", "2"));
    cluster.getSolrClient().commit(collection);

    DocCollection docCollection = getCollectionState(collection);
    Slice shard1 = docCollection.getSlice("shard1");
    Replica newLeader = shard1.getReplicas(rep -> !rep.getName().equals(shard1.getLeader().getName())).get(random().nextInt(2));
    JettySolrRunner jettyOfNewLeader = cluster.getJettySolrRunners().stream()
        .filter(jetty -> jetty.getNodeName().equals(newLeader.getNodeName()))
        .findAny().get();
    assertNotNull(jettyOfNewLeader);

    // randomly add too many docs to peer sync to one replica so that only one random replica is the valid leader
    // the versions don't matter, they just have to be higher than what the last 2 docs got
    try (HttpSolrClient client = getHttpSolrClient(jettyOfNewLeader.getBaseUrl().toString())) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM, DistributedUpdateProcessor.DistribPhase.FROMLEADER.toString());

      for (int i = 0; i < 101; i++) {
        UpdateRequest ureq = new UpdateRequest();
        ureq.setParams(new ModifiableSolrParams(params));
        ureq.add(sdoc("id", 3 + i, "_version_", Long.MAX_VALUE - 1 - i));
        ureq.process(client, collection);
      }
      client.commit(collection);
    }

    ChaosMonkey.stop(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all not live",
        ClusterStateUtil.waitForAllReplicasNotLive(cluster.getSolrClient().getZkStateReader(), 45000));

    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "shard1", cluster.getZkClient())) {
      for (Replica replica : docCollection.getReplicas()) {
        zkShardTerms.removeTerm(replica.getName());
      }
    }

    Map<String,Object> stateObj = Utils.makeMap();
    stateObj.put(ZkStateReader.STATE_PROP, "down");
    stateObj.put("createdByNodeName", "test");
    stateObj.put("createdByCoreNodeName", "test");
    byte[] znodeData = Utils.toJSON(stateObj);

    for (Replica replica : docCollection.getReplicas()) {
      try {
        cluster.getZkClient().makePath("/collections/" + collection + "/leader_initiated_recovery/shard1/" + replica.getName(),
            znodeData, true);
      } catch (KeeperException.NodeExistsException e) {

      }
    }

    ChaosMonkey.start(cluster.getJettySolrRunners());
    waitForState("Timeout waiting for active replicas", collection, clusterShape(1, 3));

    assertEquals(103, cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());


    // now expire each node
    for (Replica replica : docCollection.getReplicas()) {
      try {
        // todo remove the condition for skipping leader after SOLR-12166 is fixed
        if (newLeader.getName().equals(replica.getName())) continue;

        cluster.getZkClient().makePath("/collections/" + collection + "/leader_initiated_recovery/shard1/" + replica.getName(),
            znodeData, true);
      } catch (KeeperException.NodeExistsException e) {

      }
    }

    // only 2 replicas join the election and all of them are in LIR state, no one should win the election
    List<String> oldElectionNodes = getElectionNodes(collection, "shard1", cluster.getZkClient());

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      expire(jetty);
    }

    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.CURRENT_TIME);
    while (!timeOut.hasTimedOut()) {
      List<String> electionNodes = getElectionNodes(collection, "shard1", cluster.getZkClient());
      electionNodes.retainAll(oldElectionNodes);
      if (electionNodes.isEmpty()) break;
    }
    assertFalse("Timeout waiting for replicas rejoin election", timeOut.hasTimedOut());
    try {
      waitForState("Timeout waiting for active replicas", collection, clusterShape(1, 3));
    } catch (Throwable th) {
      String electionPath = "/collections/allReplicasInLIR/leader_elect/shard1/election/";
      List<String> children = zkClient().getChildren(electionPath, null, true);
      log.info("Election queue {}", children);
      throw th;
    }

    assertEquals(103, cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());

    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  public void expire(JettySolrRunner jetty) {
    CoreContainer cores = jetty.getCoreContainer();
    ChaosMonkey.causeConnectionLoss(jetty);
    long sessionId = cores.getZkController().getZkClient()
        .getSolrZooKeeper().getSessionId();
    cluster.getZkServer().expire(sessionId);
  }


  public void testSeveralReplicasInLIR() throws Exception {
    String collection = "severalReplicasInLIR";
    CollectionAdminRequest.createCollection(collection, 1, 3)
        .process(cluster.getSolrClient());
    cluster.getSolrClient().add(collection, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().add(collection, new SolrInputDocument("id", "2"));
    cluster.getSolrClient().commit(collection);

    DocCollection docCollection = getCollectionState(collection);
    Map<JettySolrRunner, String> nodeNameToJetty = cluster.getJettySolrRunners().stream()
        .collect(Collectors.toMap(jetty -> jetty, JettySolrRunner::getNodeName));
    ChaosMonkey.stop(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all not live",
        ClusterStateUtil.waitForAllReplicasNotLive(cluster.getSolrClient().getZkStateReader(), 45000));

    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "shard1", cluster.getZkClient())) {
      for (Replica replica : docCollection.getReplicas()) {
        zkShardTerms.removeTerm(replica.getName());
      }
    }

    Map<String,Object> stateObj = Utils.makeMap();
    stateObj.put(ZkStateReader.STATE_PROP, "down");
    stateObj.put("createdByNodeName", "test");
    stateObj.put("createdByCoreNodeName", "test");
    byte[] znodeData = Utils.toJSON(stateObj);

    Replica replicaNotInLIR = docCollection.getReplicas().get(random().nextInt(3));
    for (Replica replica : docCollection.getReplicas()) {
      if (replica.getName().equals(replicaNotInLIR.getName())) continue;
      try {
        cluster.getZkClient().makePath("/collections/" + collection + "/leader_initiated_recovery/shard1/" + replica.getName(),
            znodeData, true);
      } catch (KeeperException.NodeExistsException e) {

      }
    }

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (nodeNameToJetty.get(jetty).equals(replicaNotInLIR.getNodeName())) continue;
      jetty.start();
    }
    waitForState("Timeout waiting for no leader", collection, (liveNodes, collectionState) -> {
      Replica leader = collectionState.getSlice("shard1").getLeader();
      return leader == null;
    });

    // only 2 replicas join the election and all of them are in LIR state, no one should win the election
    List<String> oldElectionNodes = getElectionNodes(collection, "shard1", cluster.getZkClient());
    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.CURRENT_TIME);
    while (!timeOut.hasTimedOut()) {
      List<String> electionNodes = getElectionNodes(collection, "shard1", cluster.getZkClient());
      electionNodes.retainAll(oldElectionNodes);
      if (electionNodes.isEmpty()) break;
    }
    assertFalse("Timeout waiting for replicas rejoin election", timeOut.hasTimedOut());

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (nodeNameToJetty.get(jetty).equals(replicaNotInLIR.getNodeName())) {
        jetty.start();
      }
    }
    waitForState("Timeout waiting for new leader", collection, (liveNodes, collectionState) -> {
      Replica leader = collectionState.getSlice("shard1").getLeader();
      return leader != null;
    });
    waitForState("Timeout waiting for new leader", collection, clusterShape(1, 3));

    assertEquals(2L, cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  private List<String> getElectionNodes(String collection, String shard, SolrZkClient client) throws KeeperException, InterruptedException {
    return client.getChildren("/collections/"+collection+"/leader_elect/"+shard+LeaderElector.ELECTION_NODE, null, true);
  }
}
