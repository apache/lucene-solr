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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;

public class ForceLeaderTest extends HttpPartitionTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean onlyLeaderIndexes = random().nextBoolean();

  @Override
  protected boolean useTlogReplicas() {
    return onlyLeaderIndexes;
  }

  @Test
  @Override
  @Ignore
  public void test() throws Exception {

  }

  /***
   * Tests that FORCELEADER can get an active leader after leader puts all replicas in LIR and itself goes down,
   * hence resulting in a leaderless shard.
   */
  @Test
  @Slow
  public void testReplicasInLIRNoLeader() throws Exception {
    handle.put("maxScore", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    String testCollectionName = "forceleader_test_collection";
    createCollection(testCollectionName, "conf1", 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    try {
      List<Replica> notLeaders = ensureAllReplicasAreActive(testCollectionName, SHARD1, 1, 3, maxWaitSecsToSeeAllActive);
      assertEquals("Expected 2 replicas for collection " + testCollectionName
          + " but found " + notLeaders.size() + "; clusterState: "
          + printClusterStateInfo(testCollectionName), 2, notLeaders.size());

      Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, SHARD1);
      JettySolrRunner notLeader0 = getJettyOnPort(getReplicaPort(notLeaders.get(0)));
      ZkController zkController = notLeader0.getCoreContainer().getZkController();

      putNonLeadersIntoLIR(testCollectionName, SHARD1, zkController, leader, notLeaders);

      cloudClient.getZkStateReader().forceUpdateCollection(testCollectionName);
      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      int numActiveReplicas = getNumberOfActiveReplicas(clusterState, testCollectionName, SHARD1);
      assertEquals("Expected only 0 active replica but found " + numActiveReplicas +
          "; clusterState: " + printClusterStateInfo(), 0, numActiveReplicas);

      int numReplicasOnLiveNodes = 0;
      for (Replica rep : clusterState.getCollection(testCollectionName).getSlice(SHARD1).getReplicas()) {
        if (clusterState.getLiveNodes().contains(rep.getNodeName())) {
          numReplicasOnLiveNodes++;
        }
      }
      assertEquals(2, numReplicasOnLiveNodes);
      log.info("Before forcing leader: " + printClusterStateInfo());
      // Assert there is no leader yet
      assertNull("Expected no leader right now. State: " + clusterState.getCollection(testCollectionName).getSlice(SHARD1),
          clusterState.getCollection(testCollectionName).getSlice(SHARD1).getLeader());

      assertSendDocFails(3);

      doForceLeader(cloudClient, testCollectionName, SHARD1);

      // By now we have an active leader. Wait for recoveries to begin
      waitForRecoveriesToFinish(testCollectionName, cloudClient.getZkStateReader(), true);

      cloudClient.getZkStateReader().forceUpdateCollection(testCollectionName);
      clusterState = cloudClient.getZkStateReader().getClusterState();
      log.info("After forcing leader: " + clusterState.getCollection(testCollectionName).getSlice(SHARD1));
      // we have a leader
      Replica newLeader = clusterState.getCollectionOrNull(testCollectionName).getSlice(SHARD1).getLeader();
      assertNotNull(newLeader);
      // leader is active
      assertEquals(State.ACTIVE, newLeader.getState());

      numActiveReplicas = getNumberOfActiveReplicas(clusterState, testCollectionName, SHARD1);
      assertEquals(2, numActiveReplicas);

      // Assert that indexing works again
      log.info("Sending doc 4...");
      sendDoc(4);
      log.info("Committing...");
      cloudClient.commit();
      log.info("Doc 4 sent and commit issued");

      assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 1);
      assertDocsExistInAllReplicas(notLeaders, testCollectionName, 4, 4);

      // Docs 1 and 4 should be here. 2 was lost during the partition, 3 had failed to be indexed.
      log.info("Checking doc counts...");
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "*:*");
      assertEquals("Expected only 2 documents in the index", 2, cloudClient.query(params).getResults().getNumFound());

      bringBackOldLeaderAndSendDoc(testCollectionName, leader, notLeaders, 5);
    } finally {
      log.info("Cleaning up after the test.");
      // try to clean up
      attemptCollectionDelete(cloudClient, testCollectionName);
    }
  }

  /**
   * Test that FORCELEADER can set last published state of all down (live) replicas to active (so
   * that they become worthy candidates for leader election).
   */
  @Slow
  public void testLastPublishedStateIsActive() throws Exception {
    handle.put("maxScore", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    String testCollectionName = "forceleader_last_published";
    createCollection(testCollectionName, "conf1", 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);
    log.info("Collection created: " + testCollectionName);

    try {
      List<Replica> notLeaders = ensureAllReplicasAreActive(testCollectionName, SHARD1, 1, 3, maxWaitSecsToSeeAllActive);
      assertEquals("Expected 2 replicas for collection " + testCollectionName
          + " but found " + notLeaders.size() + "; clusterState: "
          + printClusterStateInfo(testCollectionName), 2, notLeaders.size());

      Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, SHARD1);
      JettySolrRunner notLeader0 = getJettyOnPort(getReplicaPort(notLeaders.get(0)));
      ZkController zkController = notLeader0.getCoreContainer().getZkController();

      // Mark all replicas down
      setReplicaState(testCollectionName, SHARD1, leader, State.DOWN);
      for (Replica rep : notLeaders) {
        setReplicaState(testCollectionName, SHARD1, rep, State.DOWN);
      }

      zkController.getZkStateReader().forceUpdateCollection(testCollectionName);
      // Assert all replicas are down and that there is no leader
      assertEquals(0, getActiveOrRecoveringReplicas(testCollectionName, SHARD1).size());

      // Now force leader
      doForceLeader(cloudClient, testCollectionName, SHARD1);

      // Assert that last published states of the two replicas are active now
      for (Replica rep: notLeaders) {
        assertEquals(Replica.State.ACTIVE, getLastPublishedState(testCollectionName, SHARD1, rep));
      }
    } finally {
      log.info("Cleaning up after the test.");
      attemptCollectionDelete(cloudClient, testCollectionName);
    }
  }

  protected void unsetLeader(String collection, String slice) throws Exception {
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(cloudClient.getZkStateReader().getZkClient());
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();

    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
        ZkStateReader.SHARD_ID_PROP, slice,
        ZkStateReader.COLLECTION_PROP, collection);
    inQueue.offer(Utils.toJSON(m));

    ClusterState clusterState = null;
    boolean transition = false;
    for (int counter = 10; counter > 0; counter--) {
      clusterState = zkStateReader.getClusterState();
      Replica newLeader = clusterState.getCollection(collection).getSlice(slice).getLeader();
      if (newLeader == null) {
        transition = true;
        break;
      }
      Thread.sleep(1000);
    }

    if (!transition) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not unset replica leader" +
          ". Cluster state: " + printClusterStateInfo(collection));
    }
  }

  protected void setReplicaState(String collection, String slice, Replica replica, Replica.State state) throws SolrServerException, IOException,
      KeeperException, InterruptedException {
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(cloudClient.getZkStateReader().getZkClient());
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();

    String baseUrl = zkStateReader.getBaseUrlForNodeName(replica.getNodeName());
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
        ZkStateReader.BASE_URL_PROP, baseUrl,
        ZkStateReader.NODE_NAME_PROP, replica.getNodeName(),
        ZkStateReader.SHARD_ID_PROP, slice,
        ZkStateReader.COLLECTION_PROP, collection,
        ZkStateReader.CORE_NAME_PROP, replica.getStr(CORE_NAME_PROP),
        ZkStateReader.CORE_NODE_NAME_PROP, replica.getName(),
        ZkStateReader.STATE_PROP, state.toString());
    inQueue.offer(Utils.toJSON(m));
    boolean transition = false;

    Replica.State replicaState = null;
    for (int counter = 10; counter > 0; counter--) {
      ClusterState clusterState = zkStateReader.getClusterState();
      replicaState = clusterState.getCollection(collection).getSlice(slice).getReplica(replica.getName()).getState();
      if (replicaState == state) {
        transition = true;
        break;
      }
      Thread.sleep(1000);
    }

    if (!transition) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not set replica [" + replica.getName() + "] as " + state +
          ". Last known state of the replica: " + replicaState);
    }
  }
  
  /*protected void setLastPublishedState(String collection, String slice, Replica replica, Replica.State state) throws SolrServerException, IOException,
  KeeperException, InterruptedException {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    String baseUrl = zkStateReader.getBaseUrlForNodeName(replica.getNodeName());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.FORCEPREPAREFORLEADERSHIP.toString());
    params.set(CoreAdminParams.CORE, replica.getStr("core"));
    params.set(ZkStateReader.STATE_PROP, state.toString());

    SolrRequest<SimpleSolrResponse> req = new GenericSolrRequest(METHOD.GET, "/admin/cores", params);
    NamedList resp = null;
    try (HttpSolrClient hsc = new HttpSolrClient(baseUrl)) {
       resp = hsc.request(req);
    }
  }*/

  protected Replica.State getLastPublishedState(String collection, String slice, Replica replica) throws SolrServerException, IOException,
  KeeperException, InterruptedException {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    String baseUrl = zkStateReader.getBaseUrlForNodeName(replica.getNodeName());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.STATUS.toString());
    params.set(CoreAdminParams.CORE, replica.getStr("core"));

    SolrRequest<SimpleSolrResponse> req = new GenericSolrRequest(METHOD.GET, "/admin/cores", params);
    NamedList resp = null;
    try (HttpSolrClient hsc = getHttpSolrClient(baseUrl)) {
       resp = hsc.request(req);
    }

    String lastPublished = (((NamedList<NamedList<String>>)resp.get("status")).get(replica.getStr("core"))).get("lastPublished");
    return Replica.State.getState(lastPublished);
  }

  void assertSendDocFails(int docId) throws Exception {
    // sending a doc in this state fails
    try {
      sendDoc(docId);
      log.error("Should've failed indexing during a down state. Cluster state: " + printClusterStateInfo());
      fail("Should've failed indexing during a down state.");
    } catch (SolrException ex) {
      log.info("Document couldn't be sent, which is expected.");
    }
  }

  void putNonLeadersIntoLIR(String collectionName, String shard, ZkController zkController, Replica leader, List<Replica> notLeaders) throws Exception {
    SocketProxy[] nonLeaderProxies = new SocketProxy[notLeaders.size()];
    for (int i = 0; i < notLeaders.size(); i++)
      nonLeaderProxies[i] = getProxyForReplica(notLeaders.get(i));

    sendDoc(1);

    // ok, now introduce a network partition between the leader and both replicas
    log.info("Closing proxies for the non-leader replicas...");
    for (SocketProxy proxy : nonLeaderProxies)
      proxy.close();

    // indexing during a partition
    log.info("Sending a doc during the network partition...");
    sendDoc(2);

    // Wait a little
    Thread.sleep(2000);

    // Kill the leader
    log.info("Killing leader for shard1 of " + collectionName + " on node " + leader.getNodeName() + "");
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));
    getProxyForReplica(leader).close();
    leaderJetty.stop();

    // Wait for a steady state, till LIR flags have been set and the shard is leaderless
    log.info("Sleep and periodically wake up to check for state...");
    for (int i = 0; i < 20; i++) {
      Thread.sleep(1000);
      State lirStates[] = new State[notLeaders.size()];
      for (int j = 0; j < notLeaders.size(); j++)
        lirStates[j] = zkController.getLeaderInitiatedRecoveryState(collectionName, shard, notLeaders.get(j).getName());

      ClusterState clusterState = zkController.getZkStateReader().getClusterState();
      boolean allDown = true;
      for (State lirState : lirStates)
        if (Replica.State.DOWN.equals(lirState) == false)
          allDown = false;
      if (allDown && clusterState.getCollection(collectionName).getSlice(shard).getLeader() == null) {
        break;
      }
      log.warn("Attempt " + i + ", waiting on for 1 sec to settle down in the steady state. State: " +
          printClusterStateInfo(collectionName));
      log.warn("LIR state: " + getLIRState(zkController, collectionName, shard));
    }
    log.info("Waking up...");

    // remove the network partition
    log.info("Reopening the proxies for the non-leader replicas...");
    for (SocketProxy proxy : nonLeaderProxies)
      proxy.reopen();

    log.info("LIR state: " + getLIRState(zkController, collectionName, shard));

    State lirStates[] = new State[notLeaders.size()];
    for (int j = 0; j < notLeaders.size(); j++)
      lirStates[j] = zkController.getLeaderInitiatedRecoveryState(collectionName, shard, notLeaders.get(j).getName());
    for (State lirState : lirStates)
      assertTrue("Expected that the replicas would be in LIR state by now. LIR states: "+Arrays.toString(lirStates),
          Replica.State.DOWN == lirState || Replica.State.RECOVERING == lirState);
  }

  protected void bringBackOldLeaderAndSendDoc(String collection, Replica leader, List<Replica> notLeaders, int docid) throws Exception {
    // Bring back the leader which was stopped
    log.info("Bringing back originally killed leader...");
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));
    leaderJetty.start();
    waitForRecoveriesToFinish(collection, cloudClient.getZkStateReader(), true);
    cloudClient.getZkStateReader().forceUpdateCollection(collection);
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    log.info("After bringing back leader: " + clusterState.getCollection(collection).getSlice(SHARD1));
    int numActiveReplicas = getNumberOfActiveReplicas(clusterState, collection, SHARD1);
    assertEquals(1+notLeaders.size(), numActiveReplicas);
    log.info("Sending doc "+docid+"...");
    sendDoc(docid);
    log.info("Committing...");
    cloudClient.commit();
    log.info("Doc "+docid+" sent and commit issued");
    assertDocsExistInAllReplicas(notLeaders, collection, docid, docid);
    assertDocsExistInAllReplicas(Collections.singletonList(leader), collection, docid, docid);
  }

  protected String getLIRState(ZkController zkController, String collection, String shard) throws KeeperException, InterruptedException {
    StringBuilder sb = new StringBuilder();
    String path = zkController.getLeaderInitiatedRecoveryZnodePath(collection, shard);
    if (path == null)
      return null;
    try {
      zkController.getZkClient().printLayout(path, 4, sb);
    } catch (NoNodeException ex) {
      return null;
    }
    return sb.toString();
  }

  @Override
  protected int sendDoc(int docId) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);

    return sendDocsWithRetry(Collections.singletonList(doc), 1, 5, 1);
  }

  private void doForceLeader(SolrClient client, String collectionName, String shard) throws IOException, SolrServerException {
    CollectionAdminRequest.ForceLeader forceLeader = CollectionAdminRequest.forceLeaderElection(collectionName, shard);
    client.request(forceLeader);
  }

  protected int getNumberOfActiveReplicas(ClusterState clusterState, String collection, String sliceId) {
    int numActiveReplicas = 0;
    // Assert all replicas are active
    for (Replica rep : clusterState.getCollection(collection).getSlice(sliceId).getReplicas()) {
      if (rep.getState().equals(State.ACTIVE)) {
        numActiveReplicas++;
      }
    }
    return numActiveReplicas;
  }
}

