package org.apache.solr.cloud;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestRebalanceLeaders extends AbstractFullDistribZkTestBase {

  public static final String COLLECTION_NAME = "testcollection";

  public TestRebalanceLeaders() {
    schemaString = "schema15.xml";      // we need a string id
    sliceCount = 4;
  }

  int reps = 10;
  int timeoutMs = 60000;
  Map<String, List<Replica>> initial = new HashMap<>();

  Map<String, Replica> expected = new HashMap<>();

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    reps = random().nextInt(9) + 1; // make sure and do at least one.
    try (CloudSolrClient client = createCloudClient(null)) {
      // Mix up a bunch of different combinations of shards and replicas in order to exercise boundary cases.
      // shards, replicationfactor, maxreplicaspernode
      int shards = random().nextInt(7);
      if (shards < 2) shards = 2;
      int rFactor = random().nextInt(4);
      if (rFactor < 2) rFactor = 2;
      createCollection(null, COLLECTION_NAME, shards, rFactor, shards * rFactor + 1, client, null, "conf1");
    }

    waitForCollection(cloudClient.getZkStateReader(), COLLECTION_NAME, 2);
    waitForRecoveriesToFinish(COLLECTION_NAME, false);

    listCollection();

    rebalanceLeaderTest();
  }

  private void listCollection() throws IOException, SolrServerException {
    //CloudSolrServer client = createCloudClient(null);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.LIST.toString());
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = cloudClient.request(request);
      List<String> collections = (List<String>) rsp.get("collections");
      assertTrue("control_collection was not found in list", collections.contains("control_collection"));
      assertTrue(DEFAULT_COLLECTION + " was not found in list", collections.contains(DEFAULT_COLLECTION));
      assertTrue(COLLECTION_NAME + " was not found in list", collections.contains(COLLECTION_NAME));
    } finally {
      //remove collections
      //client.shutdown();
    }
  }

  void recordInitialState() throws InterruptedException {
    Map<String, Slice> slices = cloudClient.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getSlicesMap();

    // Assemble a list of all the replicas for all the shards in a convenient way to look at them.
    for (Map.Entry<String, Slice> ent : slices.entrySet()) {
      initial.put(ent.getKey(), new ArrayList<>(ent.getValue().getReplicas()));
    }
  }

  void rebalanceLeaderTest() throws InterruptedException, IOException, SolrServerException, KeeperException {
    recordInitialState();
    for (int idx = 0; idx < reps; ++idx) {
      issueCommands();
      checkConsistency();
    }
  }

  // After we've called the rebalance command, we want to insure that:
  // 1> all replicas appear once and only once in the respective leader election queue
  // 2> All the replicas we _think_ are leaders are in the 0th position in the leader election queue.
  // 3> The node that ZooKeeper thinks is the leader is the one we think should be the leader.
  void checkConsistency() throws InterruptedException, KeeperException {
    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS);

    while (! timeout.hasTimedOut()) {
      if (checkAppearOnce() &&
          checkElectionZero() &&
          checkZkLeadersAgree()) {
        return;
      }
      Thread.sleep(1000);
    }
    fail("Checking the rebalance leader command failed");
  }


  // Do all the nodes appear exactly once in the leader election queue and vice-versa?
  Boolean checkAppearOnce() throws KeeperException, InterruptedException {

    for (Map.Entry<String, List<Replica>> ent : initial.entrySet()) {
      List<String> leaderQueue = cloudClient.getZkStateReader().getZkClient().getChildren("/collections/" + COLLECTION_NAME +
          "/leader_elect/" + ent.getKey() + "/election", null, true);

      if (leaderQueue.size() != ent.getValue().size()) {
        return false;
      }
      // Check that each election node has a corresponding replica.
      for (String electionNode : leaderQueue) {
        if (checkReplicaName(LeaderElector.getNodeName(electionNode), ent.getValue())) {
          continue;
        }
        return false;
      }
      // Check that each replica has an election node.
      for (Replica rep : ent.getValue()) {
        if (checkElectionNode(rep.getName(), leaderQueue)) {
          continue;
        }
        return false;
      }
    }
    return true;
  }

  // Check that the given name is in the leader election queue
  Boolean checkElectionNode(String repName, List<String> leaderQueue) {
    for (String electionNode : leaderQueue) {
      if (repName.equals(LeaderElector.getNodeName(electionNode))) {
        return true;
      }
    }
    return false;
  }

  // Check that the name passed in corresponds to a replica.
  Boolean checkReplicaName(String toCheck, List<Replica> replicas) {
    for (Replica rep : replicas) {
      if (toCheck.equals(rep.getName())) {
        return true;
      }
    }
    return false;
  }

  // Get the shard leader election from ZK and sort it. The node may not actually be there, so retry
  List<String> getOverseerSort(String key) {
    List<String> ret = null;
    try {
      ret = OverseerCollectionProcessor.getSortedElectionNodes(cloudClient.getZkStateReader().getZkClient(),
          "/collections/" + COLLECTION_NAME + "/leader_elect/" + key + "/election");
      return ret;
    } catch (KeeperException e) {
      cloudClient.connect();
    } catch (InterruptedException e) {
      return null;
    }
    return null;
  }

  // Is every node we think is the leader in the zeroth position in the leader election queue?
  Boolean checkElectionZero() {
    for (Map.Entry<String, Replica> ent : expected.entrySet()) {

      List<String> leaderQueue = getOverseerSort(ent.getKey());
      if (leaderQueue == null) return false;

      String electName = LeaderElector.getNodeName(leaderQueue.get(0));
      String coreName = ent.getValue().getName();
      if (electName.equals(coreName) == false) {
        return false;
      }
    }
    return true;
  }

  // Do who we _think_ should be the leader agree with the leader nodes?
  Boolean checkZkLeadersAgree() throws KeeperException, InterruptedException {
    for (Map.Entry<String, Replica> ent : expected.entrySet()) {

      String path = "/collections/" + COLLECTION_NAME + "/leaders/" + ent.getKey();
      byte[] data = getZkData(cloudClient, path);
      if (data == null) return false;

      String repCore = null;
      String zkCore = null;

      if (data == null) {
        return false;
      } else {
        Map m = (Map) Utils.fromJSON(data);
        zkCore = (String) m.get("core");
        repCore = ent.getValue().getStr("core");
        if (zkCore.equals(repCore) == false) {
          return false;
        }
      }
    }
    return true;
  }

  byte[] getZkData(CloudSolrClient client, String path) {
    org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
    try {
      byte[] data = client.getZkStateReader().getZkClient().getData(path, null, stat, true);
      if (data != null) {
        return data;
      }
    } catch (KeeperException.NoNodeException e) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e1) {
        return null;
      }
    } catch (InterruptedException | KeeperException e) {
      return null;
    }
    return null;
  }

  // It's OK not to check the return here since the subsequent tests will fail.
  void issueCommands() throws IOException, SolrServerException, KeeperException, InterruptedException {

    // Find a replica to make the preferredLeader. NOTE: may be one that's _already_ leader!
    expected.clear();
    for (Map.Entry<String, List<Replica>> ent : initial.entrySet()) {
      List<Replica> replicas = ent.getValue();
      Replica rep = replicas.get(Math.abs(random().nextInt()) % replicas.size());
      expected.put(ent.getKey(), rep);
      issuePreferred(ent.getKey(), rep);
    }

    if (waitForAllPreferreds() == false) {
      fail("Waited for timeout for preferredLeader assignments to be made and they werent.");
    }
    //fillExpectedWithCurrent();
    // Now rebalance the leaders
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.REBALANCELEADERS.toString());

    // Insure we get error returns when omitting required parameters
    params.set("collection", COLLECTION_NAME);
    params.set("maxAtOnce", "10");
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cloudClient.request(request);
  }

  void issuePreferred(String slice, Replica rep) throws IOException, SolrServerException, InterruptedException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString());

    // Insure we get error returns when omitting required parameters

    params.set("collection", COLLECTION_NAME);
    params.set("shard", slice);
    params.set("replica", rep.getName());
    params.set("property", "preferredLeader");
    params.set("property.value", "true");

    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cloudClient.request(request);
  }

  boolean waitForAllPreferreds() throws KeeperException, InterruptedException {
    boolean goAgain = true;
    TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS);
    while (! timeout.hasTimedOut()) {
      goAgain = false;
      cloudClient.getZkStateReader().updateClusterState();
      Map<String, Slice> slices = cloudClient.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getSlicesMap();

      for (Map.Entry<String, Replica> ent : expected.entrySet()) {
        Replica me = slices.get(ent.getKey()).getReplica(ent.getValue().getName());
        if (me.getBool("property.preferredleader", false) == false) {
          goAgain = true;
          break;
        }
      }
      if (goAgain) {
        Thread.sleep(250);
      } else {
        return true;
      }
    }
    return false;
  }

}

