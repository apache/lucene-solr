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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test sync phase that occurs when Leader goes down and a new Leader is
 * elected.
 */
@Slow
public class SyncSliceTest extends AbstractFullDistribZkTestBase {
  private boolean success = false;

  @Override
  public void distribTearDown() throws Exception {
    if (!success) {
      printLayoutOnTearDown = true;
    }
    super.distribTearDown();
  }

  public SyncSliceTest() {
    super();
    sliceCount = 1;
    fixShardCount(TEST_NIGHTLY ? 7 : 4);
  }

  @Test
  public void test() throws Exception {
    
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(30);

    del("*:*");
    List<CloudJettyRunner> skipServers = new ArrayList<>();
    int docId = 0;
    indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
        "to come to the aid of their country.");
    
    indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
        "old haven was blue.");
    
    skipServers.add(shardToJetty.get("shard1").get(1));
    
    indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
        "but the song was fancy.");
    
    skipServers.add(shardToJetty.get("shard1").get(2));
    
    indexDoc(skipServers, id,docId++, i1, 50, tlong, 50, t1,
        "under the moon and over the lake");
    
    commit();
    
    waitForRecoveriesToFinish(false);

    // shard should be inconsistent
    String shardFailMessage = checkShardConsistency("shard1", true, false);
    assertNotNull(shardFailMessage);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.SYNCSHARD.toString());
    params.set("collection", "collection1");
    params.set("shard", "shard1");
    @SuppressWarnings({"rawtypes"})
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    String baseUrl = ((HttpSolrClient) shardToJetty.get("shard1").get(2).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());
    
    // we only set the connect timeout, not so timeout
    try (HttpSolrClient baseClient = getHttpSolrClient(baseUrl, 30000)) {
      baseClient.request(request);
    }

    waitForThingsToLevelOut(15);
    
    checkShardConsistency(false, true);
    
    long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(4, cloudClientDocs);
    
    
    // kill the leader - new leader could have all the docs or be missing one
    CloudJettyRunner leaderJetty = shardToLeaderJetty.get("shard1");
    
    skipServers = getRandomOtherJetty(leaderJetty, null); // but not the leader
    
    // this doc won't be on one node
    indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
        "to come to the aid of their country.");
    commit();
    
    
    Set<CloudJettyRunner> jetties = new HashSet<>();
    jetties.addAll(shardToJetty.get("shard1"));
    jetties.remove(leaderJetty);
    assertEquals(getShardCount() - 1, jetties.size());
    
    leaderJetty.jetty.stop();
    
    Thread.sleep(3000);
    
    waitForNoShardInconsistency();
    
    Thread.sleep(1000);
    
    checkShardConsistency(false, true);
    
    cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(5, cloudClientDocs);
    
    CloudJettyRunner deadJetty = leaderJetty;
    
    // let's get the latest leader
    while (deadJetty == leaderJetty) {
      updateMappingsFromZk(this.jettys, this.clients);
      leaderJetty = shardToLeaderJetty.get("shard1");
    }
    
    // bring back dead node
    deadJetty.jetty.start(); // he is not the leader anymore
    
    waitTillAllNodesActive();
    
    skipServers = getRandomOtherJetty(leaderJetty, deadJetty);
    skipServers.addAll( getRandomOtherJetty(leaderJetty, deadJetty));
    // skip list should be 
    
//    System.out.println("leader:" + leaderJetty.url);
//    System.out.println("dead:" + deadJetty.url);
//    System.out.println("skip list:" + skipServers);
    
    // we are skipping  2 nodes
    assertEquals(2, skipServers.size());
    
    // more docs than can peer sync
    for (int i = 0; i < 300; i++) {
      indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
          "to come to the aid of their country.");
    }
    
    commit();
    
    Thread.sleep(1000);
    
    waitForRecoveriesToFinish(false);
    
    // shard should be inconsistent
    shardFailMessage = waitTillInconsistent();
    assertNotNull(
        "Test Setup Failure: shard1 should have just been set up to be inconsistent - but it's still consistent. Leader:"
            + leaderJetty.url + " Dead Guy:" + deadJetty.url + "skip list:" + skipServers, shardFailMessage);
    
    // good place to test compareResults
    boolean shouldFail = CloudInspectUtil.compareResults(controlClient, cloudClient);
    assertTrue("A test that compareResults is working correctly failed", shouldFail);
    
    jetties = new HashSet<>();
    jetties.addAll(shardToJetty.get("shard1"));
    jetties.remove(leaderJetty);
    assertEquals(getShardCount() - 1, jetties.size());

    
    // kill the current leader
    leaderJetty.jetty.stop();
    
    waitForNoShardInconsistency();

    checkShardConsistency(true, true);
    
    success = true;
  }

  private void waitTillAllNodesActive() throws Exception {
    for (int i = 0; i < 60; i++) { 
      Thread.sleep(3000);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection collection1 = clusterState.getCollection("collection1");
      Slice slice = collection1.getSlice("shard1");
      Collection<Replica> replicas = slice.getReplicas();
      boolean allActive = true;
      for (Replica replica : replicas) {
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

  private String waitTillInconsistent() throws Exception, InterruptedException {
    String shardFailMessage = null;
    
    shardFailMessage = pollConsistency(shardFailMessage, 0);
    shardFailMessage = pollConsistency(shardFailMessage, 3000);
    shardFailMessage = pollConsistency(shardFailMessage, 5000);
    shardFailMessage = pollConsistency(shardFailMessage, 15000);
    
    return shardFailMessage;
  }

  private String pollConsistency(String shardFailMessage, int sleep)
      throws InterruptedException, Exception {
    try {
      commit();
    } catch (Throwable t) {
      t.printStackTrace();
    }
    if (shardFailMessage == null) {
      // try again
      Thread.sleep(sleep);
      shardFailMessage = checkShardConsistency("shard1", true, false);
    }
    return shardFailMessage;
  }
  
  private List<CloudJettyRunner> getRandomOtherJetty(CloudJettyRunner leader, CloudJettyRunner down) {
    List<CloudJettyRunner> skipServers = new ArrayList<>();
    List<CloudJettyRunner> candidates = new ArrayList<>();
    candidates.addAll(shardToJetty.get("shard1"));

    if (leader != null) {
      candidates.remove(leader);
    }
    
    if (down != null) {
      candidates.remove(down);
    }
    
    CloudJettyRunner cjetty = candidates.get(random().nextInt(candidates.size()));
    skipServers.add(cjetty);
    return skipServers;
  }
  
  protected void indexDoc(List<CloudJettyRunner> skipServers, Object... fields) throws IOException,
      SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    
    controlClient.add(doc);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (CloudJettyRunner skip : skipServers) {
      params.add("test.distrib.skip.servers", skip.url + "/");
    }
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
