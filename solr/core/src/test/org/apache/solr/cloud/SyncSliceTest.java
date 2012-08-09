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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Test sync phase that occurs when Leader goes down and a new Leader is
 * elected.
 */
@Slow
public class SyncSliceTest extends AbstractFullDistribZkTestBase {
  
  @BeforeClass
  public static void beforeSuperClass() {
    
  }
  
  @AfterClass
  public static void afterSuperClass() {
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");
    
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }
  
  public SyncSliceTest() {
    super();
    sliceCount = 1;
    shardCount = TEST_NIGHTLY ? 7 : 4;
  }
  
  @Override
  public void doTest() throws Exception {
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(15);

    del("*:*");
    List<String> skipServers = new ArrayList<String>();
    int docId = 0;
    indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
        "to come to the aid of their country.");
    
    indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
        "old haven was blue.");
    
    skipServers.add(shardToJetty.get("shard1").get(1).url + "/");
    
    indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
        "but the song was fancy.");
    
    skipServers.add(shardToJetty.get("shard1").get(2).url + "/");
    
    indexDoc(skipServers, id,docId++, i1, 50, tlong, 50, t1,
        "under the moon and over the lake");
    
    commit();
    
    waitForRecoveriesToFinish(false);

    // shard should be inconsistent
    String shardFailMessage = checkShardConsistency("shard1", true);
    assertNotNull(shardFailMessage);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.SYNCSHARD.toString());
    params.set("collection", "collection1");
    params.set("shard", "shard1");
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    String baseUrl = ((HttpSolrServer) shardToJetty.get("shard1").get(2).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());
    
    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.request(request);
    
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
    
    
    Set<CloudJettyRunner> jetties = new HashSet<CloudJettyRunner>();
    jetties.addAll(shardToJetty.get("shard1"));
    jetties.remove(leaderJetty);
    assertEquals(shardCount - 1, jetties.size());
    
    chaosMonkey.killJetty(leaderJetty);

    // we are careful to make sure the downed node is no longer in the state,
    // because on some systems (especially freebsd w/ blackhole enabled), trying
    // to talk to a downed node causes grief
    waitToSeeDownInClusterState(leaderJetty, jetties);

    waitForThingsToLevelOut(15);
    
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
    ChaosMonkey.start(deadJetty.jetty); // he is not the leader anymore
    
    // give a moment to be sure it has started recovering
    Thread.sleep(2000);
    
    waitForThingsToLevelOut(15);
    waitForRecoveriesToFinish(false);
    
    skipServers = getRandomOtherJetty(leaderJetty, null);
    skipServers.addAll( getRandomOtherJetty(leaderJetty, null));
    // skip list should be 
    
    //System.out.println("leader:" + leaderJetty.url);
    //System.out.println("skip list:" + skipServers);
    
    // we are skipping  one nodes
    assertEquals(2, skipServers.size());
    
    // more docs than can peer sync
    for (int i = 0; i < 300; i++) {
      indexDoc(skipServers, id, docId++, i1, 50, tlong, 50, t1,
          "to come to the aid of their country.");
    }
    
    commit();
    
    waitForRecoveriesToFinish(false);
    
    // shard should be inconsistent
    shardFailMessage = checkShardConsistency("shard1", true);
    assertNotNull(shardFailMessage);
    
    
    jetties = new HashSet<CloudJettyRunner>();
    jetties.addAll(shardToJetty.get("shard1"));
    jetties.remove(leaderJetty);
    assertEquals(shardCount - 1, jetties.size());

    
    // kill the current leader
    chaosMonkey.killJetty(leaderJetty);
    
    waitToSeeDownInClusterState(leaderJetty, jetties);
    
    Thread.sleep(4000);
    
    waitForRecoveriesToFinish(false);

    checkShardConsistency(true, true);
    
  }

  private List<String> getRandomJetty() {
    return getRandomOtherJetty(null, null);
  }
  
  private List<String> getRandomOtherJetty(CloudJettyRunner leader, CloudJettyRunner down) {
    List<String> skipServers = new ArrayList<String>();
    List<CloudJettyRunner> candidates = new ArrayList<CloudJettyRunner>();
    candidates.addAll(shardToJetty.get("shard1"));

    if (leader != null) {
      candidates.remove(leader);
    }
    
    if (down != null) {
      candidates.remove(down);
    }
    
    CloudJettyRunner cjetty = candidates.get(random().nextInt(candidates.size()));
    skipServers.add(cjetty.url + "/");
    return skipServers;
  }

  private void waitToSeeDownInClusterState(CloudJettyRunner leaderJetty,
      Set<CloudJettyRunner> jetties) throws InterruptedException {

    for (CloudJettyRunner cjetty : jetties) {
      waitToSeeNotLive(((SolrDispatchFilter) cjetty.jetty.getDispatchFilter()
          .getFilter()).getCores().getZkController().getZkStateReader(),
          leaderJetty);
    }
    waitToSeeNotLive(cloudClient.getZkStateReader(), leaderJetty);
  }
  
  protected void indexDoc(List<String> skipServers, Object... fields) throws IOException,
      SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    
    controlClient.add(doc);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (String skip : skipServers) {
      params.add("test.distrib.skip.servers", skip);
    }
    ureq.setParams(params);
    ureq.process(cloudClient);
  }
  
  // skip the randoms - they can deadlock...
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

}
