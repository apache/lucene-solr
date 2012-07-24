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
import org.apache.solr.common.cloud.ZkStateReader;
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
public class SyncSliceTest extends FullSolrCloudTest {
  
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
    shardCount = 3;
  }
  
  @Override
  public void doTest() throws Exception {
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut();

    // something wrong with this?
    //del("*:*");
    
    List<String> skipServers = new ArrayList<String>();
    
    indexDoc(skipServers, id, 0, i1, 50, tlong, 50, t1,
        "to come to the aid of their country.");
    
    indexDoc(skipServers, id, 1, i1, 50, tlong, 50, t1,
        "old haven was blue.");
    
    skipServers.add(shardToJetty.get("shard1").get(1).url + "/");
    
    indexDoc(skipServers, id, 2, i1, 50, tlong, 50, t1,
        "but the song was fancy.");
    
    skipServers.add(shardToJetty.get("shard1").get(2).url + "/");
    
    indexDoc(skipServers, id, 3, i1, 50, tlong, 50, t1,
        "under the moon and over the lake");
    
    commit();

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
    
    waitForThingsToLevelOut();
    
    checkShardConsistency(false, true);
    
    long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(4, cloudClientDocs);
    
    skipServers = new ArrayList<String>();
    
    skipServers.add(shardToJetty.get("shard1").get(random().nextInt(shardCount)).url + "/");
    
    // this doc won't be on one node
    indexDoc(skipServers, id, 4, i1, 50, tlong, 50, t1,
        "to come to the aid of their country.");
    
    // kill the leader - new leader could have all the docs or be missing one
    CloudJettyRunner leaderJetty = shardToLeaderJetty.get("shard1");

    Set<CloudJettyRunner> jetties = new HashSet<CloudJettyRunner>();
    for (int i = 0; i < shardCount; i++) {
      jetties.add(shardToJetty.get("shard1").get(i));
    }
    jetties.remove(leaderJetty);
    
    chaosMonkey.killJetty(leaderJetty);

    CloudJettyRunner upJetty = jetties.iterator().next();
    // we are careful to make sure the downed node is no longer in the state,
    // because on some systems (especially freebsd w/ blackhole enabled), trying
    // to talk to a downed node causes grief
    assertNotNull(upJetty.jetty.getDispatchFilter());
    assertNotNull(upJetty.jetty.getDispatchFilter());
    assertNotNull(upJetty.jetty.getDispatchFilter().getFilter());
    
    
    int tries = 0;
    while (((SolrDispatchFilter) upJetty.jetty.getDispatchFilter().getFilter())
        .getCores().getZkController().getZkStateReader().getCloudState()
        .liveNodesContain(leaderJetty.info.get(ZkStateReader.NODE_NAME_PROP))) {
      if (tries++ == 120) {
        fail("Shard still reported as live in zk");
      }
      Thread.sleep(1000);
    }
    
    waitForThingsToLevelOut();
    
    checkShardConsistency(false, true);
    
    cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(5, cloudClientDocs);
  }

  private void waitForThingsToLevelOut() throws Exception {
    int cnt = 0;
    boolean retry = false;
    do {
      waitForRecoveriesToFinish(false);
      
      commit();
      
      updateMappingsFromZk(jettys, clients);
      
      Set<String> theShards = shardToJetty.keySet();
      String failMessage = null;
      for (String shard : theShards) {
        failMessage = checkShardConsistency(shard, false);
      }
      
      if (failMessage != null) {
        retry = true;
      } else {
        retry = false;
      }
      
      cnt++;
      if (cnt > 10) break;
      Thread.sleep(2000);
    } while (retry);
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
