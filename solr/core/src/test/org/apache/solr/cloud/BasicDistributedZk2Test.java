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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
public class BasicDistributedZk2Test extends AbstractFullDistribZkTestBase {
  private static final String SHARD2 = "shard2";
  private static final String SHARD1 = "shard1";
  private static final String ONE_NODE_COLLECTION = "onenodecollection";

  public BasicDistributedZk2Test() {
    super();
    fixShardCount = true;
    
    sliceCount = 2;
    shardCount = 4;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.BaseDistributedSearchTestCase#doTest()
   * 
   * Create 3 shards, each with one replica
   */
  @Override
  public void doTest() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("QTime", SKIPVAL);
      handle.put("timestamp", SKIPVAL);
      
      testNodeWithoutCollectionForwarding();
     
      indexr(id, 1, i1, 100, tlong, 100, t1,
          "now is the time for all good men", "foo_f", 1.414f, "foo_b", "true",
          "foo_d", 1.414d);
      
      commit();
      
      // make sure we are in a steady state...
      waitForRecoveriesToFinish(false);

      assertDocCounts(false);
      
      indexAbunchOfDocs();
      
      // check again 
      waitForRecoveriesToFinish(false);
      
      commit();
      
      assertDocCounts(VERBOSE);
      checkQueries();
      
      assertDocCounts(VERBOSE);
      
      query("q", "*:*", "sort", "n_tl1 desc");
      
      brindDownShardIndexSomeDocsAndRecover();
      
      query("q", "*:*", "sort", "n_tl1 desc");
      
      // test adding another replica to a shard - it should do a
      // recovery/replication to pick up the index from the leader
      addNewReplica();
      
      long docId = testUpdateAndDelete();
      
      // index a bad doc...
      try {
        indexr(t1, "a doc with no id");
        fail("this should fail");
      } catch (SolrException e) {
        // expected
      }
      
      // TODO: bring this to it's own method?
      // try indexing to a leader that has no replicas up
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      ZkNodeProps leaderProps = zkStateReader.getLeaderRetry(
          DEFAULT_COLLECTION, SHARD2);
      
      String nodeName = leaderProps.getStr(ZkStateReader.NODE_NAME_PROP);
      chaosMonkey.stopShardExcept(SHARD2, nodeName);
      
      SolrServer client = getClient(nodeName);
      
      index_specific(client, "id", docId + 1, t1, "what happens here?");
      
      // expire a session...
      CloudJettyRunner cloudJetty = shardToJetty.get(SHARD1).get(0);
      chaosMonkey.expireSession(cloudJetty.jetty);
      
      indexr("id", docId + 1, t1, "slip this doc in");
      
      waitForRecoveriesToFinish(false);
      
      checkShardConsistency(SHARD1);
      checkShardConsistency(SHARD2);
      
      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
    
  }
  
  private void testNodeWithoutCollectionForwarding() throws Exception,
      SolrServerException, IOException {
    try {
      final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(0));
      HttpSolrServer server = new HttpSolrServer(baseUrl);
      server.setConnectionTimeout(30000);
      Create createCmd = new Create();
      createCmd.setRoles("none");
      createCmd.setCoreName(ONE_NODE_COLLECTION + "core");
      createCmd.setCollection(ONE_NODE_COLLECTION);
      createCmd.setNumShards(1);
      createCmd.setDataDir(getDataDir(dataDir.getAbsolutePath() + File.separator
          + ONE_NODE_COLLECTION));
      server.request(createCmd);
      server.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    
    waitForCollection(cloudClient.getZkStateReader(), ONE_NODE_COLLECTION, 1);
    waitForRecoveriesToFinish(ONE_NODE_COLLECTION, cloudClient.getZkStateReader(), false);
    
    cloudClient.getZkStateReader().getLeaderRetry(ONE_NODE_COLLECTION, SHARD1, 30000);
    
    int docs = 2;
    for (SolrServer client : clients) {
      final String baseUrl = getBaseUrl((HttpSolrServer) client);
      addAndQueryDocs(baseUrl, docs);
      docs += 2;
    }
  }

  // 2 docs added every call
  private void addAndQueryDocs(final String baseUrl, int docs)
      throws Exception {
    HttpSolrServer qclient = new HttpSolrServer(baseUrl + "/onenodecollection" + "core");
    
    // it might take a moment for the proxy node to see us in their cloud state
    waitForNon403or404or503(qclient);
    
    // add a doc
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", docs);
    qclient.add(doc);
    qclient.commit();
    
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse results = qclient.query(query);
    assertEquals(docs - 1, results.getResults().getNumFound());
    qclient.shutdown();
    
    qclient = new HttpSolrServer(baseUrl + "/onenodecollection");
    results = qclient.query(query);
    assertEquals(docs - 1, results.getResults().getNumFound());
    
    doc = new SolrInputDocument();
    doc.addField("id", docs + 1);
    qclient.add(doc);
    qclient.commit();
    
    query = new SolrQuery("*:*");
    query.set("rows", 0);
    results = qclient.query(query);
    assertEquals(docs, results.getResults().getNumFound());
    qclient.shutdown();
  }
  
  private long testUpdateAndDelete() throws Exception {
    long docId = 99999999L;
    indexr("id", docId, t1, "originalcontent");
    
    commit();
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", t1 + ":originalcontent");
    QueryResponse results = clients.get(0).query(params);
    assertEquals(1, results.getResults().getNumFound());
    
    // update doc
    indexr("id", docId, t1, "updatedcontent");
    
    commit();
    
    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());
    
    params.set("q", t1 + ":updatedcontent");
    
    results = clients.get(0).query(params);
    assertEquals(1, results.getResults().getNumFound());
    
    UpdateRequest uReq = new UpdateRequest();
    // uReq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    uReq.deleteById(Long.toString(docId)).process(clients.get(0));
    
    commit();
    
    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());
    return docId;
  }
  
  private void brindDownShardIndexSomeDocsAndRecover() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.set("distrib", false);
    
    commit();
    
    long deadShardCount = shardToJetty.get(SHARD2).get(0).client.solrClient
        .query(query).getResults().getNumFound();

    query("q", "*:*", "sort", "n_tl1 desc");
    
    int oldLiveNodes = cloudClient.getZkStateReader().getZkClient().getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null, true).size();
    
    assertEquals(5, oldLiveNodes);
    
    // kill a shard
    CloudJettyRunner deadShard = chaosMonkey.stopShard(SHARD1, 0);
    
    // ensure shard is dead
    try {
      index_specific(deadShard.client.solrClient, id, 999, i1, 107, t1,
          "specific doc!");
      fail("This server should be down and this update should have failed");
    } catch (SolrServerException e) {
      // expected..
    }
    
    commit();
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // long cloudClientDocs = cloudClient.query(new
    // SolrQuery("*:*")).getResults().getNumFound();
    // System.out.println("clouddocs:" + cloudClientDocs);
    
    // try to index to a living shard at shard2

  
    long numFound1 = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    
    cloudClient.getZkStateReader().getLeaderRetry(DEFAULT_COLLECTION, SHARD1, 60000);
    index_specific(shardToJetty.get(SHARD1).get(1).client.solrClient, id, 1000, i1, 108, t1,
        "specific doc!");
    
    commit();
    
    checkShardConsistency(true, false);
    
    query("q", "*:*", "sort", "n_tl1 desc");
    

    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);

    long numFound2 = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    
    assertEquals(numFound1 + 1, numFound2);
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1001);
    
    controlClient.add(doc);
    
    // try adding a doc with CloudSolrServer
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    
    try {
      ureq.process(cloudClient);
    } catch(SolrServerException e){
      // try again
      Thread.sleep(3500);
      ureq.process(cloudClient);
    }
    
    commit();
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    long numFound3 = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    
    // lets just check that the one doc since last commit made it in...
    assertEquals(numFound2 + 1, numFound3);
    
    // test debugging
    testDebugQueries();
    
    if (VERBOSE) {
      System.err.println(controlClient.query(new SolrQuery("*:*")).getResults()
          .getNumFound());
      
      for (SolrServer client : clients) {
        try {
          SolrQuery q = new SolrQuery("*:*");
          q.set("distrib", false);
          System.err.println(client.query(q).getResults()
              .getNumFound());
        } catch (Exception e) {
          
        }
      }
    }
    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");
    
    // this should trigger a recovery phase on deadShard
    ChaosMonkey.start(deadShard.jetty);
    
    // make sure we have published we are recovering
    Thread.sleep(1500);
    
    waitForRecoveriesToFinish(false);
    
    deadShardCount = shardToJetty.get(SHARD1).get(0).client.solrClient
        .query(query).getResults().getNumFound();
    // if we properly recovered, we should now have the couple missing docs that
    // came in while shard was down
    checkShardConsistency(true, false);
    
    
    // recover over 100 docs so we do more than just peer sync (replicate recovery)
    chaosMonkey.stopJetty(deadShard);

    for (int i = 0; i < 226; i++) {
      doc = new SolrInputDocument();
      doc.addField("id", 2000 + i);
      controlClient.add(doc);
      ureq = new UpdateRequest();
      ureq.add(doc);
      // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
      ureq.process(cloudClient);
    }
    commit();
    
    Thread.sleep(1500);
    
    ChaosMonkey.start(deadShard.jetty);
    
    // make sure we have published we are recovering
    Thread.sleep(1500);
    
    waitForThingsToLevelOut(30);
    
    Thread.sleep(500);
    
    waitForRecoveriesToFinish(false);
    
    checkShardConsistency(true, false);
    
    // try a backup command
    final HttpSolrServer client = (HttpSolrServer) shardToJetty.get(SHARD2).get(0).client.solrClient;
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/replication");
    params.set("command", "backup");
    File location = new File(TEMP_DIR, BasicDistributedZk2Test.class.getName() + "-backupdir-" + System.currentTimeMillis());
    params.set("location", location.getAbsolutePath());

    QueryRequest request = new QueryRequest(params);
    NamedList<Object> results = client.request(request );
    
    checkForBackupSuccess(client, location);

  }
  private void checkForBackupSuccess(final HttpSolrServer client, File location)
      throws InterruptedException, IOException {
    class CheckStatus extends Thread {
      volatile String fail = null;
      volatile String response = null;
      volatile boolean success = false;
      final Pattern p = Pattern
          .compile("<str name=\"snapshotCompletedAt\">(.*?)</str>");
      
      CheckStatus() {}
      
      @Override
      public void run() {
        String masterUrl = client.getBaseURL() + "/replication?command="
            + ReplicationHandler.CMD_DETAILS;
        
        try {
          response = client.getHttpClient().execute(new HttpGet(masterUrl), new BasicResponseHandler());
          if (response.contains("<str name=\"status\">success</str>")) {
            Matcher m = p.matcher(response);
            if (!m.find()) {
              fail("could not find the completed timestamp in response.");
            }
            
            success = true;
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail = e.getMessage();
        }
        
      };
    }
    
    int waitCnt = 0;
    CheckStatus checkStatus = new CheckStatus();
    while (true) {
      checkStatus.run();
      if (checkStatus.fail != null) {
        fail(checkStatus.fail);
      }
      if (checkStatus.success) {
        break;
      }
      Thread.sleep(500);
      if (waitCnt == 90) {
        fail("Backup success not detected:" + checkStatus.response);
      }
      waitCnt++;
    }
    
    File[] files = location.listFiles(new FilenameFilter() {
      
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith("snapshot")) {
          return true;
        }
        return false;
      }
    });
    assertEquals(Arrays.asList(files).toString(), 1, files.length);
    File snapDir = files[0];
    
    AbstractSolrTestCase.recurseDelete(snapDir); // clean up the snap dir
  }
  
  private void addNewReplica() throws Exception {
    JettySolrRunner newReplica = createJettys(1).get(0);
    
    waitForRecoveriesToFinish(false);
    
    // new server should be part of first shard
    // how many docs are on the new shard?
    for (CloudJettyRunner cjetty : shardToJetty.get(SHARD1)) {
      if (VERBOSE) System.err.println("shard1 total:"
          + cjetty.client.solrClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    for (CloudJettyRunner cjetty : shardToJetty.get(SHARD2)) {
      if (VERBOSE) System.err.println("shard2 total:"
          + cjetty.client.solrClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    checkShardConsistency(SHARD1);
    checkShardConsistency(SHARD2);
    
    assertDocCounts(VERBOSE);
  }
  
  private void testDebugQueries() throws Exception {
    handle.put("explain", SKIPVAL);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    handle.put("track", SKIP);
    query("q", "now their fox sat had put", "fl", "*,score",
        CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);
  }
  
}
