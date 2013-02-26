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
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
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
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
public class BasicDistributedZk2Test extends AbstractFullDistribZkTestBase {
  private static final String ONE_NODE_COLLECTION = "onenodecollection";

  @BeforeClass
  public static void beforeThisClass2() throws Exception {
    // TODO: we use an fs based dir because something
    // like a ram dir will not recover correctly right now
    // because tran log will still exist on restart and ram
    // dir will not persist - perhaps translog can empty on
    // start if using an EphemeralDirectoryFactory 
    useFactory(null);
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
      CloudJettyRunner cloudJetty = shardToJetty.get("shard1").get(0);
      chaosMonkey.expireSession(cloudJetty.jetty);
      
      indexr("id", docId + 1, t1, "slip this doc in");
      
      waitForRecoveriesToFinish(false);
      
      checkShardConsistency("shard1");
      
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
      final String baseUrl = ((HttpSolrServer) clients.get(0)).getBaseURL().substring(
          0,
          ((HttpSolrServer) clients.get(0)).getBaseURL().length()
              - DEFAULT_COLLECTION.length() - 1);
      HttpSolrServer server = new HttpSolrServer(baseUrl);
      server.setConnectionTimeout(15000);
      server.setSoTimeout(30000);
      Create createCmd = new Create();
      createCmd.setRoles("none");
      createCmd.setCoreName(ONE_NODE_COLLECTION + "core");
      createCmd.setCollection(ONE_NODE_COLLECTION);
      createCmd.setNumShards(1);
      createCmd.setDataDir(dataDir.getAbsolutePath() + File.separator
          + ONE_NODE_COLLECTION);
      server.request(createCmd);
    } catch (Exception e) {
      e.printStackTrace();
      //fail
    }
    
    waitForRecoveriesToFinish(ONE_NODE_COLLECTION, cloudClient.getZkStateReader(), false);
    
    final String baseUrl2 = ((HttpSolrServer) clients.get(1)).getBaseURL().substring(
        0,
        ((HttpSolrServer) clients.get(1)).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    HttpSolrServer qclient = new HttpSolrServer(baseUrl2 + "/onenodecollection" + "core");
    
    // add a doc
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    qclient.add(doc);
    qclient.commit();
    
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse results = qclient.query(query);
    assertEquals(1, results.getResults().getNumFound());
    
    qclient = new HttpSolrServer(baseUrl2 + "/onenodecollection");
    results = qclient.query(query);
    assertEquals(1, results.getResults().getNumFound());
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
    CloudJettyRunner deadShard = chaosMonkey.stopShard(SHARD2, 0);


    // we are careful to make sure the downed node is no longer in the state,
    // because on some systems (especially freebsd w/ blackhole enabled), trying
    // to talk to a downed node causes grief
    Set<CloudJettyRunner> jetties = new HashSet<CloudJettyRunner>();
    jetties.addAll(shardToJetty.get(SHARD2));
    jetties.remove(deadShard);
    
    // ensure shard is dead
    try {
      index_specific(deadShard.client.solrClient, id, 999, i1, 107, t1,
          "specific doc!");
      fail("This server should be down and this update should have failed");
    } catch (SolrServerException e) {
      // expected..
    }
    
    commit();
    
    printLayout();
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // long cloudClientDocs = cloudClient.query(new
    // SolrQuery("*:*")).getResults().getNumFound();
    // System.out.println("clouddocs:" + cloudClientDocs);
    
    // try to index to a living shard at shard2

  
    long numFound1 = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    
    index_specific(shardToJetty.get(SHARD2).get(1).client.solrClient, id, 1000, i1, 108, t1,
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
      Thread.sleep(500);
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
    
    deadShardCount = shardToJetty.get(SHARD2).get(0).client.solrClient
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
    
    waitForThingsToLevelOut(15);
    
    Thread.sleep(500);
    
    waitForRecoveriesToFinish(false);
    
    checkShardConsistency(true, false);
    
    // try a backup command
    final HttpSolrServer client = (HttpSolrServer) shardToJetty.get(SHARD2).get(0).client.solrClient;
    System.out.println("base url: "+ client.getBaseURL());
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/replication");
    params.set("command", "backup");

    QueryRequest request = new QueryRequest(params);
    NamedList<Object> results = client.request(request );
    System.out.println("results:" + results);
    
    
    checkForBackupSuccess(client);
    
  }
  private void checkForBackupSuccess(final HttpSolrServer client)
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
        URL url;
        InputStream stream = null;
        try {
          url = new URL(masterUrl);
          stream = url.openStream();
          response = IOUtils.toString(stream, "UTF-8");
          if (response.contains("<str name=\"status\">success</str>")) {
            Matcher m = p.matcher(response);
            if (!m.find()) {
              fail("could not find the completed timestamp in response.");
            }
            
            success = true;
          }
          stream.close();
        } catch (Exception e) {
          e.printStackTrace();
          fail = e.getMessage();
        } finally {
          IOUtils.closeQuietly(stream);
        }
        
      };
    }
    ;
    SolrCore core = ((SolrDispatchFilter) shardToJetty.get(SHARD2).get(0).jetty
        .getDispatchFilter().getFilter()).getCores().getCore("collection1");
    String ddir;
    try {
      ddir = core.getDataDir(); 
    } finally {
      core.close();
    }
    File dataDir = new File(ddir);
    
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
      Thread.sleep(200);
      if (waitCnt == 20) {
        fail("Backup success not detected:" + checkStatus.response);
      }
      waitCnt++;
    }
    
    File[] files = dataDir.listFiles(new FilenameFilter() {
      
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
    for (CloudJettyRunner cjetty : shardToJetty.get("shard1")) {
      if (VERBOSE) System.err.println("total:"
          + cjetty.client.solrClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    checkShardConsistency("shard1");
    
    assertDocCounts(VERBOSE);
  }
  
  private void testDebugQueries() throws Exception {
    handle.put("explain", SKIPVAL);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    query("q", "now their fox sat had put", "fl", "*,score",
        CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);
  }
  
}
