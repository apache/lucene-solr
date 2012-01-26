package org.apache.solr.cloud;

/**
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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * 
 * TODO: we should still test this works as a custom update chain as well as
 * what we test now - the default update chain
 * 
 */
@Ignore
public class FullSolrCloudTest extends AbstractDistributedZkTestCase {
  
  private static final String SHARD2 = "shard2";
  
  protected static final String DEFAULT_COLLECTION = "collection1";
  
  String t1 = "a_t";
  String i1 = "a_si";
  String nint = "n_i";
  String tint = "n_ti";
  String nfloat = "n_f";
  String tfloat = "n_tf";
  String ndouble = "n_d";
  String tdouble = "n_td";
  String nlong = "n_l";
  String tlong = "other_tl1";
  String ndate = "n_dt";
  String tdate = "n_tdt";
  
  String oddField = "oddField_s";
  String missingField = "ignore_exception__missing_but_valid_field_t";
  String invalidField = "ignore_exception__invalid_field_not_in_schema";
  protected int sliceCount;
  
  protected volatile CloudSolrServer cloudClient;
  
  protected Map<JettySolrRunner,ZkNodeProps> jettyToInfo = new HashMap<JettySolrRunner,ZkNodeProps>();
  protected Map<CloudSolrServerClient,ZkNodeProps> clientToInfo = new HashMap<CloudSolrServerClient,ZkNodeProps>();
  protected Map<String,List<SolrServer>> shardToClient = new HashMap<String,List<SolrServer>>();
  protected Map<String,List<CloudJettyRunner>> shardToJetty = new HashMap<String,List<CloudJettyRunner>>();
  private AtomicInteger jettyIntCntr = new AtomicInteger(0);
  protected ChaosMonkey chaosMonkey;
  protected volatile ZkStateReader zkStateReader;
  
  private Map<String,SolrServer> shardToLeaderClient = new HashMap<String,SolrServer>();
  private Map<String,CloudJettyRunner> shardToLeaderJetty = new HashMap<String,CloudJettyRunner>();
  
  class CloudJettyRunner {
    JettySolrRunner jetty;
    String nodeName;
    String coreNodeName;
  }
  
  static class CloudSolrServerClient {
    SolrServer client;
    String shardName;
    
    public CloudSolrServerClient() {}
    
    public CloudSolrServerClient(SolrServer client) {
      this.client = client;
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((client == null) ? 0 : client.hashCode());
      return result;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      CloudSolrServerClient other = (CloudSolrServerClient) obj;
      if (client == null) {
        if (other.client != null) return false;
      } else if (!client.equals(other.client)) return false;
      return true;
    }
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    ignoreException(".*");
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System
        .setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solrcloud.update.delay", "0");
    System.setProperty("enable.update.log", "true");
    System.setProperty("remove.version.field", "true");
  }
  
  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solrcloud.update.delay");
    System.clearProperty("enable.update.log");
    System.clearProperty("remove.version.field");
  }
  
  public FullSolrCloudTest() {
    fixShardCount = true;
    
    shardCount = 4;
    sliceCount = 2;
    // TODO: for now, turn off stress because it uses regular clients, and we
    // need the cloud client because we kill servers
    stress = 0;
  }
  
  protected void initCloud() throws Exception {
    if (zkStateReader == null) {
      synchronized (this) {
        if (zkStateReader != null) {
          return;
        }
        zkStateReader = new ZkStateReader(zkServer.getZkAddress(), 10000,
            AbstractZkTestCase.TIMEOUT);
        
        zkStateReader.createClusterStateWatchersAndUpdate();
      }
      
      chaosMonkey = new ChaosMonkey(zkServer, zkStateReader,
          DEFAULT_COLLECTION, shardToJetty, shardToClient, shardToLeaderClient,
          shardToLeaderJetty, random);
    }
    
    // wait until shards have started registering...
    while (!zkStateReader.getCloudState().getCollections()
        .contains(DEFAULT_COLLECTION)) {
      Thread.sleep(500);
    }
    while (zkStateReader.getCloudState().getSlices(DEFAULT_COLLECTION).size() != sliceCount) {
      Thread.sleep(500);
    }
    
    // use the distributed solrj client
    if (cloudClient == null) {
      synchronized (this) {
        if (cloudClient != null) {
          return;
        }
        try {
          CloudSolrServer server = new CloudSolrServer(zkServer.getZkAddress());
          server.setDefaultCollection(DEFAULT_COLLECTION);
          server.getLbServer().getHttpClient().getParams()
              .setConnectionManagerTimeout(5000);
          server.getLbServer().getHttpClient().getParams().setSoTimeout(15000);
          cloudClient = server;
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
  
  @Override
  protected void createServers(int numServers) throws Exception {
    
    System.setProperty("collection", "control_collection");
    controlJetty = createJetty(testDir, testDir + "/control/data",
        "control_shard");
    System.clearProperty("collection");
    controlClient = createNewSolrServer(controlJetty.getLocalPort());
    
    createJettys(numServers);
    
  }
  
  private List<JettySolrRunner> createJettys(int numJettys) throws Exception,
      InterruptedException, TimeoutException, IOException, KeeperException,
      URISyntaxException {
    List<JettySolrRunner> jettys = new ArrayList<JettySolrRunner>();
    List<SolrServer> clients = new ArrayList<SolrServer>();
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numJettys; i++) {
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner j = createJetty(testDir, testDir + "/jetty"
          + this.jettyIntCntr.incrementAndGet(), null, "solrconfig.xml", null);
      jettys.add(j);
      SolrServer client = createNewSolrServer(j.getLocalPort());
      clients.add(client);
    }
    
    initCloud();
    
    this.jettys.addAll(jettys);
    this.clients.addAll(clients);
    
    updateMappingsFromZk(this.jettys, this.clients);
    
    // build the shard string
    for (int i = 1; i <= numJettys / 2; i++) {
      JettySolrRunner j = this.jettys.get(i);
      JettySolrRunner j2 = this.jettys.get(i + (numJettys / 2 - 1));
      if (sb.length() > 0) sb.append(',');
      sb.append("localhost:").append(j.getLocalPort()).append(context);
      sb.append("|localhost:").append(j2.getLocalPort()).append(context);
    }
    shards = sb.toString();
    
    return jettys;
  }
  
  public JettySolrRunner createJetty(String dataDir, String shardList,
      String solrConfigOverride) throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(getSolrHome(), "/solr", 0,
        solrConfigOverride, null, false);
    jetty.setShards(shardList);
    jetty.setDataDir(dataDir);
    jetty.start();
    
    return jetty;
  }
  
  protected void updateMappingsFromZk(List<JettySolrRunner> jettys,
      List<SolrServer> clients) throws Exception, IOException, KeeperException,
      URISyntaxException {
    zkStateReader.updateCloudState(true);
    shardToClient.clear();
    shardToJetty.clear();
    jettyToInfo.clear();
    
    CloudState cloudState = zkStateReader.getCloudState();
    Map<String,Slice> slices = cloudState.getSlices(DEFAULT_COLLECTION);
    
    if (slices == null) {
      throw new RuntimeException("No slices found for collection "
          + DEFAULT_COLLECTION + " in " + cloudState.getCollections());
    }
    
    for (SolrServer client : clients) {
      // find info for this client in zk 
      nextClient:
      // we find ou state by simply matching ports...
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          int port = new URI(((CommonsHttpSolrServer) client).getBaseURL())
              .getPort();
          
          if (shard.getKey().contains(":" + port + "_")) {
            CloudSolrServerClient csc = new CloudSolrServerClient();
            csc.client = client;
            csc.shardName = shard.getValue().get(ZkStateReader.NODE_NAME_PROP);
            boolean isLeader = shard.getValue().containsKey(
                ZkStateReader.LEADER_PROP);
            clientToInfo.put(csc, shard.getValue());
            List<SolrServer> list = shardToClient.get(slice.getKey());
            if (list == null) {
              list = new ArrayList<SolrServer>();
              shardToClient.put(slice.getKey(), list);
            }
            list.add(client);
            
            if (isLeader) {
              shardToLeaderClient.put(slice.getKey(), client);
            }
            break nextClient;
          }
        }
      }
    }
    
    for (Map.Entry<String,Slice> slice : slices.entrySet()) {
      // check that things look right
      assertEquals(slice.getValue().getShards().size(), shardToClient.get(slice.getKey()).size());
    }
 
    for (JettySolrRunner jetty : jettys) {
      int port = jetty.getLocalPort();
      if (port == -1) {
        continue; // If we cannot get the port, this jetty is down
      }
      
      nextJetty:
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          if (shard.getKey().contains(":" + port + "_")) {
            jettyToInfo.put(jetty, shard.getValue());
            List<CloudJettyRunner> list = shardToJetty.get(slice.getKey());
            if (list == null) {
              list = new ArrayList<CloudJettyRunner>();
              shardToJetty.put(slice.getKey(), list);
            }
            boolean isLeader = shard.getValue().containsKey(
                ZkStateReader.LEADER_PROP);
            CloudJettyRunner cjr = new CloudJettyRunner();
            cjr.jetty = jetty;
            cjr.nodeName = shard.getValue().get(ZkStateReader.NODE_NAME_PROP);
            cjr.coreNodeName = shard.getKey();
            list.add(cjr);
            if (isLeader) {
              shardToLeaderJetty.put(slice.getKey(), cjr);
            }
            break nextJetty;
          }
        }
      }
    }
    
    // # of jetties may not match replicas in shard here, because we don't map
    // jetties that are not running - every shard should have at least one
    // running jetty though
    for (Map.Entry<String,Slice> slice : slices.entrySet()) {
      // check that things look right
      List<CloudJettyRunner> jetties = shardToJetty.get(slice.getKey());
      assertNotNull("Test setup problem: We found no jetties for shard: " + slice.getKey()
          + " just:" + shardToJetty.keySet(), jetties);
      assertTrue(jetties.size() > 0);
    }
  }
  
  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {
    
    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < sliceCount; i++) {
        if (i > 0) sb.append(',');
        sb.append("shard" + (i + 1));
      }
      params.set("shards", sb.toString());
    }
  }
  
  @Override
  protected void indexDoc(SolrInputDocument doc) throws IOException,
      SolrServerException {
    controlClient.add(doc);
    
    // if we wanted to randomly pick a client - but sometimes they may be
    // down...
    
    // boolean pick = random.nextBoolean();
    //
    // int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) %
    // sliceCount;
    //
    // if (pick && sliceCount > 1) {
    // which = which + ((shardCount / sliceCount) *
    // random.nextInt(sliceCount-1));
    // }
    //
    // CommonsHttpSolrServer client = (CommonsHttpSolrServer)
    // clients.get(which);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    ureq.process(cloudClient);
  }
  
  protected void index_specific(int serverNumber, Object... fields)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);
    
    CommonsHttpSolrServer client = (CommonsHttpSolrServer) clients
        .get(serverNumber);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
  }
  
  protected void index_specific(SolrServer client, Object... fields)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
    
    // add to control second in case adding to shards fails
    controlClient.add(doc);
  }
  
  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    for (SolrServer client : clients) {
      UpdateRequest ureq = new UpdateRequest();
      // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
      ureq.deleteByQuery(q).process(client);
    }
  }// serial commit...
  
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
      
      indexr(id, 1, i1, 100, tlong, 100, t1,
          "now is the time for all good men", "foo_f", 1.414f, "foo_b", "true",
          "foo_d", 1.414d);
      
      // make sure we are in a steady state...
      waitForRecoveriesToFinish(false);
      
      commit();
      
      assertDocCounts(false);
      
      indexAbunchOfDocs();
      
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
      ZkNodeProps leaderProps = zkStateReader.getLeaderProps(
          DEFAULT_COLLECTION, SHARD2);
      
      String nodeName = leaderProps.get(ZkStateReader.NODE_NAME_PROP);
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
        printLayout();
      }
    }
    
  }
  
  private long testUpdateAndDelete() throws Exception, SolrServerException,
      IOException {
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
  
  private void addNewReplica() throws Exception, InterruptedException,
      TimeoutException, IOException, KeeperException, URISyntaxException,
      SolrServerException {
    JettySolrRunner newReplica = createJettys(1).get(0);
    
    waitForRecoveriesToFinish(false);
    
    // new server should be part of first shard
    // how many docs are on the new shard?
    for (SolrServer client : shardToClient.get("shard1")) {
      if (VERBOSE) System.out.println("total:"
          + client.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    checkShardConsistency("shard1");
    
    assertDocCounts(VERBOSE);
  }
  
  protected void waitForRecoveriesToFinish(boolean verbose)
      throws KeeperException, InterruptedException {
    boolean cont = true;
    int cnt = 0;
    
    while (cont) {
      if (verbose) System.out.println("-");
      boolean sawLiveRecovering = false;
      zkStateReader.updateCloudState(true);
      CloudState cloudState = zkStateReader.getCloudState();
      Map<String,Slice> slices = cloudState.getSlices(DEFAULT_COLLECTION);
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Map<String,ZkNodeProps> shards = entry.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : shards.entrySet()) {
          if (verbose) System.out.println("rstate:"
              + shard.getValue().get(ZkStateReader.STATE_PROP)
              + " live:"
              + cloudState.liveNodesContain(shard.getValue().get(
                  ZkStateReader.NODE_NAME_PROP)));
          String state = shard.getValue().get(ZkStateReader.STATE_PROP);
          if ((state.equals(ZkStateReader.RECOVERING)
              || state.equals(ZkStateReader.SYNC))
              && cloudState.liveNodesContain(shard.getValue().get(
                  ZkStateReader.NODE_NAME_PROP))) {
            sawLiveRecovering = true;
          }
        }
      }
      if (!sawLiveRecovering || cnt == 10) {
        if (!sawLiveRecovering) {
          if (verbose) System.out.println("no one is recoverying");
        } else {
          if (verbose) System.out
              .println("gave up waiting for recovery to finish..");
        }
        cont = false;
      } else {
        Thread.sleep(2000);
      }
      cnt++;
    }
  }
  
  private void brindDownShardIndexSomeDocsAndRecover() throws Exception,
      SolrServerException, IOException, InterruptedException {
    
    commit();
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // kill a shard
    JettySolrRunner deadShard = chaosMonkey.stopShard(SHARD2, 0);
    
    // ensure shard is dead
    try {
      // TODO: ignore fail
      index_specific(shardToClient.get(SHARD2).get(0), id, 999, i1, 107, t1,
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
    // TODO: this can fail with connection refused !????
    index_specific(shardToClient.get(SHARD2).get(1), id, 1000, i1, 108, t1,
        "specific doc!");
    
    commit();
    
    checkShardConsistency(true, false);
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // try adding a doc with CloudSolrServer
    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);
    SolrQuery query = new SolrQuery("*:*");
    long numFound1 = cloudClient.query(query).getResults().getNumFound();
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1001);
    
    controlClient.add(doc);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(cloudClient);
    
    commit();
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    long numFound2 = cloudClient.query(query).getResults().getNumFound();
    
    // lets just check that the one doc since last commit made it in...
    assertEquals(numFound1 + 1, numFound2);
    
    // test debugging
    testDebugQueries();
    
    if (VERBOSE) {
      System.out.println(controlClient.query(new SolrQuery("*:*")).getResults()
          .getNumFound());
      
      for (SolrServer client : clients) {
        try {
          System.out.println(client.query(new SolrQuery("*:*")).getResults()
              .getNumFound());
        } catch (Exception e) {
          
        }
      }
    }
    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");
    
    // this should trigger a recovery phase on deadShard
    
    deadShard.start(true);
    
    // make sure we have published we are recoverying
    Thread.sleep(1500);
    
    waitForRecoveriesToFinish(false);
    
    List<SolrServer> s2c = shardToClient.get(SHARD2);
    
    // if we properly recovered, we should now have the couple missing docs that
    // came in while shard was down
    assertEquals(s2c.get(0).query(new SolrQuery("*:*")).getResults()
        .getNumFound(), s2c.get(1).query(new SolrQuery("*:*")).getResults()
        .getNumFound());
  }
  
  private void testDebugQueries() throws Exception {
    handle.put("explain", UNORDERED);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    query("q", "now their fox sat had put", "fl", "*,score",
        CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);
  }
  
  private void checkQueries() throws Exception {
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // random value sort
    for (String f : fieldNames) {
      query("q", "*:*", "sort", f + " desc");
      query("q", "*:*", "sort", f + " asc");
    }
    
    // these queries should be exactly ordered and scores should exactly match
    query("q", "*:*", "sort", i1 + " desc");
    query("q", "*:*", "sort", i1 + " asc");
    query("q", "*:*", "sort", i1 + " desc", "fl", "*,score");
    query("q", "*:*", "sort", "n_tl1 asc", "fl", "score"); // test legacy
                                                           // behavior -
                                                           // "score"=="*,score"
    query("q", "*:*", "sort", "n_tl1 desc");
    handle.put("maxScore", SKIPVAL);
    query("q", "{!func}" + i1);// does not expect maxScore. So if it comes
                               // ,ignore it.
                               // JavaBinCodec.writeSolrDocumentList()
    // is agnostic of request params.
    handle.remove("maxScore");
    query("q", "{!func}" + i1, "fl", "*,score"); // even scores should match
                                                 // exactly here
    
    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);
    
    handle.put("maxScore", SKIPVAL);
    query("q", "quick");
    query("q", "all", "fl", "id", "start", "0");
    query("q", "all", "fl", "foofoofoo", "start", "0"); // no fields in returned
                                                        // docs
    query("q", "all", "fl", "id", "start", "100");
    
    handle.put("score", SKIPVAL);
    query("q", "quick", "fl", "*,score");
    query("q", "all", "fl", "*,score", "start", "1");
    query("q", "all", "fl", "*,score", "start", "100");
    
    query("q", "now their fox sat had put", "fl", "*,score", "hl", "true",
        "hl.fl", t1);
    
    query("q", "now their fox sat had put", "fl", "foofoofoo", "hl", "true",
        "hl.fl", t1);
    
    query("q", "matchesnothing", "fl", "*,score");
    
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "count");
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "count", "facet.mincount", 2);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "index");
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", -1, "facet.sort", "index", "facet.mincount", 2);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.limit", 1);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.query", "quick",
        "facet.query", "all", "facet.query", "*:*");
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.offset", 1);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1,
        "facet.mincount", 2);
    
    // test faceting multiple things at once
    query("q", "*:*", "rows", 100, "facet", "true", "facet.query", "quick",
        "facet.query", "all", "facet.query", "*:*", "facet.field", t1);
    
    // test filter tagging, facet exclusion, and naming (multi-select facet
    // support)
    query("q", "*:*", "rows", 100, "facet", "true", "facet.query",
        "{!key=myquick}quick", "facet.query", "{!key=myall ex=a}all",
        "facet.query", "*:*", "facet.field", "{!key=mykey ex=a}" + t1,
        "facet.field", "{!key=other ex=b}" + t1, "facet.field",
        "{!key=again ex=a,b}" + t1, "facet.field", t1, "fq",
        "{!tag=a}id:[1 TO 7]", "fq", "{!tag=b}id:[3 TO 9]");
    query("q", "*:*", "facet", "true", "facet.field",
        "{!ex=t1}SubjectTerms_mfacet", "fq",
        "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", "10",
        "facet.mincount", "1");
    
    // test field that is valid in schema but missing in all shards
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field",
        missingField, "facet.mincount", 2);
    // test field that is valid in schema and missing in some shards
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", oddField,
        "facet.mincount", 2);
    
    query("q", "*:*", "sort", i1 + " desc", "stats", "true", "stats.field", i1);
    
    // Try to get better coverage for refinement queries by turning off over
    // requesting.
    // This makes it much more likely that we may not get the top facet values
    // and hence
    // we turn of that checking.
    handle.put("facet_fields", SKIPVAL);
    query("q", "*:*", "rows", 0, "facet", "true", "facet.field", t1,
        "facet.limit", 5, "facet.shard.limit", 5);
    // check a complex key name
    query("q", "*:*", "rows", 0, "facet", "true", "facet.field",
        "{!key='a b/c \\' \\} foo'}" + t1, "facet.limit", 5,
        "facet.shard.limit", 5);
    handle.remove("facet_fields");
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // index the same document to two shards and make sure things
    // don't blow up.
    // assumes first n clients are first n shards
    if (clients.size() >= 2) {
      index(id, 100, i1, 107, t1, "oh no, a duplicate!");
      for (int i = 0; i < shardCount; i++) {
        index_specific(i, id, 100, i1, 107, t1, "oh no, a duplicate!");
      }
      commit();
      query("q", "duplicate", "hl", "true", "hl.fl", t1);
      query("q", "fox duplicate horses", "hl", "true", "hl.fl", t1);
      query("q", "*:*", "rows", 100);
    }
  }
  
  private void indexAbunchOfDocs() throws Exception {
    indexr(id, 2, i1, 50, tlong, 50, t1, "to come to the aid of their country.");
    indexr(id, 3, i1, 2, tlong, 2, t1, "how now brown cow");
    indexr(id, 4, i1, -100, tlong, 101, t1,
        "the quick fox jumped over the lazy dog");
    indexr(id, 5, i1, 500, tlong, 500, t1,
        "the quick fox jumped way over the lazy dog");
    indexr(id, 6, i1, -600, tlong, 600, t1, "humpty dumpy sat on a wall");
    indexr(id, 7, i1, 123, tlong, 123, t1, "humpty dumpy had a great fall");
    indexr(id, 8, i1, 876, tlong, 876, t1,
        "all the kings horses and all the kings men");
    indexr(id, 9, i1, 7, tlong, 7, t1, "couldn't put humpty together again");
    indexr(id, 10, i1, 4321, tlong, 4321, t1, "this too shall pass");
    indexr(id, 11, i1, -987, tlong, 987, t1,
        "An eye for eye only ends up making the whole world blind.");
    indexr(id, 12, i1, 379, tlong, 379, t1,
        "Great works are performed, not by strength, but by perseverance.");
    indexr(id, 13, i1, 232, tlong, 232, t1, "no eggs on wall, lesson learned",
        oddField, "odd man out");
    
    indexr(id, 14, "SubjectTerms_mfacet", new String[] {"mathematical models",
        "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[] {"test 1", "test 2",
        "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[] {"test 1", "test 2",
        "test3"});
    String[] vals = new String[100];
    for (int i = 0; i < 100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);
    
    for (int i = 100; i < 150; i++) {
      indexr(id, i);
    }
  }
  
  protected void checkShardConsistency(String shard) throws Exception {
    checkShardConsistency(shard, false);
  }
  
  protected String checkShardConsistency(String shard, boolean verbose)
      throws Exception {
    
    List<SolrServer> solrClients = shardToClient.get(shard);
    if (solrClients == null) {
      throw new RuntimeException("shard not found:" + shard + " keys:"
          + shardToClient.keySet());
    }
    long num = -1;
    long lastNum = -1;
    String failMessage = null;
    if (verbose) System.out.println("check const of " + shard);
    int cnt = 0;
    
    assertEquals(
        "The client count does not match up with the shard count for slice:"
            + shard,
        zkStateReader.getCloudState().getSlice(DEFAULT_COLLECTION, shard)
            .getShards().size(), solrClients.size());

    for (SolrServer client : solrClients) {
      ZkNodeProps props = clientToInfo.get(new CloudSolrServerClient(client));
      if (verbose) System.out.println("client" + cnt++);
      if (verbose) System.out.println("PROPS:" + props);
      
      try {
        SolrQuery query = new SolrQuery("*:*");
        query.set("distrib", false);
        num = client.query(query).getResults().getNumFound();
      } catch (SolrServerException e) {
        if (verbose) System.out.println("error contacting client: "
            + e.getMessage() + "\n");
        continue;
      }
      
      boolean live = false;
      String nodeName = props.get(ZkStateReader.NODE_NAME_PROP);
      if (zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        live = true;
      }
      if (verbose) System.out.println(" live:" + live);
      
      if (verbose) System.out.println(" num:" + num + "\n");
      
      boolean active = props.get(ZkStateReader.STATE_PROP).equals(
          ZkStateReader.ACTIVE);
      if (active && live) {
        if (lastNum > -1 && lastNum != num && failMessage == null) {
          failMessage = shard + " is not consistent, expected:" + lastNum
              + " and got:" + num;
        }
        lastNum = num;
      }
    }
    
    return failMessage;
    
  }
  
  protected void checkShardConsistency() throws Exception {
    checkShardConsistency(true, false);
  }
  
  protected void checkShardConsistency(boolean checkVsControl, boolean verbose)
      throws Exception {
    long docs = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    if (verbose) System.out.println("Control Docs:" + docs);
    
    updateMappingsFromZk(jettys, clients);
    
    Set<String> theShards = shardToClient.keySet();
    String failMessage = null;
    for (String shard : theShards) {
      String shardFailMessage = checkShardConsistency(shard, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }
    
    if (failMessage != null) {
      fail(failMessage);
    }
    
    if (checkVsControl) {
      // now check that the right # are on each shard
      theShards = shardToClient.keySet();
      int cnt = 0;
      for (String s : theShards) {
        int times = shardToClient.get(s).size();
        for (int i = 0; i < times; i++) {
          try {
            SolrServer client = shardToClient.get(s).get(i);
            ZkNodeProps props = clientToInfo.get(new CloudSolrServerClient(
                client));
            boolean active = props.get(ZkStateReader.STATE_PROP).equals(
                ZkStateReader.ACTIVE);
            if (active) {
              SolrQuery query = new SolrQuery("*:*");
              query.set("distrib", false);
              long results = client.query(query).getResults().getNumFound();
              if (verbose) System.out.println(new ZkCoreNodeProps(props)
                  .getCoreUrl() + " : " + results);
              if (verbose) System.out.println("shard:"
                  + props.get(ZkStateReader.SHARD_ID_PROP));
              cnt += results;
              break;
            }
          } catch (SolrServerException e) {
            // if we have a problem, try the next one
            if (i == times - 1) {
              throw e;
            }
          }
        }
      }
      
      SolrQuery q = new SolrQuery("*:*");
      long cloudClientDocs = cloudClient.query(q).getResults().getNumFound();
      assertEquals(
          "adding up the # of docs on each shard does not match the control - cloud client returns:"
              + cloudClientDocs, docs, cnt);
    }
  }
  
  private SolrServer getClient(String nodeName) {
    for (CloudSolrServerClient client : clientToInfo.keySet()) {
      if (client.shardName.equals(nodeName)) {
        return client.client;
      }
    }
    return null;
  }
  
  protected void assertDocCounts(boolean verbose) throws Exception {
    // TODO: as we create the clients, we should build a map from shard to
    // node/client
    // and node/client to shard?
    if (verbose) System.out.println("control docs:"
        + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound()
        + "\n\n");
    long controlCount = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    
    // do some really inefficient mapping...
    ZkStateReader zk = new ZkStateReader(zkServer.getZkAddress(), 10000,
        AbstractZkTestCase.TIMEOUT);
    Map<String,Slice> slices = null;
    CloudState cloudState;
    try {
      zk.createClusterStateWatchersAndUpdate();
      cloudState = zk.getCloudState();
      slices = cloudState.getSlices(DEFAULT_COLLECTION);
    } finally {
      zk.close();
    }
    
    if (slices == null) {
      throw new RuntimeException("Could not find collection "
          + DEFAULT_COLLECTION + " in " + cloudState.getCollections());
    }
    
    for (SolrServer client : clients) {
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          String shardName = new URI(
              ((CommonsHttpSolrServer) client).getBaseURL()).getPort()
              + "_solr_";
          if (verbose && shard.getKey().endsWith(shardName)) {
            System.out.println("shard:" + slice.getKey());
            System.out.println(shard.getValue());
          }
        }
      }
      
      long count = 0;
      String currentState = clientToInfo.get(new CloudSolrServerClient(client))
          .get(ZkStateReader.STATE_PROP);
      if (currentState != null && currentState.equals(ZkStateReader.ACTIVE)) {
        SolrQuery query = new SolrQuery("*:*");
        query.set("distrib", false);
        count = client.query(query).getResults().getNumFound();
      }
      
      if (verbose) System.out.println("client docs:" + count + "\n\n");
    }
    if (verbose) System.out.println("control docs:"
        + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound()
        + "\n\n");
    SolrQuery query = new SolrQuery("*:*");
    assertEquals("Doc Counts do not add up", controlCount,
        cloudClient.query(query).getResults().getNumFound());
  }
  
  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params)
      throws SolrServerException {
    
    if (r.nextBoolean()) params.set("collection", DEFAULT_COLLECTION);
    
    QueryResponse rsp = cloudClient.query(params);
    return rsp;
  }
  
  class StopableIndexingThread extends Thread {
    private volatile boolean stop = false;
    protected final int startI;
    protected final List<Integer> deletes = new ArrayList<Integer>();
    protected final AtomicInteger fails = new AtomicInteger();
    protected boolean doDeletes;
    
    public StopableIndexingThread(int startI, boolean doDeletes) {
      super("StopableIndexingThread");
      this.startI = startI;
      this.doDeletes = doDeletes;
      setDaemon(true);
    }
    
    @Override
    public void run() {
      int i = startI;
      int numDeletes = 0;
      int numAdds = 0;
      
      while (true && !stop) {
        ++i;
        
        if (doDeletes && random.nextBoolean() && deletes.size() > 0) {
          Integer delete = deletes.remove(0);
          try {
            numDeletes++;
            controlClient.deleteById(Integer.toString(delete));
            cloudClient.deleteById(Integer.toString(delete));
          } catch (Exception e) {
            System.err.println("REQUEST FAILED:");
            e.printStackTrace();
            fails.incrementAndGet();
          }
        }
        
        try {
          numAdds++;
          indexr(id, i, i1, 50, tlong, 50, t1,
              "to come to the aid of their country.");
        } catch (Exception e) {
          System.err.println("REQUEST FAILED:");
          e.printStackTrace();
          fails.incrementAndGet();
        }
        
        if (doDeletes && random.nextBoolean()) {
          deletes.add(i);
        }
        
      }
      
      System.err.println("added docs:" + numAdds + " with " + fails + " fails"
          + " deletes:" + numDeletes);
    }
    
    public void safeStop() {
      stop = true;
    }
    
    public int getFails() {
      return fails.get();
    }
    
  };
  
  @Override
  @After
  public void tearDown() throws Exception {
    if (VERBOSE) {
      super.printLayout();
    }
    ((CommonsHttpSolrServer) controlClient).shutdown();
    if (cloudClient != null) {
      cloudClient.close();
    }
    if (zkStateReader != null) {
      zkStateReader.close();
    }
    super.tearDown();
    
    System.clearProperty("zkHost");
  }
  
  protected void commit() throws Exception {
    controlClient.commit();
    cloudClient.commit();
  }
  
  protected void destroyServers() throws Exception {
    ChaosMonkey.stop(controlJetty);
    for (JettySolrRunner jetty : jettys) {
      try {
        ChaosMonkey.stop(jetty);
      } catch (Exception e) {
        log.error("", e);
      }
    }
    clients.clear();
    jettys.clear();
  }
  
  protected SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      String url = "http://localhost:" + port + context + "/"
          + DEFAULT_COLLECTION;
      CommonsHttpSolrServer s = new CommonsHttpSolrServer(url);
      s.setConnectionTimeout(100); // 1/10th sec
      s.setSoTimeout(45000);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
