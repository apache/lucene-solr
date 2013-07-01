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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.params.CoreConnectionPNames;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: we should still test this works as a custom update chain as well as
 * what we test now - the default update chain
 */
@Slow
public abstract class AbstractFullDistribZkTestBase extends AbstractDistribZkTestBase {
  static Logger log = LoggerFactory.getLogger(AbstractFullDistribZkTestBase.class);
  
  @BeforeClass
  public static void beforeFullSolrCloudTest() {
    // shorten the log output more for this test type
    if (formatter != null) formatter.setShorterFormat();
  }
  
  public static final String SHARD1 = "shard1";
  public static final String SHARD2 = "shard2";
  
  protected boolean printLayoutOnTearDown = false;
  
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

  protected CloudSolrServer controlClientCloud;  // cloud version of the control client
  protected volatile CloudSolrServer cloudClient;
  
  protected List<CloudJettyRunner> cloudJettys = new ArrayList<CloudJettyRunner>();
  protected Map<String,List<CloudJettyRunner>> shardToJetty = new HashMap<String,List<CloudJettyRunner>>();
  private AtomicInteger jettyIntCntr = new AtomicInteger(0);
  protected ChaosMonkey chaosMonkey;
  
  protected Map<String,CloudJettyRunner> shardToLeaderJetty = new HashMap<String,CloudJettyRunner>();
  private boolean cloudInit;
  protected boolean checkCreatedVsState;
  
  public static class CloudJettyRunner {
    public JettySolrRunner jetty;
    public String nodeName;
    public String coreNodeName;
    public String url;
    public CloudSolrServerClient client;
    public ZkNodeProps info;
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((url == null) ? 0 : url.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      CloudJettyRunner other = (CloudJettyRunner) obj;
      if (url == null) {
        if (other.url != null) return false;
      } else if (!url.equals(other.url)) return false;
      return true;
    }
    @Override
    public String toString() {
      return "CloudJettyRunner [url=" + url + "]";
    }
  }
  
  static class CloudSolrServerClient {
    SolrServer solrClient;
    String shardName;
    int port;
    public ZkNodeProps info;
    
    public CloudSolrServerClient() {}
    
    public CloudSolrServerClient(SolrServer client) {
      this.solrClient = client;
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((solrClient == null) ? 0 : solrClient.hashCode());
      return result;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      CloudSolrServerClient other = (CloudSolrServerClient) obj;
      if (solrClient == null) {
        if (other.solrClient != null) return false;
      } else if (!solrClient.equals(other.solrClient)) return false;
      return true;
    }
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // ignoreException(".*");
    if (sliceCount > 0) {
      System.setProperty("numShards", Integer.toString(sliceCount));
    } else {
      System.clearProperty("numShards");
    }
  }
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.update.delay", "0");
  }
  
  @AfterClass
  public static void afterClass() {
    System.clearProperty("solrcloud.update.delay");
    System.clearProperty("genericCoreNodeNames");
  }
  
  public AbstractFullDistribZkTestBase() {
    fixShardCount = true;
    
    shardCount = 4;
    sliceCount = 2;
    // TODO: for now, turn off stress because it uses regular clients, and we
    // need the cloud client because we kill servers
    stress = 0;
    
    useExplicitNodeNames = random().nextBoolean();
  }
  
  protected String getDataDir(String dataDir) throws IOException {
    return dataDir;
  }
  
  protected void initCloud() throws Exception {
    assert(cloudInit == false);
    cloudInit = true;
    try {
      cloudClient = createCloudClient(DEFAULT_COLLECTION);
      
      cloudClient.connect();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    
    chaosMonkey = new ChaosMonkey(zkServer, zkStateReader, DEFAULT_COLLECTION,
        shardToJetty, shardToLeaderJetty);
  }
  
  protected CloudSolrServer createCloudClient(String defaultCollection)
      throws MalformedURLException {
    CloudSolrServer server = new CloudSolrServer(zkServer.getZkAddress(), random().nextBoolean());
    if (defaultCollection != null) server.setDefaultCollection(defaultCollection);
    server.getLbServer().getHttpClient().getParams()
        .setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000);
    server.getLbServer().getHttpClient().getParams()
        .setParameter(CoreConnectionPNames.SO_TIMEOUT, 30000);
    return server;
  }
  
  @Override
  protected void createServers(int numServers) throws Exception {
    
    System.setProperty("collection", "control_collection");
    String numShards = System.getProperty(ZkStateReader.NUM_SHARDS_PROP);

    // we want hashes by default for the control, so set to 1 shard as opposed to leaving unset
    // System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
    System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "1");

    File controlJettyDir = new File(TEMP_DIR,
            getClass().getName() + "-controljetty-" + System.currentTimeMillis());
    setupJettySolrHome(controlJettyDir);

    controlJetty = createJetty(controlJettyDir, testDir + "/control/data");  // don't pass shard name... let it default to "shard1"
    System.clearProperty("collection");
    if(numShards != null) {
      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, numShards);
    } else {
      System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
    }
    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    if (sliceCount <= 0) {
      // for now, just create the cloud client for the control if we don't create the normal cloud client.
      // this can change if more tests need it.
      controlClientCloud = createCloudClient("control_collection");
      controlClientCloud.connect();
      waitForCollection(controlClientCloud.getZkStateReader(), "control_collection", 0);
      // NOTE: we are skipping creation of the chaos monkey by returning here
      cloudClient = controlClientCloud;  // temporary - some code needs/uses cloudClient
      return;
    }


    initCloud();
    
    createJettys(numServers, checkCreatedVsState).size();
    
    int cnt = getTotalReplicas(DEFAULT_COLLECTION);
    if (cnt > 0) {
      waitForCollection(cloudClient.getZkStateReader(), DEFAULT_COLLECTION, sliceCount);
    }

  }


  protected void waitForCollection(ZkStateReader reader, String collection, int slices) throws Exception {
    // wait until shards have started registering...
    int cnt = 30;
    while (!reader.getClusterState().getCollections().contains(collection)) {
      if (cnt == 0) {
        throw new RuntimeException("timeout waiting for collection in cluster state: collection=" + collection);
      }
      cnt--;
      Thread.sleep(500);
    }
    cnt = 30;
    while (reader.getClusterState().getSlices(collection).size() < slices) {
      if (cnt == 0) {
        throw new RuntimeException("timeout waiting for collection shards to come up: collection="+collection + "nSlices="+slices);
      }
      cnt--;
      Thread.sleep(500);
    }
  }
  
  protected List<JettySolrRunner> createJettys(int numJettys) throws Exception {
    return createJettys(numJettys, false);
  }
  

  /**
   * @param checkCreatedVsState
   *          if true, make sure the number created (numJettys) matches the
   *          number in the cluster state - if you add more jetties this may not
   *          be the case
   */
  protected List<JettySolrRunner> createJettys(int numJettys, boolean checkCreatedVsState) throws Exception {
    List<JettySolrRunner> jettys = new ArrayList<JettySolrRunner>();
    List<SolrServer> clients = new ArrayList<SolrServer>();
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numJettys; i++) {
      if (sb.length() > 0) sb.append(',');
      int cnt = this.jettyIntCntr.incrementAndGet();
      File jettyDir = new File(TEMP_DIR,
          getClass().getName() + "-jetty" + cnt + "-" + System.currentTimeMillis());
      jettyDir.mkdirs();
      setupJettySolrHome(jettyDir);
      JettySolrRunner j = createJetty(jettyDir, getDataDir(testDir + "/jetty"
          + cnt), null, "solrconfig.xml", null);
      jettys.add(j);
      SolrServer client = createNewSolrServer(j.getLocalPort());
      clients.add(client);
    }
  
    this.jettys.addAll(jettys);
    this.clients.addAll(clients);
    
    int numShards = getTotalReplicas(DEFAULT_COLLECTION);
    if (checkCreatedVsState) {
      // now wait until we see that the number of shards in the cluster state
      // matches what we expect
      int retries = 0;
      while (numShards != shardCount) {
        numShards = getTotalReplicas(DEFAULT_COLLECTION);
        if (numShards == shardCount) break;
        if (retries++ == 60) {
          printLayoutOnTearDown = true;
          fail("Shards in the state does not match what we set:" + numShards
              + " vs " + shardCount);
        }
        Thread.sleep(500);
      }

      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      // also make sure we have a leader for each shard
      for (int i = 1; i <= sliceCount; i++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + i, 10000);
      }
    }

    if (numShards > 0) {
      updateMappingsFromZk(this.jettys, this.clients);
    }
    
    // build the shard string
    for (int i = 1; i <= numJettys / 2; i++) {
      JettySolrRunner j = this.jettys.get(i);
      JettySolrRunner j2 = this.jettys.get(i + (numJettys / 2 - 1));
      if (sb.length() > 0) sb.append(',');
      sb.append("127.0.0.1:").append(j.getLocalPort()).append(context);
      sb.append("|127.0.0.1:").append(j2.getLocalPort()).append(context);
    }
    shards = sb.toString();
    
    return jettys;
  }


  protected SolrServer startCloudJetty(String collection, String shard) throws Exception {

    // TODO: use the collection string!!!!
    collection = DEFAULT_COLLECTION;

    int totalReplicas = getTotalReplicas(collection);


    int cnt = this.jettyIntCntr.incrementAndGet();
      File jettyDir = new File(TEMP_DIR,
          getClass().getName() + "-jetty" + cnt + "-" + System.currentTimeMillis());
      jettyDir.mkdirs();
      org.apache.commons.io.FileUtils.copyDirectory(new File(getSolrHome()), jettyDir);
      JettySolrRunner j = createJetty(jettyDir, testDir + "/jetty" + cnt, shard, "solrconfig.xml", null);
      jettys.add(j);
      SolrServer client = createNewSolrServer(j.getLocalPort());
      clients.add(client);

    int retries = 60;
    while (--retries >= 0) {
      // total replicas changed.. assume it was us
      if (getTotalReplicas(collection) != totalReplicas) {
       break;
      }
      Thread.sleep(500);
    }

    if (retries <= 0) {
      fail("Timeout waiting for " + j + " to appear in clusterstate");
      printLayout();
    }

    updateMappingsFromZk(this.jettys, this.clients);
    return client;
  }


  /* Total number of replicas (number of cores serving an index to the collection) shown by the cluster state */
  protected int getTotalReplicas(String collection) {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    DocCollection coll = zkStateReader.getClusterState().getCollectionStates().get(collection);
    if (coll == null) return 0;  // support for when collection hasn't been created yet
    int cnt = 0;
    for (Slice slices : coll.getSlices()) {
      cnt += slices.getReplicas().size();
    }
    return cnt;
  }
  
  public JettySolrRunner createJetty(String dataDir, String ulogDir, String shardList,
      String solrConfigOverride) throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(getSolrHome(), context, 0,
        solrConfigOverride, null, false, getExtraServlets());
    jetty.setShards(shardList);
    jetty.setDataDir(getDataDir(dataDir));
    jetty.start();
    
    return jetty;
  }
  
  public JettySolrRunner createJetty(File solrHome, String dataDir, String shardList, String solrConfigOverride, String schemaOverride) throws Exception {

    JettySolrRunner jetty = new JettySolrRunner(solrHome.getAbsolutePath(), context, 0, solrConfigOverride, schemaOverride, false, getExtraServlets());
    jetty.setShards(shardList);
    jetty.setDataDir(getDataDir(dataDir));
    jetty.start();
    
    return jetty;
  }
  
  protected void updateMappingsFromZk(List<JettySolrRunner> jettys,
      List<SolrServer> clients) throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.updateClusterState(true);
    cloudJettys.clear();
    shardToJetty.clear();
    
    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection coll = clusterState.getCollection(DEFAULT_COLLECTION);

    List<CloudSolrServerClient> theClients = new ArrayList<CloudSolrServerClient>();
    for (SolrServer client : clients) {
      // find info for this client in zk 
      nextClient:
      // we find out state by simply matching ports...
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          int port = new URI(((HttpSolrServer) client).getBaseURL())
              .getPort();
          
          if (replica.getNodeName().contains(":" + port + "_")) {
            CloudSolrServerClient csc = new CloudSolrServerClient();
            csc.solrClient = client;
            csc.port = port;
            csc.shardName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            csc.info = replica;
            
            theClients .add(csc);
            
            break nextClient;
          }
        }
      }
    }
 
    for (JettySolrRunner jetty : jettys) {
      int port = jetty.getLocalPort();
      if (port == -1) {
        throw new RuntimeException("Cannot find the port for jetty");
      }
      
      nextJetty:
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (replica.getNodeName().contains(":" + port + "_")) {
            List<CloudJettyRunner> list = shardToJetty.get(slice.getName());
            if (list == null) {
              list = new ArrayList<CloudJettyRunner>();
              shardToJetty.put(slice.getName(), list);
            }
            boolean isLeader = slice.getLeader() == replica;
            CloudJettyRunner cjr = new CloudJettyRunner();
            cjr.jetty = jetty;
            cjr.info = replica;
            cjr.nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            cjr.coreNodeName = replica.getNodeName();
            cjr.url = replica.getStr(ZkStateReader.BASE_URL_PROP) + "/" + replica.getStr(ZkStateReader.CORE_NAME_PROP);
            cjr.client = findClientByPort(port, theClients);
            list.add(cjr);
            if (isLeader) {
              shardToLeaderJetty.put(slice.getName(), cjr);
            }
            cloudJettys.add(cjr);
            break nextJetty;
          }
        }
      }
    }
    
    // # of jetties may not match replicas in shard here, because we don't map
    // jetties that are not running - every shard should have at least one
    // running jetty though
    for (Slice slice : coll.getSlices()) {
      // check that things look right
      List<CloudJettyRunner> jetties = shardToJetty.get(slice.getName());
      assertNotNull("Test setup problem: We found no jetties for shard: " + slice.getName()
          + " just:" + shardToJetty.keySet(), jetties);
      assertEquals("slice:" + slice.getName(), slice.getReplicas().size(), jetties.size());
    }
  }
  
  private CloudSolrServerClient findClientByPort(int port, List<CloudSolrServerClient> theClients) {
    for (CloudSolrServerClient client : theClients) {
      if (client.port == port) {
        return client;
      }
    }
    throw new IllegalArgumentException("Client with the given port does not exist:" + port);
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
  
  @Override
  protected void index_specific(int serverNumber, Object... fields)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);
    
    HttpSolrServer client = (HttpSolrServer) clients
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
  
  @Override
  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    cloudClient.deleteByQuery(q);

    /***
    for (SolrServer client : clients) {
      UpdateRequest ureq = new UpdateRequest();
      // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
      ureq.deleteByQuery(q).process(client);
    }
     ***/
  }// serial commit...
  
  protected void waitForRecoveriesToFinish(boolean verbose)
      throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    super.waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, verbose);
  }
  
  protected void waitForRecoveriesToFinish(String collection, boolean verbose)
      throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    super.waitForRecoveriesToFinish(collection, zkStateReader, verbose);
  }
  
  protected void waitForRecoveriesToFinish(boolean verbose, int timeoutSeconds)
      throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    super.waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, verbose, true, timeoutSeconds);
  }
  
  protected void checkQueries() throws Exception {

    handle.put("_version_", SKIPVAL);

    query("q", "*:*", "sort", "n_tl1 desc");

    handle.put("response", UNORDERED);  // get?ids=a,b,c requests are unordered
    String ids = "987654";
    for (int i=0; i<20; i++) {
      query("qt","/get", "id",Integer.toString(i));
      query("qt","/get", "ids",Integer.toString(i));
      ids = ids + ',' + Integer.toString(i);
      query("qt","/get", "ids",ids);
    }
    handle.remove("response");



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
  
  protected void indexAbunchOfDocs() throws Exception {
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
  
  /**
   * Executes a query against each live and active replica of the specified shard 
   * and aserts that the results are identical.
   *
   * @see #queryAndCompare
   */
  public QueryResponse queryAndCompareReplicas(SolrParams params, String shard) 
    throws Exception {

    ArrayList<SolrServer> shardClients = new ArrayList<SolrServer>(7);

    updateMappingsFromZk(jettys, clients);
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
    assertNotNull("no jetties found for shard: " + shard, solrJetties);


    for (CloudJettyRunner cjetty : solrJetties) {
      ZkNodeProps props = cjetty.info;
      String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      boolean active = props.getStr(ZkStateReader.STATE_PROP).equals(ZkStateReader.ACTIVE);
      boolean live = zkStateReader.getClusterState().liveNodesContain(nodeName);
      if (active && live) {
        shardClients.add(cjetty.client.solrClient);
      }
    }
    return queryAndCompare(params, shardClients);
  }

  /**
   * For each Shard, executes a query against each live and active replica of that shard
   * and asserts that the results are identical for each replica of the same shard.  
   * Because results are not compared between replicas of different shards, this method 
   * should be safe for comparing the results of any query, even if it contains 
   * "distrib=false", because the replicas should all be identical.
   *
   * @see AbstractFullDistribZkTestBase#queryAndCompareReplicas(SolrParams, String)
   */
  public void queryAndCompareShards(SolrParams params) throws Exception {

    updateMappingsFromZk(jettys, clients);
    List<String> shards = new ArrayList<String>(shardToJetty.keySet());
    for (String shard : shards) {
      queryAndCompareReplicas(params, shard);
    }
  }

  /** 
   * Returns a non-null string if replicas within the same shard do not have a 
   * consistent number of documents. 
   */
  protected void checkShardConsistency(String shard) throws Exception {
    checkShardConsistency(shard, false, false);
  }

  /** 
   * Returns a non-null string if replicas within the same shard do not have a 
   * consistent number of documents.
   * If expectFailure==false, the exact differences found will be logged since 
   * this would be an unexpected failure.
   * verbose causes extra debugging into to be displayed, even if everything is 
   * consistent.
   */
  protected String checkShardConsistency(String shard, boolean expectFailure, boolean verbose)
      throws Exception {
    
    List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
    if (solrJetties == null) {
      throw new RuntimeException("shard not found:" + shard + " keys:"
          + shardToJetty.keySet());
    }
    long num = -1;
    long lastNum = -1;
    String failMessage = null;
    if (verbose) System.err.println("check const of " + shard);
    int cnt = 0;
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    assertEquals(
        "The client count does not match up with the shard count for slice:"
            + shard,
        zkStateReader.getClusterState().getSlice(DEFAULT_COLLECTION, shard)
            .getReplicasMap().size(), solrJetties.size());

    CloudJettyRunner lastJetty = null;
    for (CloudJettyRunner cjetty : solrJetties) {
      ZkNodeProps props = cjetty.info;
      if (verbose) System.err.println("client" + cnt++);
      if (verbose) System.err.println("PROPS:" + props);

      try {
        SolrParams query = params("q","*:*", "rows","0", "distrib","false", "tests","checkShardConsistency"); // "tests" is just a tag that won't do anything except be echoed in logs
        num = cjetty.client.solrClient.query(query).getResults().getNumFound();
      } catch (SolrServerException e) {
        if (verbose) System.err.println("error contacting client: "
            + e.getMessage() + "\n");
        continue;
      } catch (SolrException e) {
        if (verbose) System.err.println("error contacting client: "
            + e.getMessage() + "\n");
        continue;
      }
      
      boolean live = false;
      String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      if (zkStateReader.getClusterState().liveNodesContain(nodeName)) {
        live = true;
      }
      if (verbose) System.err.println(" live:" + live);
      if (verbose) System.err.println(" num:" + num + "\n");

      boolean active = props.getStr(ZkStateReader.STATE_PROP).equals(
          ZkStateReader.ACTIVE);
      if (active && live) {
        if (lastNum > -1 && lastNum != num && failMessage == null) {
          failMessage = shard + " is not consistent.  Got " + lastNum + " from " + lastJetty.url + "lastClient"
              + " and got " + num + " from " + cjetty.url;

          if (!expectFailure || verbose) {
            System.err.println("######" + failMessage);
            SolrQuery query = new SolrQuery("*:*");
            query.set("distrib", false);
            query.set("fl","id,_version_");
            query.set("rows","100000");
            query.set("sort","id asc");
            query.set("tests","checkShardConsistency/showDiff");

            SolrDocumentList lst1 = lastJetty.client.solrClient.query(query).getResults();
            SolrDocumentList lst2 = cjetty.client.solrClient.query(query).getResults();

            showDiff(lst1, lst2, lastJetty.url, cjetty.url);
          }

        }
        lastNum = num;
        lastJetty = cjetty;
      }
    }
    return failMessage;
    
  }
  
  public void showCounts() {
    Set<String> theShards = shardToJetty.keySet();
    
    for (String shard : theShards) {
      List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
      
      for (CloudJettyRunner cjetty : solrJetties) {
        ZkNodeProps props = cjetty.info;
        System.err.println("PROPS:" + props);
        
        try {
          SolrParams query = params("q", "*:*", "rows", "0", "distrib",
              "false", "tests", "checkShardConsistency"); // "tests" is just a
                                                          // tag that won't do
                                                          // anything except be
                                                          // echoed in logs
          long num = cjetty.client.solrClient.query(query).getResults()
              .getNumFound();
          System.err.println("DOCS:" + num);
        } catch (SolrServerException e) {
          System.err.println("error contacting client: " + e.getMessage()
              + "\n");
          continue;
        } catch (SolrException e) {
          System.err.println("error contacting client: " + e.getMessage()
              + "\n");
          continue;
        }
        boolean live = false;
        String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
        ZkStateReader zkStateReader = cloudClient.getZkStateReader();
        if (zkStateReader.getClusterState().liveNodesContain(nodeName)) {
          live = true;
        }
        System.err.println(" live:" + live);
        
      }
    }
  }
  
  private String toStr(SolrDocumentList lst, int maxSz) {
    if (lst.size() <= maxSz) return lst.toString();

    StringBuilder sb = new StringBuilder("SolrDocumentList[sz=" + lst.size());
    if (lst.size() != lst.getNumFound()) {
      sb.append(" numFound=" + lst.getNumFound());
    }
    sb.append("]=");
    sb.append(lst.subList(0,maxSz/2).toString());
    sb.append(" , [...] , ");
    sb.append(lst.subList(lst.size()-maxSz/2, lst.size()).toString());

    return sb.toString();
  }

  Set<Map> showDiff(SolrDocumentList a, SolrDocumentList b, String aName, String bName) {
    System.err.println("######"+aName+ ": " + toStr(a,10));
    System.err.println("######"+bName+ ": " + toStr(b,10));
    System.err.println("###### sizes=" + a.size() + "," + b.size());
    
    Set<Map> setA = new HashSet<Map>();
    for (SolrDocument sdoc : a) {
      setA.add(new HashMap(sdoc));
    }

    Set<Map> setB = new HashSet<Map>();
    for (SolrDocument sdoc : b) {
      setB.add(new HashMap(sdoc));
    }

    Set<Map> onlyInA = new HashSet<Map>(setA);
    onlyInA.removeAll(setB);
    Set<Map> onlyInB = new HashSet<Map>(setB);
    onlyInB.removeAll(setA);

    if (onlyInA.size() > 0) {
      System.err.println("###### Only in " + aName + ": " + onlyInA);
    }
    if (onlyInB.size() > 0) {
      System.err.println("###### Only in " + bName + ": " + onlyInB);
    }

    onlyInA.addAll(onlyInB);
    return onlyInA;
  }

  /* Checks both shard replcia consistency and against the control shard.
  * The test will be failed if differences are found.
  */
  protected void checkShardConsistency() throws Exception {
    checkShardConsistency(true, false);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(boolean checkVsControl, boolean verbose)
      throws Exception {

    updateMappingsFromZk(jettys, clients);
    
    Set<String> theShards = shardToJetty.keySet();
    String failMessage = null;
    for (String shard : theShards) {
      String shardFailMessage = checkShardConsistency(shard, false, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }
    
    if (failMessage != null) {
      fail(failMessage);
    }

    if (!checkVsControl) return;

    SolrParams q = params("q","*:*","rows","0", "tests","checkShardConsistency(vsControl)");    // add a tag to aid in debugging via logs

    SolrDocumentList controlDocList = controlClient.query(q).getResults();
    long controlDocs = controlDocList.getNumFound();

    SolrDocumentList cloudDocList = cloudClient.query(q).getResults();
    long cloudClientDocs = cloudDocList.getNumFound();




    // now check that the right # are on each shard
    theShards = shardToJetty.keySet();
    int cnt = 0;
    for (String s : theShards) {
      int times = shardToJetty.get(s).size();
      for (int i = 0; i < times; i++) {
        try {
          CloudJettyRunner cjetty = shardToJetty.get(s).get(i);
          ZkNodeProps props = cjetty.info;
          SolrServer client = cjetty.client.solrClient;
          boolean active = props.getStr(ZkStateReader.STATE_PROP).equals(
              ZkStateReader.ACTIVE);
          if (active) {
            SolrQuery query = new SolrQuery("*:*");
            query.set("distrib", false);
            long results = client.query(query).getResults().getNumFound();
            if (verbose) System.err.println(new ZkCoreNodeProps(props)
                .getCoreUrl() + " : " + results);
            if (verbose) System.err.println("shard:"
                + props.getStr(ZkStateReader.SHARD_ID_PROP));
            cnt += results;
            break;
          }
        } catch (Exception e) {
          // if we have a problem, try the next one
          if (i == times - 1) {
            throw e;
          }
        }
      }
    }


    if (controlDocs != cnt || cloudClientDocs != controlDocs) {
      String msg = "document count mismatch.  control=" + controlDocs + " sum(shards)="+ cnt + " cloudClient="+cloudClientDocs;
      log.error(msg);

      // re-execute the query getting ids
      q = params("q","*:*","rows","100000", "fl","id", "tests","checkShardConsistency(vsControl)/getIds");    // add a tag to aid in debugging via logs
      controlDocList = controlClient.query(q).getResults();
      if (controlDocs != controlDocList.getNumFound()) {
        log.error("Something changed! control now " + controlDocList.getNumFound());
      };

      cloudDocList = cloudClient.query(q).getResults();
      if (cloudClientDocs != cloudDocList.getNumFound()) {
        log.error("Something changed! cloudClient now " + cloudDocList.getNumFound());
      };

      Set<Map> differences = showDiff(controlDocList, cloudDocList,"controlDocList","cloudDocList");

      // get versions for the mismatched ids
      boolean foundId = false;
      StringBuilder ids = new StringBuilder("id:(");
      for (Map doc : differences) {
        ids.append(" "+doc.get("id"));
        foundId = true;
      }
      ids.append(")");
      
      if (foundId) {
        // get versions for those ids that don't match
        q = params("q", ids.toString(), "rows", "100000", "fl", "id,_version_",
            "sort", "id asc", "tests",
            "checkShardConsistency(vsControl)/getVers"); // add a tag to aid in
                                                         // debugging via logs
        
        SolrDocumentList a = controlClient.query(q).getResults();
        SolrDocumentList b = cloudClient.query(q).getResults();
        
        log.error("controlClient :" + a + "\n\tcloudClient :" + b);
      }

      fail(msg);
    }
  }
  
  protected SolrServer getClient(String nodeName) {
    for (CloudJettyRunner cjetty : cloudJettys) {
      CloudSolrServerClient client = cjetty.client;
      if (client.shardName.equals(nodeName)) {
        return client.solrClient;
      }
    }
    return null;
  }
  
  protected void assertDocCounts(boolean verbose) throws Exception {
    // TODO: as we create the clients, we should build a map from shard to
    // node/client
    // and node/client to shard?
    if (verbose) System.err.println("control docs:"
        + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound()
        + "\n\n");
    long controlCount = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    
    // do some really inefficient mapping...
    ZkStateReader zk = new ZkStateReader(zkServer.getZkAddress(), 10000,
        AbstractZkTestCase.TIMEOUT);
    Map<String,Slice> slices = null;
    ClusterState clusterState;
    try {
      zk.createClusterStateWatchersAndUpdate();
      clusterState = zk.getClusterState();
      slices = clusterState.getSlicesMap(DEFAULT_COLLECTION);
    } finally {
      zk.close();
    }
    
    if (slices == null) {
      throw new RuntimeException("Could not find collection "
          + DEFAULT_COLLECTION + " in " + clusterState.getCollections());
    }
    
    for (CloudJettyRunner cjetty : cloudJettys) {
      CloudSolrServerClient client = cjetty.client;
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,Replica> theShards = slice.getValue().getReplicasMap();
        for (Map.Entry<String,Replica> shard : theShards.entrySet()) {
          String shardName = new URI(
              ((HttpSolrServer) client.solrClient).getBaseURL()).getPort()
              + "_solr_";
          if (verbose && shard.getKey().endsWith(shardName)) {
            System.err.println("shard:" + slice.getKey());
            System.err.println(shard.getValue());
          }
        }
      }
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      long count = 0;
      String currentState = cjetty.info.getStr(ZkStateReader.STATE_PROP);
      if (currentState != null
          && currentState.equals(ZkStateReader.ACTIVE)
          && zkStateReader.getClusterState().liveNodesContain(
              cjetty.info.getStr(ZkStateReader.NODE_NAME_PROP))) {
        SolrQuery query = new SolrQuery("*:*");
        query.set("distrib", false);
        count = client.solrClient.query(query).getResults().getNumFound();
      }
      
      if (verbose) System.err.println("client docs:" + count + "\n\n");
    }
    if (verbose) System.err.println("control docs:"
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
  
  abstract class StopableThread extends Thread {
    public StopableThread(String name) {
      super(name);
    }
    public abstract void safeStop();
  }
  
  class StopableIndexingThread extends StopableThread {
    private volatile boolean stop = false;
    protected final int startI;
    protected final List<Integer> deletes = new ArrayList<Integer>();
    protected final AtomicInteger fails = new AtomicInteger();
    protected boolean doDeletes;
    private int numCycles;
    
    public StopableIndexingThread(int startI, boolean doDeletes) {
      this(startI, doDeletes, -1);
    }
    
    public StopableIndexingThread(int startI, boolean doDeletes, int numCycles) {
      super("StopableIndexingThread");
      this.startI = startI;
      this.doDeletes = doDeletes;
      this.numCycles = numCycles;
      setDaemon(true);
    }
    
    @Override
    public void run() {
      int i = startI;
      int numDone = 0;
      int numDeletes = 0;
      int numAdds = 0;
      
      while (true && !stop) {
        if (numCycles != -1) {
          if (numDone > numCycles) {
            break;
          }
        }
        ++numDone;
        ++i;
        boolean addFailed = false;
        
        if (doDeletes && random().nextBoolean() && deletes.size() > 0) {
          Integer delete = deletes.remove(0);
          try {
            numDeletes++;
            controlClient.deleteById(Integer.toString(delete));
            cloudClient.deleteById(Integer.toString(delete));
          } catch (Exception e) {
            System.err.println("REQUEST FAILED:");
            e.printStackTrace();
            if (e instanceof SolrServerException) {
              System.err.println("ROOT CAUSE:");
              ((SolrServerException) e).getRootCause().printStackTrace();
            }
            fails.incrementAndGet();
          }
        }
        
        try {
          numAdds++;
          indexr(id, i, i1, 50, tlong, 50, t1,
              "to come to the aid of their country.");
        } catch (Exception e) {
          addFailed = true;
          System.err.println("REQUEST FAILED:");
          e.printStackTrace();
          if (e instanceof SolrServerException) {
            System.err.println("ROOT CAUSE:");
            ((SolrServerException) e).getRootCause().printStackTrace();
          }
          fails.incrementAndGet();
        }
        
        if (!addFailed && doDeletes && random().nextBoolean()) {
          deletes.add(i);
        }
        
        try {
          Thread.currentThread().sleep(random().nextInt(100));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      
      System.err.println("added docs:" + numAdds + " with " + fails + " fails"
          + " deletes:" + numDeletes);
    }
    
    @Override
    public void safeStop() {
      System.out.println("safe stop:");
      stop = true;
    }
    
    public int getFails() {
      return fails.get();
    }
    
  };
  
  class StopableSearchThread extends StopableThread {
    private volatile boolean stop = false;
    protected final AtomicInteger fails = new AtomicInteger();
    private String[] QUERIES = new String[] {"to come","their country","aid","co*"};
    
    public StopableSearchThread() {
      super("StopableSearchThread");
      setDaemon(true);
    }
    
    @Override
    public void run() {
      Random random = random();
      int numSearches = 0;
      
      while (true && !stop) {
        numSearches++;
        try {
          //to come to the aid of their country.
          cloudClient.query(new SolrQuery(QUERIES[random.nextInt(QUERIES.length)]));
        } catch (Exception e) {
          System.err.println("QUERY REQUEST FAILED:");
          e.printStackTrace();
          if (e instanceof SolrServerException) {
            System.err.println("ROOT CAUSE:");
            ((SolrServerException) e).getRootCause().printStackTrace();
          }
          fails.incrementAndGet();
        }
        try {
          Thread.sleep(random.nextInt(4000) + 300);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      
      System.err.println("num searches done:" + numSearches + " with " + fails + " fails");
    }
    
    @Override
    public void safeStop() {
      stop = true;
    }
    
    public int getFails() {
      return fails.get();
    }
    
  };
  
  public void waitForThingsToLevelOut(int waitForRecTimeSeconds) throws Exception {
    log.info("Wait for recoveries to finish - wait " + waitForRecTimeSeconds + " for each attempt");
    int cnt = 0;
    boolean retry = false;
    do {
      waitForRecoveriesToFinish(VERBOSE, waitForRecTimeSeconds);
      
      try {
        commit();
      } catch (Throwable t) {
        t.printStackTrace();
        // we don't care if this commit fails on some nodes
      }
      
      updateMappingsFromZk(jettys, clients);
      
      Set<String> theShards = shardToJetty.keySet();
      String failMessage = null;
      for (String shard : theShards) {
        failMessage = checkShardConsistency(shard, true, false);
      }
      
      if (failMessage != null) {
        retry  = true;
      }
      cnt++;
      if (cnt > 20) break;
      Thread.sleep(2000);
    } while (retry);
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    if (VERBOSE || printLayoutOnTearDown) {
      super.printLayout();
    }
    if (commondCloudSolrServer != null) {
      commondCloudSolrServer.shutdown();
    }
    if (controlClient != null) {
      ((HttpSolrServer) controlClient).shutdown();
    }
    if (cloudClient != null) {
      cloudClient.shutdown();
    }
    if (controlClientCloud != null) {
      controlClientCloud.shutdown();
    }
    super.tearDown();
    
    System.clearProperty("zkHost");
    System.clearProperty("numShards");
  }
  
  @Override
  protected void commit() throws Exception {
    controlClient.commit();
    cloudClient.commit();
  }
  
  @Override
  protected void destroyServers() throws Exception {
    if (controlJetty != null) {
      ChaosMonkey.stop(controlJetty);
    }
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
  
  protected void createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode) throws SolrServerException, IOException {
    createCollection(null, collectionName, numShards, numReplicas, maxShardsPerNode, null, null);
  }
  
  protected void createCollection(Map<String,List<Integer>> collectionInfos,
      String collectionName, int numShards, int numReplicas, int maxShardsPerNode, SolrServer client, String createNodeSetStr) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set(OverseerCollectionProcessor.NUM_SLICES, numShards);
    params.set(OverseerCollectionProcessor.REPLICATION_FACTOR, numReplicas);
    params.set(OverseerCollectionProcessor.MAX_SHARDS_PER_NODE, maxShardsPerNode);
    if (createNodeSetStr != null) params.set(OverseerCollectionProcessor.CREATE_NODE_SET, createNodeSetStr);

    int clientIndex = random().nextInt(2);
    List<Integer> list = new ArrayList<Integer>();
    list.add(numShards);
    list.add(numReplicas);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
  
    if (client == null) {
      final String baseUrl = getBaseUrl((HttpSolrServer) clients.get(clientIndex));
      
      createNewSolrServer("", baseUrl).request(request);
    } else {
      client.request(request);
    }
  }
  
  @Override
  protected SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      String url = "http://127.0.0.1:" + port + context + 
        (context.endsWith("/") ? "" : "/") + DEFAULT_COLLECTION;
      HttpSolrServer s = new HttpSolrServer(url);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      s.setSoTimeout(60000);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected SolrServer createNewSolrServer(String collection, String baseUrl) {
    try {
      // setup the server...
      HttpSolrServer s = new HttpSolrServer(baseUrl + "/" + collection);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected String getBaseUrl(HttpSolrServer client) {
    return client .getBaseURL().substring(
        0, client.getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
  }
  
  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }

  private String checkCollectionExpectations(String collectionName, List<Integer> numShardsNumReplicaList, List<String> nodesAllowedToRunShards) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    
    int expectedSlices = numShardsNumReplicaList.get(0);
    // The Math.min thing is here, because we expect replication-factor to be reduced to if there are not enough live nodes to spread all shards of a collection over different nodes
    int expectedShardsPerSlice = numShardsNumReplicaList.get(1);
    int expectedTotalShards = expectedSlices * expectedShardsPerSlice;
    
      Map<String,DocCollection> collections = clusterState
          .getCollectionStates();
      if (collections.containsKey(collectionName)) {
        Map<String,Slice> slices = collections.get(collectionName).getSlicesMap();
        // did we find expectedSlices slices/shards?
      if (slices.size() != expectedSlices) {
        return "Found new collection " + collectionName + ", but mismatch on number of slices. Expected: " + expectedSlices + ", actual: " + slices.size();
      }
      int totalShards = 0;
      for (String sliceName : slices.keySet()) {
        for (Replica replica : slices.get(sliceName).getReplicas()) {
          if (nodesAllowedToRunShards != null && !nodesAllowedToRunShards.contains(replica.getStr(ZkStateReader.NODE_NAME_PROP))) {
            return "Shard " + replica.getName() + " created on node " + replica.getNodeName() + " not allowed to run shards for the created collection " + collectionName;
          }
        }
        totalShards += slices.get(sliceName).getReplicas().size();
      }
      if (totalShards != expectedTotalShards) {
        return "Found new collection " + collectionName + " with correct number of slices, but mismatch on number of shards. Expected: " + expectedTotalShards + ", actual: " + totalShards; 
        }
      return null;
    } else {
      return "Could not find new collection " + collectionName;
    }
  }
  
  protected void checkForCollection(String collectionName,
      List<Integer> numShardsNumReplicaList,
      List<String> nodesAllowedToRunShards) throws Exception {
    // check for an expectedSlices new collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 120000;
    boolean success = false;
    String checkResult = "Didnt get to perform a single check";
    while (System.currentTimeMillis() < timeoutAt) {
      checkResult = checkCollectionExpectations(collectionName,
          numShardsNumReplicaList, nodesAllowedToRunShards);
      if (checkResult == null) {
        success = true;
        break;
      }
      Thread.sleep(500);
    }
    if (!success) {
      super.printLayout();
      fail(checkResult);
    }
  }
  
  volatile CloudSolrServer commondCloudSolrServer;
  protected CloudSolrServer getCommonCloudSolrServer() {
    if (commondCloudSolrServer == null) {
      synchronized(this) {
        try {
          commondCloudSolrServer = new CloudSolrServer(zkServer.getZkAddress(), random().nextBoolean());
          commondCloudSolrServer.setDefaultCollection(DEFAULT_COLLECTION);
          commondCloudSolrServer.connect();
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return commondCloudSolrServer;
  }

}
