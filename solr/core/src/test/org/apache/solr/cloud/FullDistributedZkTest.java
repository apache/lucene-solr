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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.solr.cloud.RecoveryStrat.RecoveryListener;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 *
 */
@Ignore("Trying to figure out an issue")
public class FullDistributedZkTest extends AbstractDistributedZkTestCase {
  
  static final String DISTRIB_UPDATE_CHAIN = "distrib-update-chain";

  private static final String DEFAULT_COLLECTION = "collection1";

  String t1="a_t";
  String i1="a_si";
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
  
  String oddField="oddField_s";
  String missingField="ignore_exception__missing_but_valid_field_t";
  String invalidField="ignore_exception__invalid_field_not_in_schema";
  protected int sliceCount;
  
  protected volatile CloudSolrServer cloudClient;
  
  protected Map<JettySolrRunner,ZkNodeProps> jettyToInfo = new HashMap<JettySolrRunner,ZkNodeProps>();
  protected Map<SolrServer,ZkNodeProps> clientToInfo = new HashMap<SolrServer,ZkNodeProps>();
  protected Map<String,List<SolrServer>> shardToClient = new HashMap<String,List<SolrServer>>();
  protected Map<String,List<CloudJettyRunner>> shardToJetty = new HashMap<String,List<CloudJettyRunner>>();
  private AtomicInteger i = new AtomicInteger(0);
  protected ChaosMonkey chaosMonkey;
  private volatile ZkStateReader zkStateReader;


  
  class CloudJettyRunner {
    JettySolrRunner jetty;
    String shardName;
  }
  
  class ChaosMonkey {
    private Map<String,List<CloudJettyRunner>> shardToJetty;

    public ChaosMonkey(Map<String,List<CloudJettyRunner>> shardToJetty) {
      this.shardToJetty = shardToJetty;
    }
    
    public void expireSession(CloudJettyRunner cloudJetty) {
      SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) cloudJetty.jetty.getDispatchFilter().getFilter();
      long sessionId = solrDispatchFilter.getCores().getZkController().getZkClient().getSolrZooKeeper().getSessionId();
      zkServer.expire(sessionId);
    }
    
    public JettySolrRunner killShard(String slice, int index) throws Exception {
      JettySolrRunner jetty = shardToJetty.get(slice).get(index).jetty;
      // get a clean shutdown so that no dirs are left open...
      ((SolrDispatchFilter)jetty.getDispatchFilter().getFilter()).destroy();
      jetty.stop();
      return jetty;
    }
    
    public JettySolrRunner getShard(String slice, int index) throws Exception {
      JettySolrRunner jetty = shardToJetty.get(slice).get(index).jetty;
      return jetty;
    }
    
    public JettySolrRunner killRandomShard() throws Exception {
      // add all the shards to a list
//      CloudState clusterState = zk.getCloudState();
//      for (String collection : collections)   {
//      Slice theShards = zk.getCloudState().getSlices(collection);
      return null;
    }
    
    public JettySolrRunner killRandomShard(String slice) throws Exception {
      // get latest cloud state
      zkStateReader.updateCloudState(true);
      Slice theShards = zkStateReader.getCloudState().getSlices(DEFAULT_COLLECTION)
          .get(slice);
      int numRunning = 0;
      
      for (CloudJettyRunner cloudJetty : shardToJetty.get(slice)) {
        boolean running = true;
        
        ZkNodeProps props = theShards.getShards().get(cloudJetty.shardName);
        String state = props.get(ZkStateReader.STATE_PROP);
        String nodeName = props.get(ZkStateReader.NODE_NAME_PROP);
        
        if (!cloudJetty.jetty.isRunning()
            || state.equals(ZkStateReader.RECOVERING)
            || !zkStateReader.getCloudState().liveNodesContain(nodeName)) {
          running = false;
        }
        
        if (running) {
          numRunning++;
        }
      }
      
      if (numRunning < 2) {
        // we cannot kill anyone
        return null;
      }
      
      // kill random shard in shard2
      List<CloudJettyRunner> jetties = shardToJetty.get(slice);
      int index = random.nextInt(jetties.size() - 1);
      JettySolrRunner jetty = jetties.get(index).jetty;
      jetty.stop();
      return jetty;
    }
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("CLOUD_UPDATE_DELAY", "0");
    
    System.setProperty("remove.version.field", "true");
  }
  
  public FullDistributedZkTest() {
    fixShardCount = true;
    
    shardCount = 6;
    sliceCount = 3;
    // TODO: for now, turn off stress because it uses regular clients, and we 
    // need the cloud client because we kill servers
    stress = 0;
    chaosMonkey = new ChaosMonkey(shardToJetty);

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
    }
    
    // wait until shards have started registering...
    while(!zkStateReader.getCloudState().getCollections().contains(DEFAULT_COLLECTION)) {
      Thread.sleep(500);
    }
    while(zkStateReader.getCloudState().getSlices(DEFAULT_COLLECTION).size() != sliceCount) {
      Thread.sleep(500);
    }
    
    // use the distributed solrj client
    if (cloudClient == null) {
      synchronized(this) {
        if (cloudClient != null) {
          return;
        }
        try {
          CloudSolrServer server = new CloudSolrServer(zkServer.getZkAddress());
          server.setDefaultCollection(DEFAULT_COLLECTION);
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
    controlJetty = createJetty(testDir, testDir + "/control/data", "control_shard");
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
      JettySolrRunner j = createJetty(testDir, testDir + "/jetty" + this.i.incrementAndGet(), null, "solrconfig-distrib-update.xml");
      jettys.add(j);
      SolrServer client = createNewSolrServer(j.getLocalPort());
      clients.add(client);
    }

    initCloud();
    updateMappingsFromZk(jettys, clients);
    
    this.jettys.addAll(jettys);
    this.clients.addAll(clients);
    // build the shard string
    for (int i = 1; i <= numJettys/2; i++) {
      JettySolrRunner j = this.jettys.get(i);
      JettySolrRunner j2 = this.jettys.get(i + (numJettys/2 - 1));
      if (sb.length() > 0) sb.append(',');
      sb.append("localhost:").append(j.getLocalPort()).append(context);
      sb.append("|localhost:").append(j2.getLocalPort()).append(context);
    }
    shards = sb.toString();
    return jettys;
  }

  protected void updateMappingsFromZk(List<JettySolrRunner> jettys,
      List<SolrServer> clients) throws Exception,
      IOException, KeeperException, URISyntaxException {
    zkStateReader.updateCloudState(true);
    
    while(!zkStateReader.getCloudState().getCollections().contains(DEFAULT_COLLECTION)) {
      Thread.sleep(500);
    }
    while(zkStateReader.getCloudState().getSlices(DEFAULT_COLLECTION).size() != sliceCount) {
      Thread.sleep(500);
    }
    
    for (SolrServer client : clients) {
      // find info for this client in zk

      CloudState cloudState = zkStateReader.getCloudState();
      Map<String,Slice> slices = cloudState.getSlices(
          DEFAULT_COLLECTION);
      
      if (slices == null) {
        throw new RuntimeException("No slices found for collection " + DEFAULT_COLLECTION + " in " + cloudState.getCollections());
      }
      
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          String shardName = new URI(
              ((CommonsHttpSolrServer) client).getBaseURL()).getPort()
              + "_solr_";
          if (shard.getKey().endsWith(shardName)) {
            clientToInfo.put(client, shard.getValue());
            List<SolrServer> list = shardToClient.get(slice.getKey());
            if (list == null) {
              list = new ArrayList<SolrServer>();
              shardToClient.put(slice.getKey(), list);
            }
            list.add(client);
          }
        }
      }
      
    }
    
    Map<String,Slice> slices = zkStateReader.getCloudState().getSlices(
        DEFAULT_COLLECTION);
    
    for (JettySolrRunner jetty : jettys) {

      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          String shardName = jetty.getLocalPort() + "_solr_";
          if (shard.getKey().endsWith(shardName)) {
            jettyToInfo.put(jetty, shard.getValue());
            List<CloudJettyRunner> list = shardToJetty.get(slice.getKey());
            if (list == null) {
              list = new ArrayList<CloudJettyRunner>();
              shardToJetty.put(slice.getKey(), list);
            }
            CloudJettyRunner cjr = new CloudJettyRunner();
            cjr.jetty = jetty;
            cjr.shardName = shardName;
            list.add(cjr);
          }
        }
      }
    }
  }
  
  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {

    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
      params.set("distrib", "true");
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < sliceCount ; i++) {
        if (i > 0)
          sb.append(',');
        sb.append("shard" + (i+1));
      }
      params.set("shards", sb.toString());
      params.set("distrib", "true");
    }
  }
  
  @Override
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    controlClient.add(doc);
 
    boolean pick = random.nextBoolean();
    
    int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) % sliceCount;
    
    if (pick && sliceCount > 1) {
      which = which + ((shardCount / sliceCount) * random.nextInt(sliceCount-1));
    }
    
    CommonsHttpSolrServer client = (CommonsHttpSolrServer) clients.get(which);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
  }
  
  protected void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);

    CommonsHttpSolrServer client = (CommonsHttpSolrServer) clients.get(serverNumber);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
  }
  
  protected void index_specific(SolrServer client, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
    
    // add to control second in case adding to shards fails
    controlClient.add(doc);
  }
  
  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    for (SolrServer client : clients) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
      ureq.deleteByQuery(q).process(client);
    }
  }// serial commit...
  
  /* (non-Javadoc)
   * @see org.apache.solr.BaseDistributedSearchTestCase#doTest()
   * 
   * Create 3 shards, each with one replica
   */
  @Override
  public void doTest() throws Exception {
    // TODO: remove the need for this...
    //Thread.sleep(1000);
    //pause for cloud state to be updated with latest...
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    del("*:*");
    
    indexr(id,1, i1, 100, tlong, 100,t1,"now is the time for all good men"
            ,"foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
    
    commit();
    
    assertDocCounts();
    
    indexr(id,2, i1, 50 , tlong, 50,t1,"to come to the aid of their country."
    );
    indexr(id,3, i1, 2, tlong, 2,t1,"how now brown cow"
    );
    indexr(id,4, i1, -100 ,tlong, 101,t1,"the quick fox jumped over the lazy dog"
    );
    indexr(id,5, i1, 500, tlong, 500 ,t1,"the quick fox jumped way over the lazy dog"
    );
    indexr(id,6, i1, -600, tlong, 600 ,t1,"humpty dumpy sat on a wall");
    indexr(id,7, i1, 123, tlong, 123 ,t1,"humpty dumpy had a great fall");
    indexr(id,8, i1, 876, tlong, 876,t1,"all the kings horses and all the kings men");
    indexr(id,9, i1, 7, tlong, 7,t1,"couldn't put humpty together again");
    indexr(id,10, i1, 4321, tlong, 4321,t1,"this too shall pass");
    indexr(id,11, i1, -987, tlong, 987,t1,"An eye for eye only ends up making the whole world blind.");
    indexr(id,12, i1, 379, tlong, 379,t1,"Great works are performed, not by strength, but by perseverance.");
    indexr(id,13, i1, 232, tlong, 232,t1,"no eggs on wall, lesson learned", oddField, "odd man out");

    indexr(id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    String[] vals = new String[100];
    for (int i=0; i<100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);

    for (int i=100; i<150; i++) {
      indexr(id, i);      
    }

    commit();
    
    assertDocCounts();
    query("q", "*:*", "sort", "n_tl1 desc");

    // random value sort
    for (String f : fieldNames) {
      query("q","*:*", "sort",f+" desc");
      query("q","*:*", "sort",f+" asc");
    }

    // these queries should be exactly ordered and scores should exactly match
    query("q","*:*", "sort",i1+" desc");
    query("q","*:*", "sort",i1+" asc");
    query("q","*:*", "sort",i1+" desc", "fl","*,score");
    query("q","*:*", "sort","n_tl1 asc", "fl","score");  // test legacy behavior - "score"=="*,score"
    query("q","*:*", "sort","n_tl1 desc");
    handle.put("maxScore", SKIPVAL);
    query("q","{!func}"+i1);// does not expect maxScore. So if it comes ,ignore it. JavaBinCodec.writeSolrDocumentList()
    //is agnostic of request params.
    handle.remove("maxScore");
    query("q","{!func}"+i1, "fl","*,score");  // even scores should match exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query("q","quick");
    query("q","all","fl","id","start","0");
    query("q","all","fl","foofoofoo","start","0");  // no fields in returned docs
    query("q","all","fl","id","start","100");

    handle.put("score", SKIPVAL);
    query("q","quick","fl","*,score");
    query("q","all","fl","*,score","start","1");
    query("q","all","fl","*,score","start","100");

    query("q","now their fox sat had put","fl","*,score",
            "hl","true","hl.fl",t1);

    query("q","now their fox sat had put","fl","foofoofoo",
            "hl","true","hl.fl",t1);

    query("q","matchesnothing","fl","*,score");  

    query("q","*:*", "rows",100, "facet","true", "facet.field",t1);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","count");
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","count", "facet.mincount",2);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","index");
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","index", "facet.mincount",2);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1,"facet.limit",1);
    query("q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*");
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.offset",1);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.mincount",2);

    // test faceting multiple things at once
    query("q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"
    ,"facet.field",t1);

    // test filter tagging, facet exclusion, and naming (multi-select facet support)
    query("q","*:*", "rows",100, "facet","true", "facet.query","{!key=myquick}quick", "facet.query","{!key=myall ex=a}all", "facet.query","*:*"
    ,"facet.field","{!key=mykey ex=a}"+t1
    ,"facet.field","{!key=other ex=b}"+t1
    ,"facet.field","{!key=again ex=a,b}"+t1
    ,"facet.field",t1
    ,"fq","{!tag=a}id:[1 TO 7]", "fq","{!tag=b}id:[3 TO 9]"
    );
    query("q", "*:*", "facet", "true", "facet.field", "{!ex=t1}SubjectTerms_mfacet", "fq", "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", "10", "facet.mincount", "1");

    // test field that is valid in schema but missing in all shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",missingField, "facet.mincount",2);
    // test field that is valid in schema and missing in some shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",oddField, "facet.mincount",2);

    query("q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", i1);


    // Try to get better coverage for refinement queries by turning off over requesting.
    // This makes it much more likely that we may not get the top facet values and hence
    // we turn of that checking.
    handle.put("facet_fields", SKIPVAL);    
    query("q","*:*", "rows",0, "facet","true", "facet.field",t1,"facet.limit",5, "facet.shard.limit",5);
    // check a complex key name
    query("q","*:*", "rows",0, "facet","true", "facet.field","{!key='a b/c \\' \\} foo'}"+t1,"facet.limit",5, "facet.shard.limit",5);
    handle.remove("facet_fields");

    query("q", "*:*", "sort", "n_tl1 desc");

    // index the same document to two shards and make sure things
    // don't blow up.
    // assumes first n clients are first n shards
    if (clients.size()>=2) {
      index(id,100, i1, 107 ,t1,"oh no, a duplicate!");
      for (int i=0; i<shardCount; i++) {
        index_specific(i, id,100, i1, 107 ,t1,"oh no, a duplicate!");
      }
      commit();
      query("q","duplicate", "hl","true", "hl.fl", t1);
      query("q","fox duplicate horses", "hl","true", "hl.fl", t1);
      query("q","*:*", "rows",100);
    }
    
    // TODO: this is failing because the counts per shard don't add up to the control - distrib total
    // counts do match, so the same doc (same id) must be on different shards.
    // our hash is not stable yet in distrib update proc
    assertDocCounts();

    query("q", "*:*", "sort", "n_tl1 desc");
    
    // kill a shard
    JettySolrRunner deadShard = chaosMonkey.killShard("shard2", 0);
    //JettySolrRunner deadShard2 = killShard("shard3", 1);
    
    // ensure shard is dead
    try {
      index_specific(shardToClient.get("shard2").get(0), id, 999, i1, 107, t1,
        "specific doc!");
      fail("This server should be down and this update should have failed");
    } catch (SolrServerException e) {
      // expected..
    }
    
    // try to index to a living shard at shard2
    index_specific(shardToClient.get("shard2").get(1), id, 1000, i1, 108, t1,
        "specific doc!");

    commit();
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // TMP: try adding a doc with CloudSolrServer
    cloudClient.setDefaultCollection(DEFAULT_COLLECTION);
    SolrQuery query = new SolrQuery("*:*");
    query.add("distrib", "true");
    long numFound1 = cloudClient.query(query).getResults().getNumFound();
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1001);
    
    controlClient.add(doc);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(cloudClient);
    
    commit();
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    long numFound2 = cloudClient.query(query).getResults().getNumFound();
    
    // lets just check that the one doc since last commit made it in...
    assertEquals(numFound1 + 1, numFound2);
    
    // test debugging
    handle.put("explain", UNORDERED);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    query("q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);

    
    if (VERBOSE) {
      System.out.println(controlClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    
    for (SolrServer client : clients) {
      try {
        System.out.println(client.query(new SolrQuery("*:*")).getResults().getNumFound());
      } catch(Exception e) {
        
      }
    }
    }
    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");

    // this should trigger a recovery phase on deadShard

    deadShard.start(true);
    
    waitForRecovery(deadShard);
    
    List<SolrServer> s2c = shardToClient.get("shard2");

    // if we properly recovered, we should now have the couple missing docs that
    // came in while shard was down
    assertEquals(s2c.get(0).query(new SolrQuery("*:*")).getResults()
        .getNumFound(), s2c.get(1).query(new SolrQuery("*:*"))
        .getResults().getNumFound());
    
    query("q", "*:*", "sort", "n_tl1 desc");
    
    // test adding another replica to a shard - it should do a recovery/replication to pick up the index from the leader
    JettySolrRunner newReplica = createJettys(1).get(0);
    
    waitForRecovery(newReplica);
    
    // new server should be part of first shard
    // how many docs are on the new shard?
    for (SolrServer client : shardToClient.get("shard1")) {
      if (VERBOSE) System.out.println("total:" + client.query(new SolrQuery("*:*")).getResults().getNumFound());
    }

    // assert the new server has the same number of docs as another server in
    // that shard
    // TODO: make a new call that checks each shard in slice has equal docs
    assertEquals(shardToClient.get("shard1").get(0).query(new SolrQuery("*:*"))
        .getResults().getNumFound(),
        shardToClient.get("shard1").get(shardToClient.get("shard1").size() - 1)
            .query(new SolrQuery("*:*")).getResults().getNumFound());

    assertDocCounts();
    
//    String docId = "99999999";
//    indexr("id", docId, t1, "originalcontent");
//    
//    commit();
//    
//    ModifiableSolrParams params = new ModifiableSolrParams();
//    params.add("distrib", "true");
//    params.add("q", t1 + ":originalcontent");
//    QueryResponse results = clients.get(0).query(params);
//    assertEquals(1, results.getResults().getNumFound());
//    System.out.println("results:" + results);
//    
//    // update doc
//    indexr("id", docId, t1, "updatedcontent");
//    
//    commit();
//    
//    results = clients.get(0).query(params);
//    assertEquals(0, results.getResults().getNumFound());
//    
//    params.set("q", t1 + ":updatedcontent");
//    
//    results = clients.get(0).query(params);
//    assertEquals(1, results.getResults().getNumFound());
//    
//    UpdateRequest uReq = new UpdateRequest();
//    uReq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
//    uReq.deleteById(docId).process(clients.get(0));
//    
//    commit();
//    
//    results = clients.get(0).query(params);
//    assertEquals(0, results.getResults().getNumFound());
    
    // expire a session...
    //CloudJettyRunner cloudJetty = shardToJetty.get("shard1").get(0);
    //chaosMonkey.expireSession(cloudJetty);
    
    // should cause another recovery...
    
    //Thread.sleep(10000000000L);

  }

  protected void assertDocCounts() throws Exception {
    // TODO: as we create the clients, we should build a map from shard to node/client
    // and node/client to shard?
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
    long controlCount = controlClient.query(new SolrQuery("*:*")).getResults().getNumFound();

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
      throw new RuntimeException("Could not find collection " + DEFAULT_COLLECTION + " in " + cloudState.getCollections());
    }

    for (SolrServer client : clients) {
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          String shardName = new URI(((CommonsHttpSolrServer)client).getBaseURL()).getPort() + "_solr_";
          if (VERBOSE && shard.getKey().endsWith(shardName)) {
            System.out.println("shard:" + slice.getKey());
            System.out.println(shard.getValue());
          }
        }
      }
      
      long count = 0;
      String currentState = clientToInfo.get(client).get(ZkStateReader.STATE_PROP);
      if (currentState != null && !currentState.equals(ZkStateReader.RECOVERING)) {
        count = client.query(new SolrQuery("*:*")).getResults().getNumFound();
      }

      if (VERBOSE) System.out.println("client docs:" + count + "\n\n");
    }
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
    SolrQuery query = new SolrQuery("*:*");
    query.add("distrib", "true");
    assertEquals("Doc Counts do not add up", controlCount, cloudClient.query(query).getResults().getNumFound());
  }
  
  protected void waitForRecovery(JettySolrRunner replica)
      throws InterruptedException {
    final CountDownLatch recoveryLatch = new CountDownLatch(1);
    RecoveryStrat recoveryStrat = ((SolrDispatchFilter) replica.getDispatchFilter().getFilter()).getCores()
        .getZkController().getRecoveryStrat();
    
    recoveryStrat.setRecoveryListener(new RecoveryListener() {
      
      @Override
      public void startRecovery() {}
      
      @Override
      public void finishedReplication() {}
      
      @Override
      public void finishedRecovery() {
        recoveryLatch.countDown();
      }
    });
    
    // wait for recovery to finish
    // if it takes over n seconds, assume we didnt get our listener attached before
    // recover started - it should be done before n though
    recoveryLatch.await(30, TimeUnit.SECONDS);
  }

  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException {  
    
    if (r.nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp = cloudClient.query(params);
    return rsp;
  }
  
  @Override
  public void tearDown() throws Exception {
    if (VERBOSE) {
      super.printLayout();
    }
    if (cloudClient != null) {
      cloudClient.close();
    }
    if (zkStateReader != null) {
      zkStateReader.close();
    }
    super.tearDown();
    System.clearProperty("CLOUD_UPDATE_DELAY");
    System.clearProperty("zkHost");
    System.clearProperty("remove.version.field");
  }
  
  protected void commit() throws Exception {
    controlClient.commit();
    for (SolrServer client : clients) {
      try {
        client.commit();
      } catch (SolrServerException e) {
        // we might have killed a server on purpose in the test
        log.warn("", e);
      }
    }
  }
}
