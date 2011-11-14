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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

/**
 *
 */
public class FullDistributedZkTest extends AbstractDistributedZkTestCase {
  
  private static final String DEFAULT_COLLECTION = "collection1";
  private static final boolean DEBUG = true;
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
  private static final int sliceCount = 3;
  
  
  protected Map<SolrServer,ZkNodeProps> clientToInfo = new HashMap<SolrServer,ZkNodeProps>();
  protected Map<String,List<SolrServer>> shardToClient = new HashMap<String,List<SolrServer>>();
  protected Map<String,List<JettySolrRunner>> shardToJetty = new HashMap<String,List<JettySolrRunner>>();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("CLOUD_UPDATE_DELAY", "0");
    System.setProperty("numShards", Integer.toString(sliceCount));
    
    System.setProperty("remove.version.field", "true");
  }
  
  public FullDistributedZkTest() {
    fixShardCount = true;
    shardCount = 6;
    
    // TODO: for now, turn off stress because it uses regular clients, and we 
    // need the cloud client because we kill servers
    stress = 0;
  }
  
  @Override
  protected void createServers(int numShards) throws Exception {
    System.setProperty("collection", "control_collection");
    controlJetty = createJetty(testDir, testDir + "/control/data", "control_shard");
    System.clearProperty("collection");
    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner j = createJetty(testDir, testDir + "/jetty" + i, null, "solrconfig-distrib-update.xml");
      jettys.add(j);
      SolrServer client = createNewSolrServer(j.getLocalPort());
      clients.add(client);
    }
    
    for (SolrServer client : clients) {
      // find info for this client in zk
      ZkStateReader zk = new ZkStateReader(zkServer.getZkAddress(), 10000,
          AbstractZkTestCase.TIMEOUT);
      zk.createClusterStateWatchersAndUpdate();
      
      Map<String,Slice> slices = zk.getCloudState().getSlices(
          DEFAULT_COLLECTION);
      zk.updateCloudState(true);
      
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          String shardName = new URI(
              ((CommonsHttpSolrServer) client).getBaseURL()).getPort()
              + "_solr_";
          // System.out.println("key:" + shard.getKey() + " try:" + shardName);
          if (shard.getKey().endsWith(shardName)) {
            System.out.println("shard:" + slice.getKey());
            System.out.println(shard.getValue());
            
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
    
    for (JettySolrRunner jetty : jettys) {
      // find info for this client in zk
      ZkStateReader zk = new ZkStateReader(zkServer.getZkAddress(), 10000,
          AbstractZkTestCase.TIMEOUT);
      zk.createClusterStateWatchersAndUpdate();
      
      Map<String,Slice> slices = zk.getCloudState().getSlices(
          DEFAULT_COLLECTION);
      zk.updateCloudState(true);
      
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          String shardName = jetty.getLocalPort() + "_solr_";
          // System.out.println("key:" + shard.getKey() + " try:" + shardName);
          if (shard.getKey().endsWith(shardName)) {
//            System.out.println("shard:" + slice.getKey());
//            System.out.println(shard.getValue());
            
            List<JettySolrRunner> list = shardToJetty.get(slice.getKey());
            if (list == null) {
              list = new ArrayList<JettySolrRunner>();
              shardToJetty.put(slice.getKey(), list);
            }
            list.add(jetty);
          }
        }
      }
      
    }
    
    // build the shard string
    for (int i = 1; i <= numShards/2; i++) {
      JettySolrRunner j = jettys.get(i);
      JettySolrRunner j2 = jettys.get(i + (numShards/2 - 1));
      if (sb.length() > 0) sb.append(',');
      sb.append("localhost:").append(j.getLocalPort()).append(context);
      sb.append("|localhost:").append(j2.getLocalPort()).append(context);
    }
    shards = sb.toString();
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
    System.out.println("add doc to shard:" + which);
    
    if (pick) {
      which = which + ((shardCount / sliceCount) * random.nextInt(sliceCount-1));
    }
    
    CommonsHttpSolrServer client = (CommonsHttpSolrServer) clients.get(which);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam("update.chain", "distrib-update-chain");
    System.out.println("set update.chain on req");
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
    ureq.setParam("update.chain", "distrib-update-chain");
    System.out.println("set update.chain on req");
    ureq.process(client);
  }
  
  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    for (SolrServer client : clients) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParam("update.chain", "distrib-update-chain");
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
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

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
    //assertDocCounts();

    // kill a shard
    JettySolrRunner deadShard = killShard("shard2", 0);
    JettySolrRunner deadShard2 = killShard("shard3", 1);
    
    // TODO: test indexing after killing shards - smart solrj client should not
    // care at all
    
    // try to index to a living shard at shard2
    index_specific(3, id,1000, i1, 107 ,t1,"specific doc!");
    
    commit();
    
    // TMP: try adding a doc with CloudSolrServer
    CloudSolrServer server = new CloudSolrServer(zkServer.getZkAddress());
    server.setDefaultCollection(DEFAULT_COLLECTION);
    SolrQuery query = new SolrQuery("*:*");
    query.add("distrib", "true");
    long numFound1 = server.query(query).getResults().getNumFound();
    
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1001);
    
    controlClient.add(doc);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam("update.chain", "distrib-update-chain");
    ureq.process(server);
    
    commit();
    
    long numFound2 = server.query(query).getResults().getNumFound();
    
    // lets just check that the one doc since last commit made it in...
    //TODO this sometimes fails - need to dig up what missed/messed up part causes it
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

    
    System.out.println(controlClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    for (SolrServer client : clients) {
      try {
        System.out.println(client.query(new SolrQuery("*:*")).getResults().getNumFound());
      } catch(Exception e) {
        
      }
    }
    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");

    // this should trigger a recovery phase on deadShard
    deadShard.start(true);
    
    
    
    // Thread.sleep(10000000000L);
    if (DEBUG) {
      super.printLayout();
    }
  }

  private JettySolrRunner killShard(String shard, int index) throws Exception {
    // kill
    System.out.println(" KILL:" + shardToClient);
    System.out.println(shardToJetty.get(shard));
    
    // kill first shard in shard2
    JettySolrRunner jetty = shardToJetty.get(shard).get(index);
    jetty.stop();
    return jetty;
  }

  private void assertDocCounts() throws Exception {
    // TODO: as we create the clients, we should build a map from shard to node/client
    // and node/client to shard?
    System.out.println("after first doc:");
    long controlCount = controlClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    System.out.println("control:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    // do some really inefficient mapping...
    ZkStateReader zk = new ZkStateReader(zkServer.getZkAddress(), 10000, AbstractZkTestCase.TIMEOUT);
    zk.createClusterStateWatchersAndUpdate();
  //  Map<SolrServer,ZkNodeProps> clientToInfo = new HashMap<SolrServer,ZkNodeProps>();
    Map<String,Slice> slices = zk.getCloudState().getSlices(DEFAULT_COLLECTION);
 
    zk.updateCloudState(true);
    
    long clientCount = 0;
    for (SolrServer client : clients) {
      for (Map.Entry<String,Slice> slice : slices.entrySet()) {
        Map<String,ZkNodeProps> theShards = slice.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : theShards.entrySet()) {
          String shardName = new URI(((CommonsHttpSolrServer)client).getBaseURL()).getPort() + "_solr_";
         // System.out.println("key:" + shard.getKey() + " try:" + shardName);
          if (shard.getKey().endsWith(shardName)) {
            System.out.println("shard:" + slice.getKey());
            System.out.println(shard.getValue());
          }
        }
      }
      
      long count = client.query(new SolrQuery("*:*")).getResults().getNumFound();
      
      System.out.println("docs:" + count + "\n\n");
      clientCount += count;
    }
    assertEquals("Doc Counts do not add up", controlCount, clientCount / (shardCount / sliceCount));
  }

  volatile CloudSolrServer solrj;

  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException {

    // use the distributed solrj client
    if (solrj == null) {
      synchronized(this) {
        try {
          CloudSolrServer server = new CloudSolrServer(zkServer.getZkAddress());
          server.setDefaultCollection(DEFAULT_COLLECTION);
          solrj = server;
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }
    }

    if (r.nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp = solrj.query(params);
    return rsp;
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("CLOUD_UPDATE_DELAY");
    System.clearProperty("zkHost");
    System.clearProperty("remove.version.field");
  }
}
