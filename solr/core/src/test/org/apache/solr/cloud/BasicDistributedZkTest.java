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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Unload;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrCmdDistributor.Request;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Before;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@Slow
public class BasicDistributedZkTest extends AbstractFullDistribZkTestBase {
  
  private static final String DEFAULT_COLLECTION = "collection1";
  private static final boolean DEBUG = false;
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
  
  private Map<String,List<SolrServer>> otherCollectionClients = new HashMap<String,List<SolrServer>>();

  private String oneInstanceCollection = "oneInstanceCollection";
  private String oneInstanceCollection2 = "oneInstanceCollection2";
  
  ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));
  
  CompletionService<Request> completionService;
  Set<Future<Request>> pending;
  
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
  }

  
  public BasicDistributedZkTest() {
    fixShardCount = true;
    
    sliceCount = 2;
    shardCount = 4;
    completionService = new ExecutorCompletionService<Request>(executor);
    pending = new HashSet<Future<Request>>();
    
  }
  
  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {

    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < shardCount; i++) {
        if (i > 0)
          sb.append(',');
        sb.append("shard" + (i + 3));
      }
      params.set("shards", sb.toString());
    }
  }
  
  @Override
  public void doTest() throws Exception {
    // setLoggingLevel(null);

    
    // make sure we have leaders for each shard
    for (int j = 1; j < sliceCount; j++) {
      zkStateReader.getLeaderProps(DEFAULT_COLLECTION, "shard" + j, 10000);
    }      // make sure we again have leaders for each shard
    
    waitForRecoveriesToFinish(false);
    
    del("*:*");

    indexr(id,1, i1, 100, tlong, 100,t1,"now is the time for all good men"
            ,"foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
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

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    // random value sort
    for (String f : fieldNames) {
      query(false, new String[] {"q","*:*", "sort",f+" desc"});
      query(false, new String[] {"q","*:*", "sort",f+" asc"});
    }

    // these queries should be exactly ordered and scores should exactly match
    query(false, new String[] {"q","*:*", "sort",i1+" desc"});
    query(false, new String[] {"q","*:*", "sort",i1+" asc"});
    query(false, new String[] {"q","*:*", "sort",i1+" desc", "fl","*,score"});
    query(false, new String[] {"q","*:*", "sort","n_tl1 asc", "fl","*,score"}); 
    query(false, new String[] {"q","*:*", "sort","n_tl1 desc"});
    handle.put("maxScore", SKIPVAL);
    query(false, new String[] {"q","{!func}"+i1});// does not expect maxScore. So if it comes ,ignore it. JavaBinCodec.writeSolrDocumentList()
    //is agnostic of request params.
    handle.remove("maxScore");
    query(false, new String[] {"q","{!func}"+i1, "fl","*,score"});  // even scores should match exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query(false, new String[] {"q","quick"});
    query(false, new String[] {"q","all","fl","id","start","0"});
    query(false, new String[] {"q","all","fl","foofoofoo","start","0"});  // no fields in returned docs
    query(false, new String[] {"q","all","fl","id","start","100"});

    handle.put("score", SKIPVAL);
    query(false, new String[] {"q","quick","fl","*,score"});
    query(false, new String[] {"q","all","fl","*,score","start","1"});
    query(false, new String[] {"q","all","fl","*,score","start","100"});

    query(false, new String[] {"q","now their fox sat had put","fl","*,score",
            "hl","true","hl.fl",t1});

    query(false, new String[] {"q","now their fox sat had put","fl","foofoofoo",
            "hl","true","hl.fl",t1});

    query(false, new String[] {"q","matchesnothing","fl","*,score"});  

    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","count"});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","count", "facet.mincount",2});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","index"});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","index", "facet.mincount",2});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1,"facet.limit",1});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.offset",1});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.mincount",2});

    // test faceting multiple things at once
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"
    ,"facet.field",t1});

    // test filter tagging, facet exclusion, and naming (multi-select facet support)
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.query","{!key=myquick}quick", "facet.query","{!key=myall ex=a}all", "facet.query","*:*"
    ,"facet.field","{!key=mykey ex=a}"+t1
    ,"facet.field","{!key=other ex=b}"+t1
    ,"facet.field","{!key=again ex=a,b}"+t1
    ,"facet.field",t1
    ,"fq","{!tag=a}id:[1 TO 7]", "fq","{!tag=b}id:[3 TO 9]"}
    );
    query(false, new Object[] {"q", "*:*", "facet", "true", "facet.field", "{!ex=t1}SubjectTerms_mfacet", "fq", "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", "10", "facet.mincount", "1"});

    // test field that is valid in schema but missing in all shards
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",missingField, "facet.mincount",2});
    // test field that is valid in schema and missing in some shards
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",oddField, "facet.mincount",2});

    query(false, new Object[] {"q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", i1});

    /*** TODO: the failure may come back in "exception"
    try {
      // test error produced for field that is invalid for schema
      query("q","*:*", "rows",100, "facet","true", "facet.field",invalidField, "facet.mincount",2);
      TestCase.fail("SolrServerException expected for invalid field that is not in schema");
    } catch (SolrServerException ex) {
      // expected
    }
    ***/

    // Try to get better coverage for refinement queries by turning off over requesting.
    // This makes it much more likely that we may not get the top facet values and hence
    // we turn of that checking.
    handle.put("facet_fields", SKIPVAL);    
    query(false, new Object[] {"q","*:*", "rows",0, "facet","true", "facet.field",t1,"facet.limit",5, "facet.shard.limit",5});
    // check a complex key name
    query(false, new Object[] {"q","*:*", "rows",0, "facet","true", "facet.field","{!key='a b/c \\' \\} foo'}"+t1,"facet.limit",5, "facet.shard.limit",5});
    handle.remove("facet_fields");


    // index the same document to two servers and make sure things
    // don't blow up.
    if (clients.size()>=2) {
      index(id,100, i1, 107 ,t1,"oh no, a duplicate!");
      for (int i=0; i<clients.size(); i++) {
        index_specific(i, id,100, i1, 107 ,t1,"oh no, a duplicate!");
      }
      commit();
      query(false, new Object[] {"q","duplicate", "hl","true", "hl.fl", t1});
      query(false, new Object[] {"q","fox duplicate horses", "hl","true", "hl.fl", t1});
      query(false, new Object[] {"q","*:*", "rows",100});
    }

    // test debugging
    handle.put("explain", SKIPVAL);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    query(false, new Object[] {"q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true"});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true"});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY});

    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");

    // would be better if these where all separate tests - but much, much
    // slower
    doOptimisticLockingAndUpdating();
    testMultipleCollections();
    testANewCollectionInOneInstance();
    testSearchByCollectionName();
    testANewCollectionInOneInstanceWithManualShardAssignement();
    testNumberOfCommitsWithCommitAfterAdd();

    testUpdateProcessorsRunOnlyOnce("distrib-dup-test-chain-explicit");
    testUpdateProcessorsRunOnlyOnce("distrib-dup-test-chain-implicit");

    testCollectionsAPI();
    testCoreUnloadAndLeaders();
    testUnloadLotsOfCores();
    testStopAndStartCoresInOneInstance();
    testUnloadShardAndCollection();
    // Thread.sleep(10000000000L);
    if (DEBUG) {
      super.printLayout();
    }
  }

  private void testUnloadShardAndCollection() throws Exception{
    // create one leader and one replica
    
    Create createCmd = new Create();
    createCmd.setCoreName("test_unload_shard_and_collection_1");
    String collection = "test_unload_shard_and_collection";
    createCmd.setCollection(collection);
    String coreDataDir = dataDir.getAbsolutePath() + File.separator
        + System.currentTimeMillis() + collection + "1";
    createCmd.setDataDir(coreDataDir);
    createCmd.setNumShards(2);
    
    SolrServer client = clients.get(0);
    String url1 = getBaseUrl(client);
    HttpSolrServer server = new HttpSolrServer(url1);
    
    server.request(createCmd);
    
    createCmd = new Create();
    createCmd.setCoreName("test_unload_shard_and_collection_2");
    collection = "test_unload_shard_and_collection";
    createCmd.setCollection(collection);
    coreDataDir = dataDir.getAbsolutePath() + File.separator
        + System.currentTimeMillis() + collection + "2";
    createCmd.setDataDir(coreDataDir);
    
    server.request(createCmd);

    // now unload one of the two
    Unload unloadCmd = new Unload(false);
    unloadCmd.setCoreName("test_unload_shard_and_collection_2");
    server.request(unloadCmd);
    
    // there should be only one shard
    Slice shard2 = getCommonCloudSolrServer().getZkStateReader().getClusterState().getSlice(collection, "shard2");
    long timeoutAt = System.currentTimeMillis() + 30000;
    while (shard2 != null) {
      if (System.currentTimeMillis() > timeoutAt) {
        printLayout();
        fail("Still found shard");
      }
      
      Thread.sleep(50);
      shard2 = getCommonCloudSolrServer().getZkStateReader().getClusterState().getSlice(collection, "shard2");
    }

    Slice shard1 = getCommonCloudSolrServer().getZkStateReader().getClusterState().getSlice(collection, "shard1");
    assertNotNull(shard1);
    assertTrue(getCommonCloudSolrServer().getZkStateReader().getClusterState().getCollections().contains(collection));
    
    // now unload one of the other
    unloadCmd = new Unload(false);
    unloadCmd.setCoreName("test_unload_shard_and_collection_1");
    server.request(unloadCmd);
    
    //printLayout();
    // the collection should be gone
    timeoutAt = System.currentTimeMillis() + 30000;
    while (getCommonCloudSolrServer().getZkStateReader().getClusterState().getCollections().contains(collection)) {
      if (System.currentTimeMillis() > timeoutAt) {
        printLayout();
        fail("Still found collection");
      }
      
      Thread.sleep(50);
    }
    
  }

  /**
   * @throws Exception on any problem
   */
  private void testCoreUnloadAndLeaders() throws Exception {
    // create a new collection collection
    SolrServer client = clients.get(0);
    String url1 = getBaseUrl(client);
    HttpSolrServer server = new HttpSolrServer(url1);
    
    Create createCmd = new Create();
    createCmd.setCoreName("unloadcollection1");
    createCmd.setCollection("unloadcollection");
    createCmd.setNumShards(1);
    String core1DataDir = dataDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection1" + "_1n";
    createCmd.setDataDir(core1DataDir);
    server.request(createCmd);
    
    ZkStateReader zkStateReader = getCommonCloudSolrServer().getZkStateReader();

    zkStateReader.updateClusterState(true);

    int slices = zkStateReader.getClusterState().getCollectionStates().get("unloadcollection").size();
    assertEquals(1, slices);
    
    client = clients.get(1);
    String url2 = getBaseUrl(client);
    server = new HttpSolrServer(url2);
    
    createCmd = new Create();
    createCmd.setCoreName("unloadcollection2");
    createCmd.setCollection("unloadcollection");
    String core2dataDir = dataDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection1" + "_2n";
    createCmd.setDataDir(core2dataDir);
    server.request(createCmd);
    
    zkStateReader.updateClusterState(true);
    slices = zkStateReader.getClusterState().getCollectionStates().get("unloadcollection").size();
    assertEquals(1, slices);
    
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    ZkCoreNodeProps leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    
    Random random = random();
    HttpSolrServer collectionClient;
    if (random.nextBoolean()) {
      collectionClient = new HttpSolrServer(leaderProps.getCoreUrl());
      // lets try and use the solrj client to index and retrieve a couple
      // documents
      SolrInputDocument doc1 = getDoc(id, 6, i1, -600, tlong, 600, t1,
          "humpty dumpy sat on a wall");
      SolrInputDocument doc2 = getDoc(id, 7, i1, -600, tlong, 600, t1,
          "humpty dumpy3 sat on a walls");
      SolrInputDocument doc3 = getDoc(id, 8, i1, -600, tlong, 600, t1,
          "humpty dumpy2 sat on a walled");
      collectionClient.add(doc1);
      collectionClient.add(doc2);
      collectionClient.add(doc3);
      collectionClient.commit();
    }

    // create another replica for our collection
    client = clients.get(2);
    String url3 = getBaseUrl(client);
    server = new HttpSolrServer(url3);
    
    createCmd = new Create();
    createCmd.setCoreName("unloadcollection3");
    createCmd.setCollection("unloadcollection");
    String core3dataDir = dataDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection" + "_3n";
    createCmd.setDataDir(core3dataDir);
    server.request(createCmd);
    
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    // so that we start with some versions when we reload...
    DirectUpdateHandler2.commitOnClose = false;
    
    HttpSolrServer addClient = new HttpSolrServer(url3 + "/unloadcollection3");
    // add a few docs
    for (int x = 20; x < 100; x++) {
      SolrInputDocument doc1 = getDoc(id, x, i1, -600, tlong, 600, t1,
          "humpty dumpy sat on a wall");
      addClient.add(doc1);
    }

    // don't commit so they remain in the tran log
    //collectionClient.commit();
    
    // unload the leader
    collectionClient = new HttpSolrServer(leaderProps.getBaseUrl());
    
    Unload unloadCmd = new Unload(false);
    unloadCmd.setCoreName(leaderProps.getCoreName());
    ModifiableSolrParams p = (ModifiableSolrParams) unloadCmd.getParams();

    collectionClient.request(unloadCmd);

//    Thread.currentThread().sleep(500);
//    printLayout();
    
    int tries = 50;
    while (leaderProps.getCoreUrl().equals(zkStateReader.getLeaderUrl("unloadcollection", "shard1", 15000))) {
      Thread.sleep(100);
      if (tries-- == 0) {
        fail("Leader never changed");
      }
    }
    
    // ensure there is a leader
    zkStateReader.getLeaderProps("unloadcollection", "shard1", 15000);
    
    addClient = new HttpSolrServer(url2 + "/unloadcollection2");
    // add a few docs while the leader is down
    for (int x = 101; x < 200; x++) {
      SolrInputDocument doc1 = getDoc(id, x, i1, -600, tlong, 600, t1,
          "humpty dumpy sat on a wall");
      addClient.add(doc1);
    }
    
    
    // create another replica for our collection
    client = clients.get(3);
    String url4 = getBaseUrl(client);
    server = new HttpSolrServer(url4);
    
    createCmd = new Create();
    createCmd.setCoreName("unloadcollection4");
    createCmd.setCollection("unloadcollection");
    String core4dataDir = dataDir.getAbsolutePath() + File.separator + System.currentTimeMillis() + "unloadcollection" + "_4n";
    createCmd.setDataDir(core4dataDir);
    server.request(createCmd);
    
    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    // unload the leader again
    leaderProps = getLeaderUrlFromZk("unloadcollection", "shard1");
    collectionClient = new HttpSolrServer(leaderProps.getBaseUrl());
    
    unloadCmd = new Unload(false);
    unloadCmd.setCoreName(leaderProps.getCoreName());
    p = (ModifiableSolrParams) unloadCmd.getParams();
    collectionClient.request(unloadCmd);
    
    tries = 50;
    while (leaderProps.getCoreUrl().equals(zkStateReader.getLeaderUrl("unloadcollection", "shard1", 15000))) {
      Thread.sleep(100);
      if (tries-- == 0) {
        fail("Leader never changed");
      }
    }
    
    zkStateReader.getLeaderProps("unloadcollection", "shard1", 15000);
    
    
    // set this back
    DirectUpdateHandler2.commitOnClose = true;
    
    // bring the downed leader back as replica
    server = new HttpSolrServer(leaderProps.getBaseUrl());
    
    createCmd = new Create();
    createCmd.setCoreName(leaderProps.getCoreName());
    createCmd.setCollection("unloadcollection");
    createCmd.setDataDir(core1DataDir);
    server.request(createCmd);

    waitForRecoveriesToFinish("unloadcollection", zkStateReader, false);
    
    
    server = new HttpSolrServer(url1 + "/unloadcollection");
   // System.out.println(server.query(new SolrQuery("*:*")).getResults().getNumFound());
    server = new HttpSolrServer(url2 + "/unloadcollection");
    server.commit();
    SolrQuery q = new SolrQuery("*:*");
    q.set("distrib", false);
    long found1 = server.query(q).getResults().getNumFound();
    server = new HttpSolrServer(url3 + "/unloadcollection");
    server.commit();
    q = new SolrQuery("*:*");
    q.set("distrib", false);
    long found3 = server.query(q).getResults().getNumFound();
    server = new HttpSolrServer(url4 + "/unloadcollection");
    server.commit();
    q = new SolrQuery("*:*");
    q.set("distrib", false);
    long found4 = server.query(q).getResults().getNumFound();
    
    // all 3 shards should now have the same number of docs
    assertEquals(found1, found3);
    assertEquals(found3, found4);
    
  }
  
  private void testUnloadLotsOfCores() throws Exception {
    SolrServer client = clients.get(2);
    String url3 = getBaseUrl(client);
    final HttpSolrServer server = new HttpSolrServer(url3);
    
    ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
        5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new DefaultSolrThreadFactory("testExecutor"));
    int cnt = atLeast(6);
    
    // create the 6 cores
    createCores(server, executor, "multiunload", 2, cnt);
    
    executor.shutdown();
    executor.awaitTermination(120, TimeUnit.SECONDS);
    executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5,
        TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new DefaultSolrThreadFactory("testExecutor"));
    for (int j = 0; j < cnt; j++) {
      final int freezeJ = j;
      executor.execute(new Runnable() {
        @Override
        public void run() {
          Unload unloadCmd = new Unload(true);
          unloadCmd.setCoreName("multiunload" + freezeJ);
          try {
            server.request(unloadCmd);
          } catch (SolrServerException e) {
            throw new RuntimeException(e);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      Thread.sleep(random().nextInt(50));
    }
    executor.shutdown();
    executor.awaitTermination(120, TimeUnit.SECONDS);
  }
  
  private void testStopAndStartCoresInOneInstance() throws Exception {
    SolrServer client = clients.get(0);
    String url3 = getBaseUrl(client);
    final HttpSolrServer server = new HttpSolrServer(url3);
    
    ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
        5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new DefaultSolrThreadFactory("testExecutor"));
    int cnt = 3;
    
    // create the cores
    createCores(server, executor, "multiunload2", 1, cnt);
    
    executor.shutdown();
    executor.awaitTermination(120, TimeUnit.SECONDS);
    
    ChaosMonkey.stop(cloudJettys.get(0).jetty);
    printLayout();

    Thread.sleep(5000);
    ChaosMonkey.start(cloudJettys.get(0).jetty);
    cloudClient.getZkStateReader().updateClusterState(true);
    try {
      cloudClient.getZkStateReader().getLeaderProps("multiunload2", "shard1", 30000);
    } catch (SolrException e) {
      printLayout();
      throw e;
    }
    
    printLayout();

  }

  private void createCores(final HttpSolrServer server,
      ThreadPoolExecutor executor, final String collection, final int numShards, int cnt) {
    for (int i = 0; i < cnt; i++) {
      final int freezeI = i;
      executor.execute(new Runnable() {
        
        @Override
        public void run() {
          Create createCmd = new Create();
          createCmd.setCoreName(collection + freezeI);
          createCmd.setCollection(collection);
          String core3dataDir = dataDir.getAbsolutePath() + File.separator
              + System.currentTimeMillis() + collection + "_3n" + freezeI;
          createCmd.setDataDir(core3dataDir);
          createCmd.setNumShards(numShards);
          try {
            server.request(createCmd);
          } catch (SolrServerException e) {
            throw new RuntimeException(e);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        
      });
    }
  }

  private String getBaseUrl(SolrServer client) {
    String url2 = ((HttpSolrServer) client).getBaseURL()
        .substring(
            0,
            ((HttpSolrServer) client).getBaseURL().length()
                - DEFAULT_COLLECTION.length() -1);
    return url2;
  }


  private void testCollectionsAPI() throws Exception {
 
    // TODO: fragile - because we dont pass collection.confName, it will only
    // find a default if a conf set with a name matching the collection name is found, or 
    // if there is only one conf set. That and the fact that other tests run first in this
    // env make this pretty fragile
    
    // create new collections rapid fire
    Map<String,List<Integer>> collectionInfos = new HashMap<String,List<Integer>>();
    int cnt = atLeast(3);
    
    for (int i = 0; i < cnt; i++) {
      int numShards = _TestUtil.nextInt(random(), 0, shardCount) + 1;
      int replicationFactor = _TestUtil.nextInt(random(), 0, 3) + 2;
      int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrServer().getZkStateReader().getClusterState().getLiveNodes().size())) + 1;
      createCollection(collectionInfos, i, numShards, replicationFactor, maxShardsPerNode);
    }
    
    Set<Entry<String,List<Integer>>> collectionInfosEntrySet = collectionInfos.entrySet();
    for (Entry<String,List<Integer>> entry : collectionInfosEntrySet) {
      String collection = entry.getKey();
      List<Integer> list = entry.getValue();
      checkForCollection(collection, list);
      
      String url = getUrlFromZk(collection);

      HttpSolrServer collectionClient = new HttpSolrServer(url);
      
      // poll for a second - it can take a moment before we are ready to serve
      waitForNon403or404or503(collectionClient);
    }

    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish("awholynewcollection_" + j, zkStateReader, false);
    }
    
    List<String> collectionNameList = new ArrayList<String>();
    collectionNameList.addAll(collectionInfos.keySet());
    String collectionName = collectionNameList.get(random().nextInt(collectionNameList.size()));
    
    String url = getUrlFromZk(collectionName);

    HttpSolrServer collectionClient = new HttpSolrServer(url);
    
    
    // lets try and use the solrj client to index a couple documents
    SolrInputDocument doc1 = getDoc(id, 6, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall");
    SolrInputDocument doc2 = getDoc(id, 7, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls");
    SolrInputDocument doc3 = getDoc(id, 8, i1, -600, tlong, 600, t1,
        "humpty dumpy2 sat on a walled");

    collectionClient.add(doc1);
    
    collectionClient.add(doc2);

    collectionClient.add(doc3);
    
    collectionClient.commit();
    
    assertEquals(3, collectionClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    
    // lets try a collection reload
    
    // get core open times
    Map<String,Long> urlToTimeBefore = new HashMap<String,Long>();
    collectStartTimes(collectionName, urlToTimeBefore);
    assertTrue(urlToTimeBefore.size() > 0);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.RELOAD.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    // we can use this client because we just want base url
    final String baseUrl = ((HttpSolrServer) clients.get(0)).getBaseURL().substring(
        0,
        ((HttpSolrServer) clients.get(0)).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    
    createNewSolrServer("", baseUrl).request(request);

    // reloads make take a short while
    boolean allTimesAreCorrect = waitForReloads(collectionName, urlToTimeBefore);
    assertTrue("some core start times did not change on reload", allTimesAreCorrect);
    
    
    waitForRecoveriesToFinish("awholynewcollection_" + (cnt - 1), zkStateReader, false);
    
    // remove a collection
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
 
    createNewSolrServer("", baseUrl).request(request);
    
    // ensure its out of the state
    checkForMissingCollection(collectionName);
    
    //collectionNameList.remove(collectionName);

    // remove an unknown collection
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "unknown_collection");
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
 
    createNewSolrServer("", baseUrl).request(request);
    
    // create another collection should still work
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set("numShards", 1);
    params.set(OverseerCollectionProcessor.REPLICATION_FACTOR, 2);
    collectionName = "acollectionafterbaddelete";

    params.set("name", collectionName);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
    createNewSolrServer("", baseUrl).request(request);
    
    List<Integer> list = new ArrayList<Integer> (2);
    list.add(1);
    list.add(2);
    checkForCollection(collectionName, list);
    
    url = getUrlFromZk(collectionName);
    
    collectionClient = new HttpSolrServer(url);
    
    // poll for a second - it can take a moment before we are ready to serve
    waitForNon403or404or503(collectionClient);
    
    for (int j = 0; j < cnt; j++) {
      waitForRecoveriesToFinish(collectionName, zkStateReader, false);
    }

    // test maxShardsPerNode
    int liveNodes = getCommonCloudSolrServer().getZkStateReader().getClusterState().getLiveNodes().size();
    int numShards = (liveNodes/2) + 1;
    int replicationFactor = 2;
    int maxShardsPerNode = 1;
    collectionInfos = new HashMap<String,List<Integer>>();
    createCollection(collectionInfos, cnt, numShards, replicationFactor, maxShardsPerNode);
    
    // TODO: enable this check after removing the 60 second wait in it
    //checkCollectionIsNotCreated(collectionInfos.keySet().iterator().next());
  }


  protected void createCollection(Map<String,List<Integer>> collectionInfos,
      int i, int numShards, int numReplica, int maxShardsPerNode) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set(OverseerCollectionProcessor.NUM_SLICES, numShards);
    params.set(OverseerCollectionProcessor.REPLICATION_FACTOR, numReplica);
    params.set(OverseerCollectionProcessor.MAX_SHARDS_PER_NODE, maxShardsPerNode);
    String collectionName = "awholynewcollection_" + i;
    int clientIndex = random().nextInt(2);
    List<Integer> list = new ArrayList<Integer>();
    list.add(numShards);
    list.add(numReplica);
    list.add(maxShardsPerNode);
    collectionInfos.put(collectionName, list);
    params.set("name", collectionName);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
  
    final String baseUrl = ((HttpSolrServer) clients.get(clientIndex)).getBaseURL().substring(
        0,
        ((HttpSolrServer) clients.get(clientIndex)).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    
    createNewSolrServer("", baseUrl).request(request);
  }

  private boolean waitForReloads(String collectionName, Map<String,Long> urlToTimeBefore) throws SolrServerException, IOException {
    
    
    long timeoutAt = System.currentTimeMillis() + 30000;

    boolean allTimesAreCorrect = false;
    while (System.currentTimeMillis() < timeoutAt) {
      Map<String,Long> urlToTimeAfter = new HashMap<String,Long>();
      collectStartTimes(collectionName, urlToTimeAfter);
      
      boolean retry = false;
      Set<Entry<String,Long>> entries = urlToTimeBefore.entrySet();
      for (Entry<String,Long> entry : entries) {
        Long beforeTime = entry.getValue();
        Long afterTime = urlToTimeAfter.get(entry.getKey());
        assertNotNull(afterTime);
        if (afterTime <= beforeTime) {
          retry = true;
          break;
        }

      }
      if (!retry) {
        allTimesAreCorrect = true;
        break;
      }
    }
    return allTimesAreCorrect;
  }

  private void collectStartTimes(String collectionName,
      Map<String,Long> urlToTime) throws SolrServerException, IOException {
    Map<String,Map<String,Slice>> collections = getCommonCloudSolrServer().getZkStateReader()
        .getClusterState().getCollectionStates();
    if (collections.containsKey(collectionName)) {
      Map<String,Slice> slices = collections.get(collectionName);

      Iterator<Entry<String,Slice>> it = slices.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String,Slice> sliceEntry = it.next();
        Map<String,Replica> sliceShards = sliceEntry.getValue().getReplicasMap();
        Iterator<Entry<String,Replica>> shardIt = sliceShards.entrySet()
            .iterator();
        while (shardIt.hasNext()) {
          Entry<String,Replica> shardEntry = shardIt.next();
          ZkCoreNodeProps coreProps = new ZkCoreNodeProps(shardEntry.getValue());
          CoreAdminResponse mcr = CoreAdminRequest.getStatus(
              coreProps.getCoreName(),
              new HttpSolrServer(coreProps.getBaseUrl()));
          long before = mcr.getStartTime(coreProps.getCoreName()).getTime();
          urlToTime.put(coreProps.getCoreUrl(), before);
        }
      }
    } else {
      throw new IllegalArgumentException("Could not find collection in :"
          + collections.keySet());
    }
  }

  private String getUrlFromZk(String collection) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    Map<String,Slice> slices = clusterState.getCollectionStates().get(collection);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection:" + collection);
    }
    
    for (Map.Entry<String,Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      Map<String,Replica> shards = slice.getReplicasMap();
      Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
      for (Map.Entry<String,Replica> shardEntry : shardEntries) {
        final ZkNodeProps node = shardEntry.getValue();
        if (clusterState.liveNodesContain(node.getStr(ZkStateReader.NODE_NAME_PROP))) {
          return ZkCoreNodeProps.getCoreUrl(node.getStr(ZkStateReader.BASE_URL_PROP), collection); //new ZkCoreNodeProps(node).getCoreUrl();
        }
      }
    }
    
    throw new RuntimeException("Could not find a live node for collection:" + collection);
  }
  
  private ZkCoreNodeProps getLeaderUrlFromZk(String collection, String slice) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    ZkNodeProps leader = clusterState.getLeader(collection, slice);
    if (leader == null) {
      throw new RuntimeException("Could not find leader:" + collection + " " + slice);
    }
    return new ZkCoreNodeProps(leader);
  }

  private void waitForNon403or404or503(HttpSolrServer collectionClient)
      throws Exception {
    SolrException exp = null;
    long timeoutAt = System.currentTimeMillis() + 30000;
    
    while (System.currentTimeMillis() < timeoutAt) {
      boolean missing = false;

      try {
        collectionClient.query(new SolrQuery("*:*"));
      } catch (SolrException e) {
        if (!(e.code() == 403 || e.code() == 503 || e.code() == 404)) {
          throw e;
        }
        exp = e;
        missing = true;
      }
      if (!missing) {
        return;
      }
      Thread.sleep(50);
    }

    fail("Could not find the new collection - " + exp.code() + " : " + collectionClient.getBaseURL());
  }

  private String checkCollectionExpectations(String collectionName, List<Integer> numShardsNumReplicaList) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    
    int expectedSlices = numShardsNumReplicaList.get(0);
    // The Math.min thing is here, because we expect replication-factor to be reduced to if there are not enough live nodes to spread all shards of a collection over different nodes
    int expectedShardsPerSlice = numShardsNumReplicaList.get(1);
    int expectedTotalShards = expectedSlices * expectedShardsPerSlice;
    
      Map<String,Map<String,Slice>> collections = clusterState
          .getCollectionStates();
      if (collections.containsKey(collectionName)) {
        Map<String,Slice> slices = collections.get(collectionName);
        // did we find expectedSlices slices/shards?
      if (slices.size() != expectedSlices) {
        return "Found new collection " + collectionName + ", but mismatch on number of slices. Expected: " + expectedSlices + ", actual: " + slices.size();
      }
      int totalShards = 0;
      for (String sliceName : slices.keySet()) {
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
  
  private void checkForCollection(String collectionName, List<Integer> numShardsNumReplicaList)
      throws Exception {
    // check for an expectedSlices new collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 120000;
    boolean success = false;
    String checkResult = "Didnt get to perform a single check";
    while (System.currentTimeMillis() < timeoutAt) {
      checkResult = checkCollectionExpectations(collectionName, numShardsNumReplicaList);
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

  private void checkCollectionIsNotCreated(String collectionName)
    throws Exception {
    // TODO: this method not called because of below sleep
    Thread.sleep(60000);
    assertFalse(collectionName + " not supposed to exist", getCommonCloudSolrServer().getZkStateReader().getClusterState().getCollections().contains(collectionName));
  }
  
  private void checkForMissingCollection(String collectionName)
      throws Exception {
    // check for a  collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 15000;
    boolean found = true;
    while (System.currentTimeMillis() < timeoutAt) {
      getCommonCloudSolrServer().getZkStateReader().updateClusterState(true);
      ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
      Map<String,Map<String,Slice>> collections = clusterState
          .getCollectionStates();
      if (!collections.containsKey(collectionName)) {
        found = false;
        break;
      }
      Thread.sleep(100);
    }
    if (found) {
      fail("Found collection that should be gone " + collectionName);
    }
  }

  /**
   * Expects a RegexReplaceProcessorFactories in the chain which will
   * "double up" the values in two (stored) string fields.
   * <p>
   * If the values are "double-doubled" or "not-doubled" then we know 
   * the processor was not run the appropriate number of times
   * </p>
   */
  private void testUpdateProcessorsRunOnlyOnce(final String chain) throws Exception {

    final String fieldA = "regex_dup_A_s";
    final String fieldB = "regex_dup_B_s";
    final String val = "x";
    final String expected = "x_x";
    final ModifiableSolrParams updateParams = new ModifiableSolrParams();
    updateParams.add(UpdateParams.UPDATE_CHAIN, chain);
    
    final int numLoops = atLeast(50);
    
    for (int i = 1; i < numLoops; i++) {
      // add doc to random client
      SolrServer updateClient = clients.get(random().nextInt(clients.size()));
      SolrInputDocument doc = new SolrInputDocument();
      addFields(doc, id, i, fieldA, val, fieldB, val);
      UpdateResponse ures = add(updateClient, updateParams, doc);
      assertEquals(chain + ": update failed", 0, ures.getStatus());
      ures = updateClient.commit();
      assertEquals(chain + ": commit failed", 0, ures.getStatus());
    }

    // query for each doc, and check both fields to ensure the value is correct
    for (int i = 1; i < numLoops; i++) {
      final String query = id + ":" + i;
      QueryResponse qres = queryServer(new SolrQuery(query));
      assertEquals(chain + ": query failed: " + query, 
                   0, qres.getStatus());
      assertEquals(chain + ": didn't find correct # docs with query: " + query,
                   1, qres.getResults().getNumFound());
      SolrDocument doc = qres.getResults().get(0);

      for (String field : new String[] {fieldA, fieldB}) { 
        assertEquals(chain + ": doc#" + i+ " has wrong value for " + field,
                     expected, doc.getFirstValue(field));
      }
    }

  }

  // cloud level test mainly needed just to make sure that versions and errors are propagated correctly
  private void doOptimisticLockingAndUpdating() throws Exception {
    printLayout();
    
    SolrInputDocument sd =  sdoc("id", 1000, "_version_", -1);
    indexDoc(sd);

    ignoreException("version conflict");
    for (SolrServer client : clients) {
      try {
        client.add(sd);
        fail();
      } catch (SolrException e) {
        assertEquals(409, e.code());
      }
    }
    unIgnoreException("version conflict");

    // TODO: test deletes.  SolrJ needs a good way to pass version for delete...

    sd =  sdoc("id", 1000, "foo_i",5);
    clients.get(0).add(sd);

    List<Integer> expected = new ArrayList<Integer>();
    int val = 0;
    for (SolrServer client : clients) {
      val += 10;
      client.add(sdoc("id", 1000, "val_i", map("add",val), "foo_i",val));
      expected.add(val);
    }

    QueryRequest qr = new QueryRequest(params("qt", "/get", "id","1000"));
    for (SolrServer client : clients) {
      val += 10;
      NamedList rsp = client.request(qr);
      String match = JSONTestUtil.matchObj("/val_i", rsp.get("doc"), expected);
      if (match != null) throw new RuntimeException(match);
    }
  }

  private void testNumberOfCommitsWithCommitAfterAdd()
      throws SolrServerException, IOException {
    long startCommits = getNumCommits((HttpSolrServer) clients.get(0));
    
    ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update");
    up.addFile(getFile("books_numeric_ids.csv"), "application/csv");
    up.setCommitWithin(900000);
    up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    NamedList<Object> result = clients.get(0).request(up);
    
    long endCommits = getNumCommits((HttpSolrServer) clients.get(0));

    assertEquals(startCommits + 1L, endCommits);
  }

  private Long getNumCommits(HttpSolrServer solrServer) throws
      SolrServerException, IOException {
    HttpSolrServer server = new HttpSolrServer(solrServer.getBaseURL());
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/mbeans?key=updateHandler&stats=true");
    // use generic request to avoid extra processing of queries
    QueryRequest req = new QueryRequest(params);
    NamedList<Object> resp = server.request(req);
    NamedList mbeans = (NamedList) resp.get("solr-mbeans");
    NamedList uhandlerCat = (NamedList) mbeans.get("UPDATEHANDLER");
    NamedList uhandler = (NamedList) uhandlerCat.get("updateHandler");
    NamedList stats = (NamedList) uhandler.get("stats");
    Long commits = (Long) stats.get("commits");

    return commits;
  }

  private void testANewCollectionInOneInstanceWithManualShardAssignement() throws Exception {
    System.clearProperty("numShards");
    List<SolrServer> collectionClients = new ArrayList<SolrServer>();
    SolrServer client = clients.get(0);
    otherCollectionClients.put(oneInstanceCollection2, collectionClients);
    final String baseUrl = ((HttpSolrServer) client).getBaseURL().substring(
        0,
        ((HttpSolrServer) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 1, "slice1");
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 2, "slice2");
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 3, "slice2");
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 4, "slice1");
    
   while (pending != null && pending.size() > 0) {
      
      Future<Request> future = completionService.take();
      pending.remove(future);
    }
    
    SolrServer client1 = createNewSolrServer(oneInstanceCollection2 + "1", baseUrl);
    SolrServer client2 = createNewSolrServer(oneInstanceCollection2 + "2", baseUrl);
    SolrServer client3 = createNewSolrServer(oneInstanceCollection2 + "3", baseUrl);
    SolrServer client4 = createNewSolrServer(oneInstanceCollection2 + "4", baseUrl);
    

    // no one should be recovering
    waitForRecoveriesToFinish(oneInstanceCollection2, getCommonCloudSolrServer().getZkStateReader(), false, true);
    
    assertAllActive(oneInstanceCollection2, getCommonCloudSolrServer().getZkStateReader());
    
    //printLayout();
    
   // TODO: enable when we don't falsely get slice1...
   // solrj.getZkStateReader().getLeaderUrl(oneInstanceCollection2, "slice1", 30000);
   // solrj.getZkStateReader().getLeaderUrl(oneInstanceCollection2, "slice2", 30000);
    client2.add(getDoc(id, "1")); 
    client3.add(getDoc(id, "2")); 
    client4.add(getDoc(id, "3")); 
    
    client1.commit();
    SolrQuery query = new SolrQuery("*:*");
    query.set("distrib", false);
    long oneDocs = client1.query(query).getResults().getNumFound();
    long twoDocs = client2.query(query).getResults().getNumFound();
    long threeDocs = client3.query(query).getResults().getNumFound();
    long fourDocs = client4.query(query).getResults().getNumFound();
    
    query.set("collection", oneInstanceCollection2);
    query.set("distrib", true);
    long allDocs = getCommonCloudSolrServer().query(query).getResults().getNumFound();
    
//    System.out.println("1:" + oneDocs);
//    System.out.println("2:" + twoDocs);
//    System.out.println("3:" + threeDocs);
//    System.out.println("4:" + fourDocs);
//    System.out.println("All Docs:" + allDocs);
    
//    assertEquals(oneDocs, threeDocs);
//    assertEquals(twoDocs, fourDocs);
//    assertNotSame(oneDocs, twoDocs);
    assertEquals(3, allDocs);
    
    // we added a role of none on these creates - check for it
    ZkStateReader zkStateReader = getCommonCloudSolrServer().getZkStateReader();
    zkStateReader.updateClusterState(true);
    Map<String,Slice> slices = zkStateReader.getClusterState().getSlices(oneInstanceCollection2);
    assertNotNull(slices);
    String roles = slices.get("slice1").getReplicasMap().values().iterator().next().getStr(ZkStateReader.ROLES_PROP);
    assertEquals("none", roles);
    
    
    ZkCoreNodeProps props = new ZkCoreNodeProps(getCommonCloudSolrServer().getZkStateReader().getClusterState().getLeader(oneInstanceCollection2, "slice1"));
    
    // now test that unloading a core gets us a new leader
    HttpSolrServer server = new HttpSolrServer(baseUrl);
    Unload unloadCmd = new Unload(true);
    unloadCmd.setCoreName(props.getCoreName());
    
    String leader = props.getCoreUrl();
    
    server.request(unloadCmd);
    
    int tries = 50;
    while (leader.equals(zkStateReader.getLeaderUrl(oneInstanceCollection2, "slice1", 10000))) {
      Thread.sleep(100);
      if (tries-- == 0) {
        fail("Leader never changed");
      }
    }

  }

  private void testSearchByCollectionName() throws SolrServerException {
    SolrServer client = clients.get(0);
    final String baseUrl = ((HttpSolrServer) client).getBaseURL().substring(
        0,
        ((HttpSolrServer) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    
    // the cores each have different names, but if we add the collection name to the url
    // we should get mapped to the right core
    SolrServer client1 = createNewSolrServer(oneInstanceCollection, baseUrl);
    SolrQuery query = new SolrQuery("*:*");
    long oneDocs = client1.query(query).getResults().getNumFound();
    assertEquals(3, oneDocs);
  }

  private void testANewCollectionInOneInstance() throws Exception {
    List<SolrServer> collectionClients = new ArrayList<SolrServer>();
    SolrServer client = clients.get(0);
    otherCollectionClients.put(oneInstanceCollection , collectionClients);
    final String baseUrl = ((HttpSolrServer) client).getBaseURL().substring(
        0,
        ((HttpSolrServer) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 1);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 2);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 3);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 4);
    
   while (pending != null && pending.size() > 0) {
      
      Future<Request> future = completionService.take();
      if (future == null) return;
      pending.remove(future);
    }
    
    SolrServer client1 = createNewSolrServer(oneInstanceCollection + "1", baseUrl);
    SolrServer client2 = createNewSolrServer(oneInstanceCollection + "2", baseUrl);
    SolrServer client3 = createNewSolrServer(oneInstanceCollection + "3", baseUrl);
    SolrServer client4 = createNewSolrServer(oneInstanceCollection + "4", baseUrl);
    
    waitForRecoveriesToFinish(oneInstanceCollection, getCommonCloudSolrServer().getZkStateReader(), false);
    assertAllActive(oneInstanceCollection, getCommonCloudSolrServer().getZkStateReader());
    
    client2.add(getDoc(id, "1")); 
    client3.add(getDoc(id, "2")); 
    client4.add(getDoc(id, "3")); 
    
    client1.commit();
    SolrQuery query = new SolrQuery("*:*");
    query.set("distrib", false);
    long oneDocs = client1.query(query).getResults().getNumFound();
    long twoDocs = client2.query(query).getResults().getNumFound();
    long threeDocs = client3.query(query).getResults().getNumFound();
    long fourDocs = client4.query(query).getResults().getNumFound();
    
    query.set("collection", oneInstanceCollection);
    query.set("distrib", true);
    long allDocs = getCommonCloudSolrServer().query(query).getResults().getNumFound();
    
//    System.out.println("1:" + oneDocs);
//    System.out.println("2:" + twoDocs);
//    System.out.println("3:" + threeDocs);
//    System.out.println("4:" + fourDocs);
//    System.out.println("All Docs:" + allDocs);
    
    assertEquals(3, allDocs);
  }

  private void createCollection(String collection,
      List<SolrServer> collectionClients, String baseUrl, int num) {
    createSolrCore(collection, collectionClients, baseUrl, num, null);
  }
  
  private void createSolrCore(final String collection,
      List<SolrServer> collectionClients, final String baseUrl, final int num,
      final String shardId) {
    Callable call = new Callable() {
      public Object call() {
        HttpSolrServer server;
        try {
          server = new HttpSolrServer(baseUrl);
          
          Create createCmd = new Create();
          createCmd.setRoles("none");
          createCmd.setCoreName(collection + num);
          createCmd.setCollection(collection);
          if (shardId == null) {
            createCmd.setNumShards(2);
          }
          createCmd.setDataDir(dataDir.getAbsolutePath() + File.separator
              + collection + num);
          if (shardId != null) {
            createCmd.setShardId(shardId);
          }
          server.request(createCmd);
        } catch (Exception e) {
          e.printStackTrace();
          //fail
        }
        return null;
      }
    };
    
    pending.add(completionService.submit(call));
 
    
    collectionClients.add(createNewSolrServer(collection, baseUrl));
  }

  private void testMultipleCollections() throws Exception {
    // create another 2 collections and search across them
    createNewCollection("collection2");
    createNewCollection("collection3");
    
    while (pending != null && pending.size() > 0) {
      
      Future<Request> future = completionService.take();
      if (future == null) return;
      pending.remove(future);
    }
    
    indexDoc("collection2", getDoc(id, "10000000")); 
    indexDoc("collection2", getDoc(id, "10000001")); 
    indexDoc("collection2", getDoc(id, "10000003")); 
    
    getCommonCloudSolrServer().setDefaultCollection("collection2");
    getCommonCloudSolrServer().add(getDoc(id, "10000004"));
    getCommonCloudSolrServer().setDefaultCollection(null);
    
    indexDoc("collection3", getDoc(id, "20000000"));
    indexDoc("collection3", getDoc(id, "20000001")); 

    getCommonCloudSolrServer().setDefaultCollection("collection3");
    getCommonCloudSolrServer().add(getDoc(id, "10000005"));
    getCommonCloudSolrServer().setDefaultCollection(null);
    
    otherCollectionClients.get("collection2").get(0).commit();
    otherCollectionClients.get("collection3").get(0).commit();

    getCommonCloudSolrServer().setDefaultCollection("collection1");
    long collection1Docs = getCommonCloudSolrServer().query(new SolrQuery("*:*")).getResults()

        .getNumFound();
    long collection2Docs = otherCollectionClients.get("collection2").get(0)
        .query(new SolrQuery("*:*")).getResults().getNumFound();
    long collection3Docs = otherCollectionClients.get("collection3").get(0)
        .query(new SolrQuery("*:*")).getResults().getNumFound();
    
    SolrQuery query = new SolrQuery("*:*");
    query.set("collection", "collection2,collection3");
    long found = clients.get(0).query(query).getResults().getNumFound();
    assertEquals(collection2Docs + collection3Docs, found);
    
    query = new SolrQuery("*:*");
    query.set("collection", "collection1,collection2,collection3");
    found = clients.get(0).query(query).getResults().getNumFound();
    assertEquals(collection1Docs + collection2Docs + collection3Docs, found);
    
    // try to search multiple with cloud client
    found = getCommonCloudSolrServer().query(query).getResults().getNumFound();
    assertEquals(collection1Docs + collection2Docs + collection3Docs, found);
    
    query.set("collection", "collection2,collection3");
    found = getCommonCloudSolrServer().query(query).getResults().getNumFound();
    assertEquals(collection2Docs + collection3Docs, found);
    
    query.set("collection", "collection3");
    found = getCommonCloudSolrServer().query(query).getResults().getNumFound();
    assertEquals(collection3Docs, found);
    
    query.remove("collection");
    found = getCommonCloudSolrServer().query(query).getResults().getNumFound();
    assertEquals(collection1Docs, found);
  }
  
  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }
  
  protected void indexDoc(String collection, SolrInputDocument doc) throws IOException, SolrServerException {
    List<SolrServer> clients = otherCollectionClients.get(collection);
    int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) % clients.size();
    SolrServer client = clients.get(which);
    client.add(doc);
  }
  
  private void createNewCollection(final String collection) throws InterruptedException {
    final List<SolrServer> collectionClients = new ArrayList<SolrServer>();
    otherCollectionClients.put(collection, collectionClients);
    int unique = 0;
    for (final SolrServer client : clients) {
      unique++;
      final String baseUrl = ((HttpSolrServer) client).getBaseURL()
          .substring(
              0,
              ((HttpSolrServer) client).getBaseURL().length()
                  - DEFAULT_COLLECTION.length() -1);
      final int frozeUnique = unique;
      Callable call = new Callable() {
        public Object call() {
          HttpSolrServer server;
          try {
            server = new HttpSolrServer(baseUrl);
            
            Create createCmd = new Create();
            createCmd.setCoreName(collection);
            createCmd.setDataDir(dataDir.getAbsolutePath() + File.separator
                + collection + frozeUnique);
            server.request(createCmd);

          } catch (Exception e) {
            e.printStackTrace();
            //fails
          }
          return null;
        }
      };
     
      collectionClients.add(createNewSolrServer(collection, baseUrl));
      pending.add(completionService.submit(call));
      while (pending != null && pending.size() > 0) {
        
        Future<Request> future = completionService.take();
        if (future == null) return;
        pending.remove(future);
      }
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

  volatile CloudSolrServer commondCloudSolrServer;
  private CloudSolrServer getCommonCloudSolrServer() {
    if (commondCloudSolrServer == null) {
      synchronized(this) {
        try {
          commondCloudSolrServer = new CloudSolrServer(zkServer.getZkAddress());
          commondCloudSolrServer.setDefaultCollection(DEFAULT_COLLECTION);
          commondCloudSolrServer.connect();
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return commondCloudSolrServer;
  }

  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException {

    if (r.nextBoolean())
      return super.queryServer(params);

    if (r.nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp = getCommonCloudSolrServer().query(params);
    return rsp;
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (commondCloudSolrServer != null) {
      commondCloudSolrServer.shutdown();
    }
    System.clearProperty("numShards");
    System.clearProperty("zkHost");
    System.clearProperty("solr.xml.persist");
    
    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }
}
