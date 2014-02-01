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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Unload;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
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
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@Slow
public class BasicDistributedZkTest extends AbstractFullDistribZkTestBase {
  
  private static final String DEFAULT_COLLECTION = "collection1";
  protected static final boolean DEBUG = false;
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
  
  private AtomicInteger nodeCounter = new AtomicInteger();
  
  ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;
  
  @BeforeClass
  public static void beforeThisClass2() throws Exception {
  }
  
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
    completionService = new ExecutorCompletionService<Object>(executor);
    pending = new HashSet<Future<Object>>();
    
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

    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    // make sure we have leaders for each shard
    for (int j = 1; j < sliceCount; j++) {
      zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 10000);
    }      // make sure we again have leaders for each shard
    
    waitForRecoveriesToFinish(false);
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    del("*:*");
    queryAndCompareShards(params("q", "*:*", "distrib", "false", "sanity_check", "is_empty"));

    // ask every individual replica of every shard to update+commit the same doc id
    // with an incrementing counter on each update+commit
    int foo_i_counter = 0;
    for (SolrServer server : clients) {
      foo_i_counter++;
      indexDoc(server, params("commit", "true"), // SOLR-4923
               sdoc(id,1, i1,100, tlong,100, "foo_i", foo_i_counter));
      // after every update+commit, check all the shards consistency
      queryAndCompareShards(params("q", "id:1", "distrib", "false", 
                                   "sanity_check", "non_distrib_id_1_lookup"));
      queryAndCompareShards(params("q", "id:1", 
                                   "sanity_check", "distrib_id_1_lookup"));
    }

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
    queryAndCompareShards(params("q", "*:*", 
                                 "sort", "id desc",
                                 "distrib", "false", 
                                 "sanity_check", "is_empty"));

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
    handle.put("track", SKIP);
    query(false, new Object[] {"q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true"});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true"});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS});
    query(false, new Object[] {"q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY});

    // try commitWithin
    long before = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("commitWithin", 10);
    add(cloudClient, params , getDoc("id", 300));
    
    long timeout = System.currentTimeMillis() + 15000;
    while (cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound() != before + 1) {
      if (timeout <= System.currentTimeMillis()) {
        fail("commitWithin did not work");
      }
      Thread.sleep(100);
    }
    
    for (SolrServer client : clients) {
      assertEquals("commitWithin did not work", before + 1, client.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");

    // would be better if these where all separate tests - but much, much
    // slower
    doOptimisticLockingAndUpdating();
    testShardParamVariations();
    testMultipleCollections();
    testANewCollectionInOneInstance();
    testSearchByCollectionName();
    testUpdateByCollectionName();
    testANewCollectionInOneInstanceWithManualShardAssignement();
    testNumberOfCommitsWithCommitAfterAdd();

    testUpdateProcessorsRunOnlyOnce("distrib-dup-test-chain-explicit");
    testUpdateProcessorsRunOnlyOnce("distrib-dup-test-chain-implicit");

    testStopAndStartCoresInOneInstance();
    testFailedCoreCreateCleansUp();
    // Thread.sleep(10000000000L);
    if (DEBUG) {
      super.printLayout();
    }
  }
  
  private void testFailedCoreCreateCleansUp() throws Exception {
    Create createCmd = new Create();
    createCmd.setCoreName("core1");
    createCmd.setCollection("the_core_collection");
    String coredataDir = dataDir.getAbsolutePath() + File.separator
        + System.currentTimeMillis() + "the_core_collection";
    createCmd.setDataDir(coredataDir);
    createCmd.setNumShards(1);
    createCmd.setSchemaName("nonexistent_schema.xml");
    
    String url = getBaseUrl(clients.get(0));
    final HttpSolrServer server = new HttpSolrServer(url);
    try {
      server.request(createCmd);
      fail("Expected SolrCore create to fail");
    } catch (Exception e) {
      
    } finally {
      server.shutdown();
    }
    
    long timeout = System.currentTimeMillis() + 15000;
    while (cloudClient.getZkStateReader().getZkClient().exists("/collections/the_core_collection", true)) {
      if (timeout <= System.currentTimeMillis()) {
        fail(cloudClient.getZkStateReader().getZkClient().getChildren("/collections", null, true).toString() + " Collection zk node still exists");
      }
      Thread.sleep(100);
    }
    
    
    assertFalse("Collection zk node still exists", cloudClient.getZkStateReader().getZkClient().exists("/collections/the_core_collection", true));
  }
  
  private void testShardParamVariations() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    Map<String,Long> shardCounts = new HashMap<String,Long>();

    for (String shard : shardToJetty.keySet()) {
      // every client should give the same numDocs for this shard
      // shffle the clients in a diff order for each shard
      List<SolrServer> solrclients = new ArrayList<SolrServer>(this.clients);
      Collections.shuffle(solrclients, random());
      for (SolrServer client : solrclients) {
        query.set("shards", shard);
        long numDocs = client.query(query).getResults().getNumFound();
        assertTrue("numDocs < 0 for shard "+shard+" via "+client,
                   0 <= numDocs);
        if (!shardCounts.containsKey(shard)) {
          shardCounts.put(shard, numDocs);
        }
        assertEquals("inconsitent numDocs for shard "+shard+" via "+client,
                     shardCounts.get(shard).longValue(), numDocs);
        
        List<CloudJettyRunner> replicaJetties 
          = new ArrayList<CloudJettyRunner>(shardToJetty.get(shard));
        Collections.shuffle(replicaJetties, random());

        // each replica should also give the same numDocs
        ArrayList<String> replicaAlts = new ArrayList<String>(replicaJetties.size() * 2);
        for (CloudJettyRunner replicaJetty : shardToJetty.get(shard)) {
          String replica = removeProtocol(replicaJetty.url);
          query.set("shards", replica);

          // replicas already shuffled, use this in the alternative check below
          if (0 == random().nextInt(3) || replicaAlts.size() < 2) {
            replicaAlts.add(replica);
          }

          numDocs = client.query(query).getResults().getNumFound();
          assertTrue("numDocs < 0 for replica "+replica+" via "+client,
                     0 <= numDocs);
          assertEquals("inconsitent numDocs for shard "+shard+
                       " in replica "+replica+" via "+client,
                       shardCounts.get(shard).longValue(), numDocs);
        }

        // any combination of replica alternatives should give same numDocs
        String replicas = StringUtils.join(replicaAlts.toArray(), "|");
        query.set("shards", replicas);
        numDocs = client.query(query).getResults().getNumFound();
        assertTrue("numDocs < 0 for replicas "+replicas+" via "+client,
                   0 <= numDocs);
          assertEquals("inconsitent numDocs for replicas "+replicas+
                       " via "+client,
                       shardCounts.get(shard).longValue(), numDocs);
      }
    }

    // sums of multiple shards should add up regardless of how we 
    // query those shards or which client we use
    long randomShardCountsExpected = 0;
    ArrayList<String> randomShards = new ArrayList<String>(shardCounts.size());
    for (Map.Entry<String,Long> shardData : shardCounts.entrySet()) {
      if (random().nextBoolean() || randomShards.size() < 2) {
        String shard = shardData.getKey();
        randomShardCountsExpected += shardData.getValue();
        if (random().nextBoolean()) {
          // use shard id
          randomShards.add(shard);
        } else {
          // use some set explicit replicas
          ArrayList<String> replicas = new ArrayList<String>(7);
          for (CloudJettyRunner replicaJetty : shardToJetty.get(shard)) {
            if (0 == random().nextInt(3) || 0 == replicas.size()) {
              replicas.add(removeProtocol(replicaJetty.url));
            }
          }
          Collections.shuffle(replicas, random());
          randomShards.add(StringUtils.join(replicas, "|"));
        }
      }
    }
    String randShards = StringUtils.join(randomShards, ",");
    query.set("shards", randShards);
    for (SolrServer client : this.clients) {
      assertEquals("numDocs for "+randShards+" via "+client,
                   randomShardCountsExpected, 
                   client.query(query).getResults().getNumFound());
    }

    // total num docs must match sum of every shard's numDocs
    query = new SolrQuery("*:*");
    long totalShardNumDocs = 0;
    for (Long c : shardCounts.values()) {
      totalShardNumDocs += c;
    }
    for (SolrServer client : clients) {
      assertEquals("sum of shard numDocs on client: " + client, 
                   totalShardNumDocs,
                   client.query(query).getResults().getNumFound());
    }
    assertTrue("total numDocs <= 0, WTF? Test is useless",
               0 < totalShardNumDocs);

  }

  private void testStopAndStartCoresInOneInstance() throws Exception {
    SolrServer client = clients.get(0);
    String url3 = getBaseUrl(client);
    final HttpSolrServer server = new HttpSolrServer(url3);
    server.setConnectionTimeout(15000);
    server.setSoTimeout(60000);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
        5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new DefaultSolrThreadFactory("testExecutor"));
    int cnt = 3;
    
    // create the cores
    createCores(server, executor, "multiunload2", 1, cnt);
    
    executor.shutdown();
    executor.awaitTermination(120, TimeUnit.SECONDS);
    server.shutdown();
    
    ChaosMonkey.stop(cloudJettys.get(0).jetty);
    printLayout();

    Thread.sleep(5000);
    ChaosMonkey.start(cloudJettys.get(0).jetty);
    cloudClient.getZkStateReader().updateClusterState(true);
    try {
      cloudClient.getZkStateReader().getLeaderRetry("multiunload2", "shard1", 30000);
    } catch (SolrException e) {
      printLayout();
      throw e;
    }
    
    printLayout();

  }

  protected void createCores(final HttpSolrServer server,
      ThreadPoolExecutor executor, final String collection, final int numShards, int cnt) {
    for (int i = 0; i < cnt; i++) {
      final int freezeI = i;
      executor.execute(new Runnable() {
        
        @Override
        public void run() {
          Create createCmd = new Create();
          createCmd.setCoreName(collection + freezeI);
          createCmd.setCollection(collection);

          createCmd.setNumShards(numShards);
          try {
            String core3dataDir = dataDir.getAbsolutePath() + File.separator
                + System.currentTimeMillis() + collection + "_3n" + freezeI;
            createCmd.setDataDir(getDataDir(core3dataDir));

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

  protected String getBaseUrl(SolrServer client) {
    String url2 = ((HttpSolrServer) client).getBaseURL()
        .substring(
            0,
            ((HttpSolrServer) client).getBaseURL().length()
                - DEFAULT_COLLECTION.length() -1);
    return url2;
  }

  protected void createCollection(Map<String,List<Integer>> collectionInfos,
      String collectionName, int numShards, int numReplicas, int maxShardsPerNode, SolrServer client, String createNodeSetStr) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set(OverseerCollectionProcessor.NUM_SLICES, numShards);
    params.set(OverseerCollectionProcessor.REPLICATION_FACTOR, numReplicas);
    params.set(OverseerCollectionProcessor.MAX_SHARDS_PER_NODE, maxShardsPerNode);
    if (createNodeSetStr != null) params.set(OverseerCollectionProcessor.CREATE_NODE_SET, createNodeSetStr);

    int clientIndex = clients.size() > 1 ? random().nextInt(2) : 0;
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
      final String baseUrl = ((HttpSolrServer) clients.get(clientIndex)).getBaseURL().substring(
          0,
          ((HttpSolrServer) clients.get(clientIndex)).getBaseURL().length()
              - DEFAULT_COLLECTION.length() - 1);
      
      SolrServer aClient = createNewSolrServer("", baseUrl);
      aClient.request(request);
      aClient.shutdown();
    } else {
      client.request(request);
    }
  }
  
  protected ZkCoreNodeProps getLeaderUrlFromZk(String collection, String slice) {
    ClusterState clusterState = getCommonCloudSolrServer().getZkStateReader().getClusterState();
    ZkNodeProps leader = clusterState.getLeader(collection, slice);
    if (leader == null) {
      throw new RuntimeException("Could not find leader:" + collection + " " + slice);
    }
    return new ZkCoreNodeProps(leader);
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
    log.info("### STARTING doOptimisticLockingAndUpdating");
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
    log.info("### STARTING testNumberOfCommitsWithCommitAfterAdd");
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
    server.setConnectionTimeout(15000);
    server.setSoTimeout(60000);
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
    server.shutdown();
    return commits;
  }

  private void testANewCollectionInOneInstanceWithManualShardAssignement() throws Exception {
    log.info("### STARTING testANewCollectionInOneInstanceWithManualShardAssignement");
    System.clearProperty("numShards");
    List<SolrServer> collectionClients = new ArrayList<SolrServer>();
    SolrServer client = clients.get(0);
    final String baseUrl = ((HttpSolrServer) client).getBaseURL().substring(
        0,
        ((HttpSolrServer) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 1, "slice1");
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 2, "slice2");
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 3, "slice2");
    createSolrCore(oneInstanceCollection2, collectionClients, baseUrl, 4, "slice1");
    
   while (pending != null && pending.size() > 0) {
      
      Future<Object> future = completionService.take();
      pending.remove(future);
    }
    
    SolrServer client1 = collectionClients.get(0);
    SolrServer client2 = collectionClients.get(1);
    SolrServer client3 = collectionClients.get(2);
    SolrServer client4 = collectionClients.get(3);
    

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
    Map<String,Slice> slices = zkStateReader.getClusterState().getSlicesMap(oneInstanceCollection2);
    assertNotNull(slices);
    String roles = slices.get("slice1").getReplicasMap().values().iterator().next().getStr(ZkStateReader.ROLES_PROP);
    assertEquals("none", roles);
    
    
    ZkCoreNodeProps props = new ZkCoreNodeProps(getCommonCloudSolrServer().getZkStateReader().getClusterState().getLeader(oneInstanceCollection2, "slice1"));
    
    // now test that unloading a core gets us a new leader
    HttpSolrServer server = new HttpSolrServer(baseUrl);
    server.setConnectionTimeout(15000);
    server.setSoTimeout(60000);
    Unload unloadCmd = new Unload(true);
    unloadCmd.setCoreName(props.getCoreName());
    
    String leader = props.getCoreUrl();
    
    server.request(unloadCmd);
    server.shutdown();
    
    int tries = 50;
    while (leader.equals(zkStateReader.getLeaderUrl(oneInstanceCollection2, "slice1", 10000))) {
      Thread.sleep(100);
      if (tries-- == 0) {
        fail("Leader never changed");
      }
    }

    for (SolrServer aClient : collectionClients) {
      aClient.shutdown();
    }

  }

  private void testSearchByCollectionName() throws SolrServerException {
    log.info("### STARTING testSearchByCollectionName");
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
    client1.shutdown();
  }
  
  private void testUpdateByCollectionName() throws SolrServerException, IOException {
    log.info("### STARTING testUpdateByCollectionName");
    SolrServer client = clients.get(0);
    final String baseUrl = ((HttpSolrServer) client).getBaseURL().substring(
        0,
        ((HttpSolrServer) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    
    // the cores each have different names, but if we add the collection name to the url
    // we should get mapped to the right core
    // test hitting an update url
    SolrServer client1 = createNewSolrServer(oneInstanceCollection, baseUrl);
    client1.commit();
    client1.shutdown();
  }

  private void testANewCollectionInOneInstance() throws Exception {
    log.info("### STARTING testANewCollectionInOneInstance");
    List<SolrServer> collectionClients = new ArrayList<SolrServer>();
    SolrServer client = clients.get(0);
    final String baseUrl = ((HttpSolrServer) client).getBaseURL().substring(
        0,
        ((HttpSolrServer) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 1);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 2);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 3);
    createCollection(oneInstanceCollection, collectionClients, baseUrl, 4);
    
   while (pending != null && pending.size() > 0) {
      
      Future<Object> future = completionService.take();
      if (future == null) return;
      pending.remove(future);
    }
   
    SolrServer client1 = collectionClients.get(0);
    SolrServer client2 = collectionClients.get(1);
    SolrServer client3 = collectionClients.get(2);
    SolrServer client4 = collectionClients.get(3);
 
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
    for(SolrServer newCollectionClient:collectionClients) {
      newCollectionClient.shutdown();
    }
  }

  private void createCollection(String collection,
      List<SolrServer> collectionClients, String baseUrl, int num) {
    createSolrCore(collection, collectionClients, baseUrl, num, null);
  }
  
  private void createSolrCore(final String collection,
      List<SolrServer> collectionClients, final String baseUrl, final int num,
      final String shardId) {
    Callable call = new Callable() {
      @Override
      public Object call() {
        HttpSolrServer server = null;
        try {
          server = new HttpSolrServer(baseUrl);
          server.setConnectionTimeout(15000);
          Create createCmd = new Create();
          createCmd.setRoles("none");
          createCmd.setCoreName(collection + num);
          createCmd.setCollection(collection);
          
          if (random().nextBoolean()) {
            // sometimes we use an explicit core node name
            createCmd.setCoreNodeName("anode" + nodeCounter.incrementAndGet());
          }
          
          if (shardId == null) {
            createCmd.setNumShards(2);
          }
          createCmd.setDataDir(getDataDir(dataDir.getAbsolutePath() + File.separator
              + collection + num));
          if (shardId != null) {
            createCmd.setShardId(shardId);
          }
          server.request(createCmd);
        } catch (Exception e) {
          e.printStackTrace();
          //fail
        } finally {
          if (server != null) {
            server.shutdown();
          }
        }
        return null;
      }
    };
    
    pending.add(completionService.submit(call));
 
    
    collectionClients.add(createNewSolrServer(collection + num, baseUrl));
  }

  private void testMultipleCollections() throws Exception {
    log.info("### STARTING testMultipleCollections");
    // create another 2 collections and search across them
    createNewCollection("collection2");
    createNewCollection("collection3");
    
    while (pending != null && pending.size() > 0) {
      
      Future<Object> future = completionService.take();
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
    System.out.println("found2: "+ collection2Docs);
    long collection3Docs = otherCollectionClients.get("collection3").get(0)
        .query(new SolrQuery("*:*")).getResults().getNumFound();
    System.out.println("found3: "+ collection3Docs);
    
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
    
    assertEquals(collection3Docs, collection2Docs - 1);
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
        @Override
        public Object call() {
          HttpSolrServer server = null;
          try {
            server = new HttpSolrServer(baseUrl);
            server.setConnectionTimeout(15000);
            server.setSoTimeout(60000);
            Create createCmd = new Create();
            createCmd.setCoreName(collection);
            createCmd.setDataDir(getDataDir(dataDir.getAbsolutePath() + File.separator
                + collection + frozeUnique));

            server.request(createCmd);

          } catch (Exception e) {
            e.printStackTrace();
            //fails
          } finally {
            if (server != null) {
              server.shutdown();
            }
          }
          return null;
        }
      };
     
      collectionClients.add(createNewSolrServer(collection, baseUrl));
      pending.add(completionService.submit(call));
      while (pending != null && pending.size() > 0) {
        
        Future<Object> future = completionService.take();
        if (future == null) return;
        pending.remove(future);
      }
    }
  }
  
  protected SolrServer createNewSolrServer(String collection, String baseUrl) {
    try {
      // setup the server...
      HttpSolrServer s = new HttpSolrServer(baseUrl + "/" + collection);
      s.setSoTimeout(120000);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  volatile CloudSolrServer commondCloudSolrServer;
  protected CloudSolrServer getCommonCloudSolrServer() {
    if (commondCloudSolrServer == null) {
      synchronized(this) {
        try {
          commondCloudSolrServer = new CloudSolrServer(zkServer.getZkAddress(), random().nextBoolean());
          commondCloudSolrServer.setParallelUpdates(random().nextBoolean());
          commondCloudSolrServer.setDefaultCollection(DEFAULT_COLLECTION);
          commondCloudSolrServer.getLbServer().setConnectionTimeout(15000);
          commondCloudSolrServer.getLbServer().setSoTimeout(30000);
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
    if (otherCollectionClients != null) {
      for (List<SolrServer> clientList : otherCollectionClients.values()) {
        for (SolrServer client : clientList) {
          client.shutdown();
        }
      }
    }
    otherCollectionClients = null;
    List<Runnable> tasks = executor.shutdownNow();
    assertTrue(tasks.isEmpty());
    
    System.clearProperty("numShards");
    System.clearProperty("zkHost");
    System.clearProperty("solr.xml.persist");
    
    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }

  /**
   * Given a URL as a string, removes the leading protocol from that string
   */
  private static String removeProtocol(String url) {
    return url.replaceFirst("^[^:/]{1,20}:/+","");
  }
}
