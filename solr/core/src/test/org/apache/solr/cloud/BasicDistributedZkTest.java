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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Unload;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.StreamingUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
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
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TestInjection.Hook;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@Slow 
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class BasicDistributedZkTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEFAULT_COLLECTION = "collection1";

  private final boolean onlyLeaderIndexes = random().nextBoolean();
  
  String t1="a_t";
  String i1="a_i1";
  String tlong = "other_tl1";
  String tsort="t_sortable";

  String oddField="oddField_s";
  String missingField="ignore_exception__missing_but_valid_field_t";

  private Map<String,List<SolrClient>> otherCollectionClients = new HashMap<>();

  private String oneInstanceCollection = "oneInstanceCollection";
  private String oneInstanceCollection2 = "oneInstanceCollection2";
  
  private AtomicInteger nodeCounter = new AtomicInteger();
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;
  
  private static Hook newSearcherHook = new Hook() {
    volatile CountDownLatch latch;
    AtomicReference<String> collection = new AtomicReference<>();

    @Override
    public void newSearcher(String collectionName) {
      String c = collection.get();
      if (c  != null && c.equals(collectionName)) {
        log.info("Hook detected newSearcher");
        try {
          latch.countDown();
        } catch (NullPointerException e) {

        }
      }
    }
  
    public void waitForSearcher(String collection, int cnt, int timeoutms, boolean failOnTimeout) throws InterruptedException {
      latch = new CountDownLatch(cnt);
      this.collection.set(collection);
      boolean timeout = !latch.await(timeoutms, TimeUnit.MILLISECONDS);
      if (timeout && failOnTimeout) {
        fail("timed out waiting for new searcher event " + latch.getCount());
      }
    }
  
  };
  
  public BasicDistributedZkTest() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    
    sliceCount = 2;
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
    
  }
  
  @BeforeClass
  public static void beforeBDZKTClass() {
    TestInjection.newSearcherHook(newSearcherHook);
  }

  @Override
  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
  }

  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {

    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < getShardCount(); i++) {
        if (i > 0)
          sb.append(',');
        sb.append("shard" + (i + 3));
      }
      params.set("shards", sb.toString());
    }
  }

  @Test
  @ShardsFixed(num = 4)
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void test() throws Exception {
    // setLoggingLevel(null);

    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    // make sure we have leaders for each shard
    for (int j = 1; j < sliceCount; j++) {
      zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 10000);
    }      // make sure we again have leaders for each shard
    
    waitForRecoveriesToFinish(false);
    
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    del("*:*");
    queryAndCompareShards(params("q", "*:*", "distrib", "false", "sanity_check", "is_empty"));

    // ask every individual replica of every shard to update+commit the same doc id
    // with an incrementing counter on each update+commit
    int foo_i_counter = 0;
    for (SolrClient client : clients) {
      foo_i_counter++;
      indexDoc(client, params("commit", "true"), // SOLR-4923
               sdoc(id,1, i1,100, tlong,100, "foo_i", foo_i_counter));
      // after every update+commit, check all the shards consistency
      queryAndCompareShards(params("q", "id:1", "distrib", "false", 
                                   "sanity_check", "non_distrib_id_1_lookup"));
      queryAndCompareShards(params("q", "id:1", 
                                   "sanity_check", "distrib_id_1_lookup"));
    }

    indexr(id,1, i1, 100, tlong, 100,t1,"now is the time for all good men"
            ,"foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d, tsort, "now is the time for all good men");
    indexr(id, 2, i1, 50, tlong, 50, t1, "to come to the aid of their country."
        , tsort, "to come to the aid of their country.");
    indexr(id, 3, i1, 2, tlong, 2, t1, "how now brown cow", tsort, "how now brown cow");
    indexr(id, 4, i1, -100, tlong, 101, t1, "the quick fox jumped over the lazy dog"
        , tsort, "the quick fox jumped over the lazy dog");
    indexr(id, 5, i1, 500, tlong, 500, t1, "the quick fox jumped way over the lazy dog"
        , tsort, "the quick fox jumped over the lazy dog");
    indexr(id, 6, i1, -600, tlong, 600, t1, "humpty dumpy sat on a wall", tsort, "the quick fox jumped over the lazy dog");
    indexr(id, 7, i1, 123, tlong, 123, t1, "humpty dumpy had a great fall", tsort, "the quick fox jumped over the lazy dog");
    indexr(id,8, i1, 876, tlong, 876,t1,"all the kings horses and all the kings men",tsort,"all the kings horses and all the kings men");
    indexr(id, 9, i1, 7, tlong, 7, t1, "couldn't put humpty together again", tsort, "the quick fox jumped over the lazy dog");
    indexr(id,10, i1, 4321, tlong, 4321,t1,"this too shall pass",tsort,"this too shall pass");
    indexr(id,11, i1, -987, tlong, 987,t1,"An eye for eye only ends up making the whole world blind."
        ,tsort,"An eye for eye only ends up making the whole world blind.");
    indexr(id,12, i1, 379, tlong, 379,t1,"Great works are performed, not by strength, but by perseverance.",
        tsort,"Great works are performed, not by strength, but by perseverance.");
    indexr(id,13, i1, 232, tlong, 232,t1,"no eggs on wall, lesson learned", oddField, "odd man out",
        tsort,"no eggs on wall, lesson learned");

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

    testTokenizedGrouping();
    testSortableTextFaceting();
    testSortableTextSorting();
    testSortableTextGrouping();

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
    ,"fq","{!tag=a}id_i1:[1 TO 7]", "fq","{!tag=b}id_i1:[3 TO 9]"}
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
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG_QUERY, "true"});
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING});
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS});
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY});

    // try add commitWithin
    long before = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    for (SolrClient client : clients) {
      assertEquals("unexpected pre-commitWithin document count on node: " + ((HttpSolrClient)client).getBaseURL(), before, client.query(new SolrQuery("*:*")).getResults().getNumFound());
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("commitWithin", 10);
    add(cloudClient, params , getDoc("id", 300), getDoc("id", 301));

    newSearcherHook.waitForSearcher(DEFAULT_COLLECTION, 2, 20000, false);
    
    ClusterState clusterState = getCommonCloudSolrClient().getZkStateReader().getClusterState();
    DocCollection dColl = clusterState.getCollection(DEFAULT_COLLECTION);

    assertSliceCounts("should have found 2 docs, 300 and 301", before + 2, dColl);

    // try deleteById commitWithin
    UpdateRequest deleteByIdReq = new UpdateRequest();
    deleteByIdReq.deleteById("300");
    deleteByIdReq.setCommitWithin(10);
    deleteByIdReq.process(cloudClient);
    
    newSearcherHook.waitForSearcher(DEFAULT_COLLECTION, 2, 20000, false);

    assertSliceCounts("deleteById commitWithin did not work", before + 1, dColl);
    
    // try deleteByQuery commitWithin
    UpdateRequest deleteByQueryReq = new UpdateRequest();
    deleteByQueryReq.deleteByQuery("id:301");
    deleteByQueryReq.setCommitWithin(10);
    deleteByQueryReq.process(cloudClient);

    newSearcherHook.waitForSearcher(DEFAULT_COLLECTION, 2, 20000, false);
    
    assertSliceCounts("deleteByQuery commitWithin did not work", before, dColl);
    

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
  }

  private void testSortableTextFaceting() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.addFacetField(tsort);
    query.setFacetMissing(false);
    QueryResponse resp = queryServer(query);
    List<FacetField> ffs = resp.getFacetFields();
    for (FacetField ff : ffs) {
      if (ff.getName().equals(tsort) == false) continue;
      for (FacetField.Count count : ff.getValues()) {
        long num = count.getCount();
        switch (count.getName()) {
          case "all the kings horses and all the kings men":
          case "An eye for eye only ends up making the whole world blind.":
          case "Great works are performed, not by strength, but by perseverance.":
          case "how now brown cow":
          case "no eggs on wall, lesson learned":
          case "now is the time for all good men":
          case "this too shall pass":
          case "to come to the aid of their country.":
            assertEquals("Should have exactly one facet count for field " + ff.getName(), 1, num);
            break;
          case "the quick fox jumped over the lazy dog":
            assertEquals("Should have 5 docs for the lazy dog", 5, num);
            break;
          default:
            fail("No case for facet '" + ff.getName() + "'");

        }
      }
    }
  }

  private void testSortableTextSorting() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.addSort(tsort, SolrQuery.ORDER.desc);
    query.addField("*");
    query.addField("eoe_sortable");
    query.addField(tsort);
    QueryResponse resp = queryServer(query);

    SolrDocumentList docs = resp.getResults();

    String title = docs.get(0).getFieldValue(tsort).toString();
    for (SolrDocument doc : docs) {
      assertTrue("Docs should be back in sorted order, descending", title.compareTo(doc.getFieldValue(tsort).toString()) >= 0);
      title = doc.getFieldValue(tsort).toString();
    }
  }

  private void testSortableTextGrouping() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.add("group", "true");
    query.add("group.field", tsort);
    QueryResponse resp = queryServer(query);
    GroupResponse groupResp = resp.getGroupResponse();
    List<GroupCommand> grpCmds = groupResp.getValues();
    for (GroupCommand grpCmd : grpCmds) {
      if (grpCmd.getName().equals(tsort) == false) continue;
      for (Group grp : grpCmd.getValues()) {
        long count = grp.getResult().getNumFound();
        if (grp.getGroupValue() == null) continue; // Don't count the groups without an entry as the numnber is variable
        switch (grp.getGroupValue()) {
          case "all the kings horses and all the kings men":
          case "An eye for eye only ends up making the whole world blind.":
          case "Great works are performed, not by strength, but by perseverance.":
          case "how now brown cow":
          case "no eggs on wall, lesson learned":
          case "now is the time for all good men":
          case "this too shall pass":
          case "to come to the aid of their country.":
            assertEquals("Should have exactly one facet count for field " + grpCmd.getName(), 1, count);
            break;
          case "the quick fox jumped over the lazy dog":
            assertEquals("Should have 5 docs for the lazy dog", 5, count);
            break;
          default:
            fail("No case for facet '" + grpCmd.getName() + "'");

        }
      }
    }
  }

  private void testTokenizedGrouping() throws Exception {
    SolrException ex = expectThrows(SolrException.class, () -> {
      query(false, new String[]{"q", "*:*", "group", "true", "group.field", t1});
    });
    assertTrue("Expected error from server that SortableTextFields are required", ex.getMessage().contains("Sorting on a tokenized field that is not a SortableTextField is not supported in cloud mode"));
  }

  private void assertSliceCounts(String msg, long expected, DocCollection dColl) throws Exception {
    long found = checkSlicesSameCounts(dColl);
    
    if (found != expected) {
      // we get one do over in a bad race
      Thread.sleep(1000);
      found = checkSlicesSameCounts(dColl);
    }
    
    assertEquals(msg, expected, checkSlicesSameCounts(dColl));
  }

  // Ensure that total docs found is the expected number.
  private void waitForDocCount(long expectedNumFound, long waitMillis, String failureMessage)
      throws Exception {
    AtomicLong total = new AtomicLong(-1);
    try {
      getCommonCloudSolrClient().getZkStateReader().waitForState(DEFAULT_COLLECTION, waitMillis, TimeUnit.MILLISECONDS, (n, c) -> {
        long docTotal;
        try {
          docTotal = checkSlicesSameCounts(c);
        } catch (SolrServerException | IOException e) {
          throw new RuntimeException(e);
        }
        total.set(docTotal);
        if (docTotal == expectedNumFound) {
          return true;
        }
        return false;
      });
    } catch (TimeoutException | InterruptedException e) {
     
    }
    // We could fail here if we broke out of the above because we exceeded the time allowed.
    assertEquals(failureMessage, expectedNumFound, total.get());

    // This should be redundant, but it caught a test error after all.
    for (SolrClient client : clients) {
      assertEquals(failureMessage, expectedNumFound, client.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
  }

  // Insure that counts are the same for all replicas in each shard
  // Return the total doc count for the query.
  private long checkSlicesSameCounts(DocCollection dColl) throws SolrServerException, IOException {
    long docTotal = 0; // total number of documents found counting only one replica per slice.
    for (Slice slice : dColl.getActiveSlices()) {
      long sliceDocCount = -1;
      for (Replica rep : slice.getReplicas()) {
        try (HttpSolrClient one = getHttpSolrClient(rep.getCoreUrl())) {
          SolrQuery query = new SolrQuery("*:*");
          query.setDistrib(false);
          QueryResponse resp = one.query(query);
          long hits = resp.getResults().getNumFound();
          if (sliceDocCount == -1) {
            sliceDocCount = hits;
            docTotal += hits;
          } else {
            if (hits != sliceDocCount) {
              return -1;
            }
          }
        }
      }
    }
    return docTotal;
  }

  private void testShardParamVariations() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    Map<String,Long> shardCounts = new HashMap<>();

    for (String shard : shardToJetty.keySet()) {
      // every client should give the same numDocs for this shard
      // shffle the clients in a diff order for each shard
      List<SolrClient> solrclients = new ArrayList<>(this.clients);
      Collections.shuffle(solrclients, random());
      for (SolrClient client : solrclients) {
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
          = new ArrayList<>(shardToJetty.get(shard));
        Collections.shuffle(replicaJetties, random());

        // each replica should also give the same numDocs
        ArrayList<String> replicaAlts = new ArrayList<>(replicaJetties.size() * 2);
        for (CloudJettyRunner replicaJetty : shardToJetty.get(shard)) {
          String replica = replicaJetty.url;
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
        String replicas = String.join("|", replicaAlts);
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
    ArrayList<String> randomShards = new ArrayList<>(shardCounts.size());
    for (Map.Entry<String,Long> shardData : shardCounts.entrySet()) {
      if (random().nextBoolean() || randomShards.size() < 2) {
        String shard = shardData.getKey();
        randomShardCountsExpected += shardData.getValue();
        if (random().nextBoolean()) {
          // use shard id
          randomShards.add(shard);
        } else {
          // use some set explicit replicas
          ArrayList<String> replicas = new ArrayList<>(7);
          for (CloudJettyRunner replicaJetty : shardToJetty.get(shard)) {
            if (0 == random().nextInt(3) || 0 == replicas.size()) {
              replicas.add(replicaJetty.url);
            }
          }
          Collections.shuffle(replicas, random());
          randomShards.add(String.join("|", replicas));
        }
      }
    }
    String randShards = String.join(",", randomShards);
    query.set("shards", randShards);
    for (SolrClient client : this.clients) {
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
    for (SolrClient client : clients) {
      assertEquals("sum of shard numDocs on client: " + client, 
                   totalShardNumDocs,
                   client.query(query).getResults().getNumFound());
    }
    assertTrue("total numDocs <= 0, WTF? Test is useless",
        0 < totalShardNumDocs);

  }

  private void testStopAndStartCoresInOneInstance() throws Exception {
    JettySolrRunner jetty = jettys.get(0);
    try (final HttpSolrClient httpSolrClient = (HttpSolrClient) jetty.newClient(15000, 60000)) {
      ThreadPoolExecutor executor = null;
      try {
        executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
            5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
            new DefaultSolrThreadFactory("testExecutor"));
        int cnt = 3;

        // create the cores
        createCollectionInOneInstance(httpSolrClient, jetty.getNodeName(), executor, "multiunload2", 1, cnt);
      } finally {
        if (executor != null) {
          ExecutorUtil.shutdownAndAwaitTermination(executor);
        }
      }
    }
    
    cloudJettys.get(0).jetty.stop();
    printLayout();

    cloudJettys.get(0).jetty.start();
    cloudClient.getZkStateReader().forceUpdateCollection("multiunload2");
    try {
      cloudClient.getZkStateReader().getLeaderRetry("multiunload2", "shard1", 30000);
    } catch (SolrException e) {
      printLayout();
      throw e;
    }
    
    printLayout();

  }

  /**
   * Create a collection in single node
   */
  protected void createCollectionInOneInstance(final SolrClient client, String nodeName,
                                               ThreadPoolExecutor executor, final String collection,
                                               final int numShards, int numReplicas) {
    assertNotNull(nodeName);
    try {
      assertEquals(0, CollectionAdminRequest.createCollection(collection, "conf1", numShards, 1)
          .setCreateNodeSet("")
          .process(client).getStatus());
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < numReplicas; i++) {
      final int freezeI = i;
      executor.execute(() -> {
        try {
          assertTrue(CollectionAdminRequest.addReplicaToShard(collection, "shard"+((freezeI%numShards)+1))
              .setCoreName(collection + freezeI)
              .setNode(nodeName).process(client).isSuccess());
        } catch (SolrServerException | IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  protected String getBaseUrl(SolrClient client) {
    String url2 = ((HttpSolrClient) client).getBaseURL()
        .substring(
            0,
            ((HttpSolrClient) client).getBaseURL().length()
                - DEFAULT_COLLECTION.length() -1);
    return url2;
  }

  @Override
  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos,
                                                     String collectionName, String configSetName, int numShards, int numReplicas, int maxShardsPerNode, SolrClient client, String createNodeSetStr) throws SolrServerException, IOException {
    // TODO: Use CollectionAdminRequest for this test
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());

    params.set(OverseerCollectionMessageHandler.NUM_SLICES, numShards);
    params.set(ZkStateReader.REPLICATION_FACTOR, numReplicas);
    params.set(ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode);
    if (createNodeSetStr != null) params.set(OverseerCollectionMessageHandler.CREATE_NODE_SET, createNodeSetStr);

    int clientIndex = clients.size() > 1 ? random().nextInt(2) : 0;
    List<Integer> list = new ArrayList<>();
    list.add(numShards);
    list.add(numReplicas);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    params.set("collection.configName", configSetName);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    if (client == null) {
      final String baseUrl = ((HttpSolrClient) clients.get(clientIndex)).getBaseURL().substring(
          0,
          ((HttpSolrClient) clients.get(clientIndex)).getBaseURL().length()
              - DEFAULT_COLLECTION.length() - 1);
      
      try (SolrClient aClient = createNewSolrClient("", baseUrl)) {
        res.setResponse(aClient.request(request));
      }
    } else {
      res.setResponse(client.request(request));
    }
    return res;
  }
  
  protected ZkCoreNodeProps getLeaderUrlFromZk(String collection, String slice) {
    ClusterState clusterState = getCommonCloudSolrClient().getZkStateReader().getClusterState();
    ZkNodeProps leader = clusterState.getCollection(collection).getLeader(slice);
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
      SolrClient updateClient = clients.get(random().nextInt(clients.size()));
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
    
    final SolrInputDocument sd =  sdoc("id", 1000, "_version_", -1);
    indexDoc(sd);

    ignoreException("version conflict");
    for (SolrClient client : clients) {
      SolrException e = expectThrows(SolrException.class, () -> client.add(sd));
      assertEquals(409, e.code());
    }
    unIgnoreException("version conflict");

    // TODO: test deletes.  SolrJ needs a good way to pass version for delete...

    final SolrInputDocument sd2 =  sdoc("id", 1000, "foo_i",5);
    clients.get(0).add(sd2);

    List<Integer> expected = new ArrayList<>();
    int val = 0;
    for (SolrClient client : clients) {
      val += 10;
      client.add(sdoc("id", 1000, "val_i", map("add",val), "foo_i",val));
      expected.add(val);
    }

    QueryRequest qr = new QueryRequest(params("qt", "/get", "id","1000"));
    for (SolrClient client : clients) {
      val += 10;
      NamedList rsp = client.request(qr);
      String match = JSONTestUtil.matchObj("/val_i", rsp.get("doc"), expected);
      if (match != null) throw new RuntimeException(match);
    }
  }

  private void testNumberOfCommitsWithCommitAfterAdd()
      throws SolrServerException, IOException {
    log.info("### STARTING testNumberOfCommitsWithCommitAfterAdd");
    long startCommits = getNumCommits((HttpSolrClient) clients.get(0));


    NamedList<Object> result = clients.get(0).request(
        new StreamingUpdateRequest("/update",
            getFile("books_numeric_ids.csv"), "application/csv")
            .setCommitWithin(900000)
            .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true));
    
    long endCommits = getNumCommits((HttpSolrClient) clients.get(0));

    assertEquals(startCommits + 1L, endCommits);
  }

  private Long getNumCommits(HttpSolrClient sourceClient) throws
      SolrServerException, IOException {
    // construct the /admin/metrics URL
    URL url = new URL(sourceClient.getBaseURL());
    String path = url.getPath().substring(1);
    String[] elements = path.split("/");
    String collection = elements[elements.length - 1];
    String urlString = url.toString();
    urlString = urlString.substring(0, urlString.length() - collection.length() - 1);
    try (HttpSolrClient client = getHttpSolrClient(urlString, 15000, 60000)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      //params.set("qt", "/admin/metrics?prefix=UPDATE.updateHandler&registry=solr.core." + collection);
      params.set("qt", "/admin/metrics");
      params.set("prefix", "UPDATE.updateHandler");
      params.set("registry", "solr.core." + collection);
      // use generic request to avoid extra processing of queries
      QueryRequest req = new QueryRequest(params);
      NamedList<Object> resp = client.request(req);
      NamedList metrics = (NamedList) resp.get("metrics");
      NamedList uhandlerCat = (NamedList) metrics.getVal(0);
      Map<String,Object> commits = (Map<String,Object>) uhandlerCat.get("UPDATE.updateHandler.commits");
      return (Long) commits.get("count");
    }
  }

  private void testANewCollectionInOneInstanceWithManualShardAssignement() throws Exception {
    log.info("### STARTING testANewCollectionInOneInstanceWithManualShardAssignement");
    assertEquals(0, CollectionAdminRequest.createCollection(oneInstanceCollection2, "conf1", 2, 2)
        .setCreateNodeSet("")
        .setMaxShardsPerNode(4)
        .process(cloudClient).getStatus());

    List<SolrClient> collectionClients = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      CollectionAdminResponse resp = CollectionAdminRequest
          .addReplicaToShard(oneInstanceCollection2, "shard" + ((i%2)+1))
          .setNode(jettys.get(0).getNodeName())
          .process(cloudClient);
      for (String coreName : resp.getCollectionCoresStatus().keySet()) {
        collectionClients.add(createNewSolrClient(coreName, jettys.get(0).getBaseUrl().toString()));
      }
      
      
    }
    
    SolrClient client1 = collectionClients.get(0);
    SolrClient client2 = collectionClients.get(1);
    SolrClient client3 = collectionClients.get(2);
    SolrClient client4 = collectionClients.get(3);
    

    // no one should be recovering
    waitForRecoveriesToFinish(oneInstanceCollection2, getCommonCloudSolrClient().getZkStateReader(), false, true);
    
    assertAllActive(oneInstanceCollection2, getCommonCloudSolrClient().getZkStateReader());
    
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
    long allDocs = getCommonCloudSolrClient().query(query).getResults().getNumFound();
    
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
    ZkStateReader zkStateReader = getCommonCloudSolrClient().getZkStateReader();
    zkStateReader.forceUpdateCollection(oneInstanceCollection2);
    Map<String,Slice> slices = zkStateReader.getClusterState().getCollection(oneInstanceCollection2).getSlicesMap();
    assertNotNull(slices);
    
    ZkCoreNodeProps props = new ZkCoreNodeProps(getCommonCloudSolrClient().getZkStateReader().getClusterState()
        .getCollection(oneInstanceCollection2).getLeader("shard1"));
    
    // now test that unloading a core gets us a new leader
    try (HttpSolrClient unloadClient = getHttpSolrClient(jettys.get(0).getBaseUrl().toString(), 15000, 60000)) {
      Unload unloadCmd = new Unload(true);
      unloadCmd.setCoreName(props.getCoreName());

      String leader = props.getCoreUrl();
      
      testExecutor.execute(new Runnable() {
        
        @Override
        public void run() {
          try {
            unloadClient.request(unloadCmd);
          } catch (SolrServerException e) {
            throw new RuntimeException(e);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });

      try {
        getCommonCloudSolrClient().getZkStateReader().waitForState(oneInstanceCollection2, 20000, TimeUnit.MILLISECONDS, (n, c) -> {
          
 
          try {
            if (leader.equals(zkStateReader.getLeaderUrl(oneInstanceCollection2, "shard1", 10000))) {
              return false;
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return true;
        });
      } catch (TimeoutException | InterruptedException e) {
        fail("Leader never changed");
      }
    }

    IOUtils.close(collectionClients);

  }

  private void testSearchByCollectionName() throws SolrServerException, IOException {
    log.info("### STARTING testSearchByCollectionName");
    SolrClient client = clients.get(0);
    final String baseUrl = ((HttpSolrClient) client).getBaseURL().substring(
        0,
        ((HttpSolrClient) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    
    // the cores each have different names, but if we add the collection name to the url
    // we should get mapped to the right core
    try (SolrClient client1 = createNewSolrClient(oneInstanceCollection, baseUrl)) {
      SolrQuery query = new SolrQuery("*:*");
      long oneDocs = client1.query(query).getResults().getNumFound();
      assertEquals(3, oneDocs);
    }
  }
  
  private void testUpdateByCollectionName() throws SolrServerException, IOException {
    log.info("### STARTING testUpdateByCollectionName");
    SolrClient client = clients.get(0);
    final String baseUrl = ((HttpSolrClient) client).getBaseURL().substring(
        0,
        ((HttpSolrClient) client).getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
    
    // the cores each have different names, but if we add the collection name to the url
    // we should get mapped to the right core
    // test hitting an update url
    try (SolrClient client1 = createNewSolrClient(oneInstanceCollection, baseUrl)) {
      client1.commit();
    }
  }

  private void testANewCollectionInOneInstance() throws Exception {
    log.info("### STARTING testANewCollectionInOneInstance");
    CollectionAdminResponse response = CollectionAdminRequest.createCollection(oneInstanceCollection, "conf1", 2, 2)
        .setCreateNodeSet(jettys.get(0).getNodeName())
        .setMaxShardsPerNode(4)
        .process(cloudClient);
    assertEquals(0, response.getStatus());
    List<SolrClient> collectionClients = new ArrayList<>();
    for (String coreName : response.getCollectionCoresStatus().keySet()) {
      collectionClients.add(createNewSolrClient(coreName, jettys.get(0).getBaseUrl().toString()));
    }
   
    SolrClient client1 = collectionClients.get(0);
    SolrClient client2 = collectionClients.get(1);
    SolrClient client3 = collectionClients.get(2);
    SolrClient client4 = collectionClients.get(3);
 
    waitForRecoveriesToFinish(oneInstanceCollection, getCommonCloudSolrClient().getZkStateReader(), false);
    assertAllActive(oneInstanceCollection, getCommonCloudSolrClient().getZkStateReader());
    
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
    long allDocs = getCommonCloudSolrClient().query(query).getResults().getNumFound();
    
//    System.out.println("1:" + oneDocs);
//    System.out.println("2:" + twoDocs);
//    System.out.println("3:" + threeDocs);
//    System.out.println("4:" + fourDocs);
//    System.out.println("All Docs:" + allDocs);
    
    assertEquals(3, allDocs);
    IOUtils.close(collectionClients);

  }

  private void createCollection(String collection,
      List<SolrClient> collectionClients, String baseUrl, int num) {
    createSolrCore(collection, collectionClients, baseUrl, num, null);
  }
  
  private void createSolrCore(final String collection,
      List<SolrClient> collectionClients, final String baseUrl, final int num,
      final String shardId) {
    Callable call = () -> {
      try (HttpSolrClient client = getHttpSolrClient(baseUrl)) {
        // client.setConnectionTimeout(15000);
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
        createCmd.setDataDir(getDataDir(createTempDir(collection).toFile().getAbsolutePath()));
        if (shardId != null) {
          createCmd.setShardId(shardId);
        }
        client.request(createCmd);
      } catch (Exception e) {
        e.printStackTrace();
        //fail
      }
      return null;
    };
    
    pending.add(completionService.submit(call));
 
    
    collectionClients.add(createNewSolrClient(collection + num, baseUrl));
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
    getCommonCloudSolrClient().setDefaultCollection("collection2");
    getCommonCloudSolrClient().add(getDoc(id, "10000004"));
    getCommonCloudSolrClient().setDefaultCollection(null);
    
    indexDoc("collection3", getDoc(id, "20000000"));
    indexDoc("collection3", getDoc(id, "20000001")); 
    getCommonCloudSolrClient().setDefaultCollection("collection3");
    getCommonCloudSolrClient().add(getDoc(id, "10000005"));
    getCommonCloudSolrClient().setDefaultCollection(null);
    
    otherCollectionClients.get("collection2").get(0).commit();
    otherCollectionClients.get("collection3").get(0).commit();
    
    getCommonCloudSolrClient().setDefaultCollection("collection1");
    long collection1Docs = getCommonCloudSolrClient().query(new SolrQuery("*:*")).getResults()
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
    found = getCommonCloudSolrClient().query(query).getResults().getNumFound();
    assertEquals(collection1Docs + collection2Docs + collection3Docs, found);
    
    query.set("collection", "collection2,collection3");
    found = getCommonCloudSolrClient().query(query).getResults().getNumFound();
    assertEquals(collection2Docs + collection3Docs, found);
    
    query.set("collection", "collection3");
    found = getCommonCloudSolrClient().query(query).getResults().getNumFound();
    assertEquals(collection3Docs, found);
    
    query.remove("collection");
    found = getCommonCloudSolrClient().query(query).getResults().getNumFound();
    assertEquals(collection1Docs, found);
    
    assertEquals(collection3Docs, collection2Docs - 1);
  }
  
  protected void indexDoc(String collection, SolrInputDocument doc) throws IOException, SolrServerException {
    List<SolrClient> clients = otherCollectionClients.get(collection);
    int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) % clients.size();
    SolrClient client = clients.get(which);
    client.add(doc);
  }
  
  private void createNewCollection(final String collection) throws InterruptedException {
    try {
      assertEquals(0, CollectionAdminRequest
          .createCollection(collection, "conf1", 2, 1)
          .setCreateNodeSet("")
          .process(cloudClient).getStatus());
    } catch (Exception e) {
      e.printStackTrace();
      //fails
    }
    final List<SolrClient> collectionClients = new ArrayList<>();
    otherCollectionClients.put(collection, collectionClients);
    int unique = 0 ;
    for (final JettySolrRunner runner : jettys) {
      unique++;
      final int frozeUnique = unique;
      Callable call = () -> {

        try {
          assertTrue(CollectionAdminRequest
              .addReplicaToShard(collection, "shard"+ ((frozeUnique%2)+1))
              .setNode(runner.getNodeName())
              .process(cloudClient).isSuccess());
        } catch (Exception e) {
          e.printStackTrace();
          //fails
        }
        return null;
      };
     
      collectionClients.add(createNewSolrClient(collection, runner.getBaseUrl().toString()));
      pending.add(completionService.submit(call));
      while (pending != null && pending.size() > 0) {
        
        Future<Object> future = completionService.take();
        if (future == null) return;
        pending.remove(future);
      }
    }
  }
  
  protected SolrClient createNewSolrClient(String collection, String baseUrl) {
    try {
      // setup the server...
      HttpSolrClient client = getHttpSolrClient(baseUrl + "/" + collection);

      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected SolrClient createNewSolrClient(String collection, String baseUrl, int connectionTimeoutMillis, int socketTimeoutMillis) {
    try {
      // setup the server...
      HttpSolrClient client = getHttpSolrClient(baseUrl + "/" + collection, connectionTimeoutMillis, socketTimeoutMillis);

      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException, IOException {

    if (r.nextBoolean())
      return super.queryServer(params);

    if (r.nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp = getCommonCloudSolrClient().query(params);
    return rsp;
  }
  
  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    if (otherCollectionClients != null) {
      for (List<SolrClient> clientList : otherCollectionClients.values()) {
        IOUtils.close(clientList);
      }
    }
    otherCollectionClients = null;
    List<Runnable> tasks = executor.shutdownNow();
    assertTrue(tasks.isEmpty());
  }
}
