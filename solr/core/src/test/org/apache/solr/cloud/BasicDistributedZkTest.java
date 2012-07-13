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
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.SolrCmdDistributor.Request;
import org.apache.solr.util.DefaultSolrThreadFactory;

/**
 *
 */
@Slow
public class BasicDistributedZkTest extends AbstractDistributedZkTestCase {
  
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
  
  public BasicDistributedZkTest() {
    fixShardCount = true;
    shardCount = 3;
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
      query("q","*:*", "sort",f+" desc");
      query("q","*:*", "sort",f+" asc");
    }

    // these queries should be exactly ordered and scores should exactly match
    query("q","*:*", "sort",i1+" desc");
    query("q","*:*", "sort",i1+" asc");
    query("q","*:*", "sort",i1+" desc", "fl","*,score");
    query("q","*:*", "sort","n_tl1 asc", "fl","*,score"); 
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
    query("q","*:*", "rows",0, "facet","true", "facet.field",t1,"facet.limit",5, "facet.shard.limit",5);
    // check a complex key name
    query("q","*:*", "rows",0, "facet","true", "facet.field","{!key='a b/c \\' \\} foo'}"+t1,"facet.limit",5, "facet.shard.limit",5);
    handle.remove("facet_fields");


    // index the same document to two servers and make sure things
    // don't blow up.
    if (clients.size()>=2) {
      index(id,100, i1, 107 ,t1,"oh no, a duplicate!");
      for (int i=0; i<clients.size(); i++) {
        index_specific(i, id,100, i1, 107 ,t1,"oh no, a duplicate!");
      }
      commit();
      query("q","duplicate", "hl","true", "hl.fl", t1);
      query("q","fox duplicate horses", "hl","true", "hl.fl", t1);
      query("q","*:*", "rows",100);
    }

    // test debugging
    handle.put("explain", UNORDERED);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    query("q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);

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
    
    // Thread.sleep(10000000000L);
    if (DEBUG) {
      super.printLayout();
    }
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
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionAction.CREATE.toString());
      int numShards = _TestUtil.nextInt(random(), 0, shardCount) + 1;
      int numReplicas = _TestUtil.nextInt(random(), 0, 5) + 1;
      params.set("numShards", numShards);
      params.set("numReplicas", numReplicas);
      String collectionName = "awholynewcollection_" + i;
      int clientIndex = random().nextInt(2);
      List<Integer> list = new ArrayList<Integer>();
      list.add(numShards);
      list.add(numReplicas);
      collectionInfos.put(collectionName, list);
      params.set("name", collectionName);
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
   
      clients.get(clientIndex).request(request);
    }
    
    Set<Entry<String,List<Integer>>> collectionInfosEntrySet = collectionInfos.entrySet();
    for (Entry<String,List<Integer>> entry : collectionInfosEntrySet) {
      String collection = entry.getKey();
      List<Integer> list = entry.getValue();
      checkForCollection(collection, list.get(0));
      
      String url = getUrlFromZk(collection);

      HttpSolrServer collectionClient = new HttpSolrServer(url);
      
      // poll for a second - it can take a moment before we are ready to serve
      waitForNon404(collectionClient);
    }
    
    List<String> collectionNameList = new ArrayList<String>();
    collectionNameList.addAll(collectionInfos.keySet());
    String collectionName = collectionNameList.get(random().nextInt(collectionNameList.size()));
    
    String url = getUrlFromZk(collectionName);

    HttpSolrServer collectionClient = new HttpSolrServer(url);
    
    
    // lets try and use the solrj client to index and retrieve a couple documents
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
    clients.get(0).request(request);

    // reloads make take a short while
    boolean allTimesAreCorrect = waitForReloads(collectionName, urlToTimeBefore);
    assertTrue("some core start times did not change on reload", allTimesAreCorrect);
    
    // remove a collection
    params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    request = new QueryRequest(params);
    request.setPath("/admin/collections");
 
    clients.get(0).request(request);
    
    // ensure its out of the state
    checkForMissingCollection(collectionName);

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
    Map<String,Map<String,Slice>> collections = solrj.getZkStateReader()
        .getCloudState().getCollectionStates();
    if (collections.containsKey(collectionName)) {
      Map<String,Slice> slices = collections.get(collectionName);

      Iterator<Entry<String,Slice>> it = slices.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String,Slice> sliceEntry = it.next();
        Map<String,ZkNodeProps> sliceShards = sliceEntry.getValue().getShards();
        Iterator<Entry<String,ZkNodeProps>> shardIt = sliceShards.entrySet()
            .iterator();
        while (shardIt.hasNext()) {
          Entry<String,ZkNodeProps> shardEntry = shardIt.next();
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
    CloudState cloudState = solrj.getZkStateReader().getCloudState();
    Map<String,Slice> slices = cloudState.getCollectionStates().get(collection);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection:" + collection);
    }
    
    for (Map.Entry<String,Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      Map<String,ZkNodeProps> shards = slice.getShards();
      Set<Map.Entry<String,ZkNodeProps>> shardEntries = shards.entrySet();
      for (Map.Entry<String,ZkNodeProps> shardEntry : shardEntries) {
        final ZkNodeProps node = shardEntry.getValue();
        if (cloudState.liveNodesContain(node.get(ZkStateReader.NODE_NAME_PROP))) {
          return new ZkCoreNodeProps(node).getCoreUrl();
        }
      }
    }
    
    throw new RuntimeException("Could not find a live node for collection:" + collection);
  }

  private void waitForNon404(HttpSolrServer collectionClient)
      throws Exception {
    
    long timeoutAt = System.currentTimeMillis() + 30000;
    
    while (System.currentTimeMillis() < timeoutAt) {
      boolean missing = false;
      try {
        collectionClient.query(new SolrQuery("*:*"));
      } catch (SolrException e) {
        // How do I get the response code!?
        if (!e.getMessage().contains("(404)")) {
          throw e;
        }
        missing = true;
      }
      if (!missing) {
        return;
      }
      Thread.sleep(50);
    }
    printLayout();
    fail("Could not find the new collection - 404 : " + collectionClient.getBaseURL());
  }

  private void checkForCollection(String collectionName, int expectedSlices)
      throws Exception {
    // check for an expectedSlices new collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 60000;
    boolean found = false;
    boolean sliceMatch = false;
    while (System.currentTimeMillis() < timeoutAt) {
      solrj.getZkStateReader().updateCloudState(true);
      CloudState cloudState = solrj.getZkStateReader().getCloudState();
      Map<String,Map<String,Slice>> collections = cloudState
          .getCollectionStates();
      if (collections.containsKey(collectionName)) {
        Map<String,Slice> slices = collections.get(collectionName);
        // did we find expectedSlices slices/shards?
        if (slices.size() == expectedSlices) {
          sliceMatch = true;
          found = true;
          // also make sure each are active
          Iterator<Entry<String,Slice>> it = slices.entrySet().iterator();
          while (it.hasNext()) {
            Entry<String,Slice> sliceEntry = it.next();
            Map<String,ZkNodeProps> sliceShards = sliceEntry.getValue()
                .getShards();
            Iterator<Entry<String,ZkNodeProps>> shardIt = sliceShards
                .entrySet().iterator();
            while (shardIt.hasNext()) {
              Entry<String,ZkNodeProps> shardEntry = shardIt.next();
              if (!shardEntry.getValue().get(ZkStateReader.STATE_PROP)
                  .equals(ZkStateReader.ACTIVE)) {
                found = false;
                break;
              }
            }
          }
          if (found) break;
        }
      }
      Thread.sleep(100);
    }
    if (!found) {
      printLayout();
      if (!sliceMatch) {
        fail("Could not find new " + expectedSlices + " slice collection called " + collectionName);
      } else {
        fail("Found expected # of slices, but some nodes are not active for collection called " + collectionName);
      }
    }
  }
  
  private void checkForMissingCollection(String collectionName)
      throws Exception {
    // check for a  collection - we poll the state
    long timeoutAt = System.currentTimeMillis() + 15000;
    boolean found = true;
    while (System.currentTimeMillis() < timeoutAt) {
      solrj.getZkStateReader().updateCloudState(true);
      CloudState cloudState = solrj.getZkStateReader().getCloudState();
      Map<String,Map<String,Slice>> collections = cloudState
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
    List<SolrServer> collectionClients = new ArrayList<SolrServer>();
    SolrServer client = clients.get(0);
    otherCollectionClients.put(oneInstanceCollection2, collectionClients);
    String baseUrl = ((HttpSolrServer) client).getBaseURL();
    createCollection(oneInstanceCollection2, collectionClients, baseUrl, 1, "slice1");
    createCollection(oneInstanceCollection2, collectionClients, baseUrl, 2, "slice2");
    createCollection(oneInstanceCollection2, collectionClients, baseUrl, 3, "slice2");
    createCollection(oneInstanceCollection2, collectionClients, baseUrl, 4, "slice1");
    
   while (pending != null && pending.size() > 0) {
      
      Future<Request> future = completionService.take();
      pending.remove(future);
    }
    
    SolrServer client1 = createNewSolrServer(oneInstanceCollection2 + "1", baseUrl);
    SolrServer client2 = createNewSolrServer(oneInstanceCollection2 + "2", baseUrl);
    SolrServer client3 = createNewSolrServer(oneInstanceCollection2 + "3", baseUrl);
    SolrServer client4 = createNewSolrServer(oneInstanceCollection2 + "4", baseUrl);
    

    // no one should be recovering
    waitForRecoveriesToFinish(oneInstanceCollection2, solrj.getZkStateReader(), false, true);
    
    assertAllActive(oneInstanceCollection2, solrj.getZkStateReader());
    
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
    long allDocs = solrj.query(query).getResults().getNumFound();
    
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
    ZkStateReader zkStateReader = solrj.getZkStateReader();
    zkStateReader.updateCloudState(true);
    Map<String,Slice> slices = zkStateReader.getCloudState().getSlices(oneInstanceCollection2);
    assertNotNull(slices);
    String roles = slices.get("slice1").getShards().values().iterator().next().get(ZkStateReader.ROLES_PROP);
    assertEquals("none", roles);
  }

  private void testSearchByCollectionName() throws SolrServerException {
    SolrServer client = clients.get(0);
    String baseUrl = ((HttpSolrServer) client).getBaseURL();
    
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
    String baseUrl = ((HttpSolrServer) client).getBaseURL();
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
    
    waitForRecoveriesToFinish(oneInstanceCollection, solrj.getZkStateReader(), false);
    assertAllActive(oneInstanceCollection, solrj.getZkStateReader());
    
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
    long allDocs = solrj.query(query).getResults().getNumFound();
    
//    System.out.println("1:" + oneDocs);
//    System.out.println("2:" + twoDocs);
//    System.out.println("3:" + threeDocs);
//    System.out.println("4:" + fourDocs);
//    System.out.println("All Docs:" + allDocs);
    
    assertEquals(3, allDocs);
  }

  private void createCollection(String collection,
      List<SolrServer> collectionClients, String baseUrl, int num) {
    createCollection(collection, collectionClients, baseUrl, num, null);
  }
  
  private void createCollection(final String collection,
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
    
    
    indexDoc("collection3", getDoc(id, "20000000"));
    indexDoc("collection3", getDoc(id, "20000001")); 
    
    otherCollectionClients.get("collection2").get(0).commit();
    otherCollectionClients.get("collection3").get(0).commit();
    
    long collection1Docs = solrj.query(new SolrQuery("*:*")).getResults()
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
    found = solrj.query(query).getResults().getNumFound();
    assertEquals(collection1Docs + collection2Docs + collection3Docs, found);
    
    query.set("collection", "collection2,collection3");
    found = solrj.query(query).getResults().getNumFound();
    assertEquals(collection2Docs + collection3Docs, found);
    
    query.set("collection", "collection3");
    found = solrj.query(query).getResults().getNumFound();
    assertEquals(collection3Docs, found);
    
    query.remove("collection");
    found = solrj.query(query).getResults().getNumFound();
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
      final int frozeUnique = unique;
      Callable call = new Callable() {
        public Object call() {
          HttpSolrServer server;
          try {
            server = new HttpSolrServer(
                ((HttpSolrServer) client).getBaseURL());
            
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
     
      collectionClients.add(createNewSolrServer(collection,
          ((HttpSolrServer) client).getBaseURL()));
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

  volatile CloudSolrServer solrj;

  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws SolrServerException {

    if (r.nextBoolean())
      return super.queryServer(params);

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
    if (solrj != null) {
      solrj.shutdown();
    }
    System.clearProperty("zkHost");
  }
}
