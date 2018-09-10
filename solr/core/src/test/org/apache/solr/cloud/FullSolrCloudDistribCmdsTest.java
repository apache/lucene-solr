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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

/**
 * Super basic testing, no shard restarting or anything.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class FullSolrCloudDistribCmdsTest extends AbstractFullDistribZkTestBase {
  
  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema15.xml";      // we need a string id
  }
  
  public FullSolrCloudDistribCmdsTest() {
    super();
    sliceCount = 3;
  }

  @Test
  @ShardsFixed(num = 6)
  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    waitForRecoveriesToFinish(false);
    
    // add a doc, update it, and delete it
    
    QueryResponse results;
    UpdateRequest uReq;
    long docId = addUpdateDelete();
    
    // add 2 docs in a request
    SolrInputDocument doc1;
    SolrInputDocument doc2;
    docId = addTwoDocsInOneRequest(docId);
    
    // two deletes
    uReq = new UpdateRequest();
    uReq.deleteById(Long.toString(docId-1));
    uReq.deleteById(Long.toString(docId-2)).process(cloudClient);
    controlClient.deleteById(Long.toString(docId-1));
    controlClient.deleteById(Long.toString(docId-2));
    
    commit();
    
    results = query(cloudClient);
    assertEquals(0, results.getResults().getNumFound());
    
    results = query(controlClient);
    assertEquals(0, results.getResults().getNumFound());
    
    // add two docs together, a 3rd doc and a delete
    indexr("id", docId++, t1, "originalcontent");
    
    uReq = new UpdateRequest();
    doc1 = new SolrInputDocument();

    addFields(doc1, "id", docId++);
    uReq.add(doc1);
    doc2 = new SolrInputDocument();
    addFields(doc2, "id", docId++);
    uReq.add(doc2);
 
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    uReq = new UpdateRequest();
    uReq.deleteById(Long.toString(docId - 2)).process(cloudClient);
    controlClient.deleteById(Long.toString(docId - 2));
    
    commit();
    
    assertDocCounts(VERBOSE);
    
    checkShardConsistency();
    
    results = query(controlClient);
    assertEquals(2, results.getResults().getNumFound());
    
    results = query(cloudClient);
    assertEquals(2, results.getResults().getNumFound());
    
    docId = testIndexQueryDeleteHierarchical(docId);
    
    docId = testIndexingDocPerRequestWithHttpSolrClient(docId);
    
    testConcurrentIndexing(docId);
    
    // TODO: testOptimisticUpdate(results);
    
    testDeleteByQueryDistrib();

    // See SOLR-7384
//    testDeleteByIdImplicitRouter();
//
//    testDeleteByIdCompositeRouterWithRouterField();

    docId = testThatCantForwardToLeaderFails(docId);


    docId = testIndexingBatchPerRequestWithHttpSolrClient(docId);
  }

  private void testDeleteByIdImplicitRouter() throws Exception {
    SolrClient server = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)));
    CollectionAdminResponse response;
    Map<String, NamedList<Integer>> coresStatus;

    CollectionAdminRequest.Create createCollectionRequest
      = CollectionAdminRequest.createCollectionWithImplicitRouter("implicit_collection_without_routerfield",
                                                                  "conf1","shard1,shard2",2);
    response = createCollectionRequest.process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(4, coresStatus.size());
    for (int i = 0; i < 4; i++) {
      NamedList<Integer> status = coresStatus.get("implicit_collection_without_routerfield_shard" + (i / 2 + 1) + "_replica" + (i % 2 + 1));
      assertEquals(0, (int) status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }

    waitForRecoveriesToFinish("implicit_collection_without_routerfield", true);

    SolrClient shard1 = createNewSolrClient("implicit_collection_without_routerfield_shard1_replica1",
        getBaseUrl((HttpSolrClient) clients.get(0)));
    SolrClient shard2 = createNewSolrClient("implicit_collection_without_routerfield_shard2_replica1",
        getBaseUrl((HttpSolrClient) clients.get(0)));

    SolrInputDocument doc = new SolrInputDocument();
    int docCounts1, docCounts2;

    // Add three documents to shard1
    doc.clear();
    doc.addField("id", "1");
    doc.addField("title", "s1 one");
    shard1.add(doc);
    shard1.commit();

    doc.clear();
    doc.addField("id", "2");
    doc.addField("title", "s1 two");
    shard1.add(doc);
    shard1.commit();

    doc.clear();
    doc.addField("id", "3");
    doc.addField("title", "s1 three");
    shard1.add(doc);
    shard1.commit();

    docCounts1 = 3; // Three documents in shard1

    // Add two documents to shard2
    doc.clear();
    doc.addField("id", "4");
    doc.addField("title", "s2 four");
    shard2.add(doc);
    shard2.commit();

    doc.clear();
    doc.addField("id", "5");
    doc.addField("title", "s2 five");
    shard2.add(doc);
    shard2.commit();

    docCounts2 = 2; // Two documents in shard2

    // Verify the documents were added to correct shards
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.set("q", "*:*");
    QueryResponse respAll = shard1.query(query);
    assertEquals(docCounts1 + docCounts2, respAll.getResults().getNumFound());

    query.set("shards", "shard1");
    QueryResponse resp1 = shard1.query(query);
    assertEquals(docCounts1, resp1.getResults().getNumFound());

    query.set("shards", "shard2");
    QueryResponse resp2 = shard2.query(query);
    assertEquals(docCounts2, resp2.getResults().getNumFound());


    // Delete a document in shard2 with update to shard1, with _route_ param
    // Should delete.
    UpdateRequest deleteRequest = new UpdateRequest();
    deleteRequest.deleteById("4", "shard2");
    shard1.request(deleteRequest);
    shard1.commit();
    query.set("shards", "shard2");
    resp2 = shard2.query(query);
    assertEquals(--docCounts2, resp2.getResults().getNumFound());

    // Delete a document in shard2 with update to shard1, without _route_ param
    // Shouldn't delete, since deleteById requests are not broadcast to all shard leaders.
    deleteRequest = new UpdateRequest();
    deleteRequest.deleteById("5");
    shard1.request(deleteRequest);
    shard1.commit();
    query.set("shards", "shard2");
    resp2 = shard2.query(query);
    assertEquals(docCounts2, resp2.getResults().getNumFound());

    // Multiple deleteById commands in a single request
    deleteRequest.clear();
    deleteRequest.deleteById("2", "shard1");
    deleteRequest.deleteById("3", "shard1");
    deleteRequest.setCommitWithin(1);
    query.set("shards", "shard1");
    shard2.request(deleteRequest);
    resp1 = shard1.query(query);
    --docCounts1;
    --docCounts1;
    assertEquals(docCounts1, resp1.getResults().getNumFound());

    // Test commitWithin, update to shard2, document deleted in shard1
    deleteRequest.clear();
    deleteRequest.deleteById("1", "shard1");
    deleteRequest.setCommitWithin(1);
    shard2.request(deleteRequest);
    Thread.sleep(1000);
    query.set("shards", "shard1");
    resp1 = shard1.query(query);
    assertEquals(--docCounts1, resp1.getResults().getNumFound());
  }

  private void testDeleteByIdCompositeRouterWithRouterField() throws Exception {
    SolrClient server = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)));
    CollectionAdminResponse response;
    Map<String, NamedList<Integer>> coresStatus;

    response = CollectionAdminRequest.createCollection("compositeid_collection_with_routerfield","conf1",2,2)
            .setRouterName("compositeId")
            .setRouterField("routefield_s")
            .setShards("shard1,shard2")
            .process(server);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(4, coresStatus.size());
    for (int i = 0; i < 4; i++) {
      NamedList<Integer> status = coresStatus.get("compositeid_collection_with_routerfield_shard" + (i / 2 + 1) + "_replica" + (i % 2 + 1));
      assertEquals(0, (int) status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }

    waitForRecoveriesToFinish("compositeid_collection_with_routerfield", true);

    SolrClient shard1 = createNewSolrClient("compositeid_collection_with_routerfield_shard1_replica1",
        getBaseUrl((HttpSolrClient) clients.get(0)));
    SolrClient shard2 = createNewSolrClient("compositeid_collection_with_routerfield_shard2_replica1",
        getBaseUrl((HttpSolrClient) clients.get(0)));

    SolrInputDocument doc = new SolrInputDocument();
    int docCounts1 = 0, docCounts2 = 0;

    // Add three documents to shard1
    doc.clear();
    doc.addField("id", "1");
    doc.addField("title", "s1 one");
    doc.addField("routefield_s", "europe");
    shard1.add(doc);
    shard1.commit();

    doc.clear();
    doc.addField("id", "2");
    doc.addField("title", "s1 two");
    doc.addField("routefield_s", "europe");
    shard1.add(doc);
    shard1.commit();

    doc.clear();
    doc.addField("id", "3");
    doc.addField("title", "s1 three");
    doc.addField("routefield_s", "europe");
    shard1.add(doc);
    shard1.commit();

    docCounts1 = 3; // Three documents in shard1

    // Add two documents to shard2
    doc.clear();
    doc.addField("id", "4");
    doc.addField("title", "s2 four");
    doc.addField("routefield_s", "africa");
    shard2.add(doc);
    //shard2.commit();

    doc.clear();
    doc.addField("id", "5");
    doc.addField("title", "s2 five");
    doc.addField("routefield_s", "africa");
    shard2.add(doc);
    shard2.commit();

    docCounts2 = 2; // Two documents in shard2

    // Verify the documents were added to correct shards
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.set("q", "*:*");
    QueryResponse respAll = shard1.query(query);
    assertEquals(docCounts1 + docCounts2, respAll.getResults().getNumFound());

    query.set("shards", "shard1");
    QueryResponse resp1 = shard1.query(query);
    assertEquals(docCounts1, resp1.getResults().getNumFound());

    query.set("shards", "shard2");
    QueryResponse resp2 = shard2.query(query);
    assertEquals(docCounts2, resp2.getResults().getNumFound());

    // Delete a document in shard2 with update to shard1, with _route_ param
    // Should delete.
    UpdateRequest deleteRequest = new UpdateRequest();
    deleteRequest.deleteById("4", "africa");
    deleteRequest.setCommitWithin(1);
    shard1.request(deleteRequest);
    shard1.commit();

    query.set("shards", "shard2");
    resp2 = shard2.query(query);
    --docCounts2;
    assertEquals(docCounts2, resp2.getResults().getNumFound());

    // Multiple deleteById commands in a single request
    deleteRequest.clear();
    deleteRequest.deleteById("2", "europe");
    deleteRequest.deleteById("3", "europe");
    deleteRequest.setCommitWithin(1);
    query.set("shards", "shard1");
    shard1.request(deleteRequest);
    shard1.commit();
    Thread.sleep(1000);
    resp1 = shard1.query(query);
    --docCounts1;
    --docCounts1;
    assertEquals(docCounts1, resp1.getResults().getNumFound());

    // Test commitWithin, update to shard2, document deleted in shard1
    deleteRequest.clear();
    deleteRequest.deleteById("1", "europe");
    deleteRequest.setCommitWithin(1);
    shard2.request(deleteRequest);
    query.set("shards", "shard1");
    resp1 = shard1.query(query);
    --docCounts1;
    assertEquals(docCounts1, resp1.getResults().getNumFound());
  }

  private long testThatCantForwardToLeaderFails(long docId) throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    ZkNodeProps props = zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard1");
    
    chaosMonkey.stopShard("shard1");
    
    Thread.sleep(1000);
    
    // fake that the leader is still advertised
    String leaderPath = ZkStateReader.getShardLeadersPath(DEFAULT_COLLECTION, "shard1");
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000);
    int fails = 0;
    try {
      zkClient.makePath(leaderPath, Utils.toJSON(props),
          CreateMode.EPHEMERAL, true);
      for (int i = 0; i < 200; i++) {
        try {
          index_specific(shardToJetty.get("shard2").get(0).client.solrClient, id, docId++);
        } catch (SolrException e) {
          // expected
          fails++;
          break;
        } catch (SolrServerException e) {
          // expected
          fails++;
          break;
        }
      }
    } finally {
      zkClient.close();
    }

    assertTrue("A whole shard is down - some of these should fail", fails > 0);
    return docId;
  }

  private long addTwoDocsInOneRequest(long docId) throws
      Exception {
    QueryResponse results;
    UpdateRequest uReq;
    uReq = new UpdateRequest();
    docId = addDoc(docId, uReq);
    docId = addDoc(docId, uReq);
    
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    commit();
    
    checkShardConsistency();
    
    assertDocCounts(VERBOSE);
    
    results = query(cloudClient);
    assertEquals(2, results.getResults().getNumFound());
    return docId;
  }

  private long addUpdateDelete() throws Exception,
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
    
    assertDocCounts(VERBOSE);
    
    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());
    
    params.set("q", t1 + ":updatedcontent");
    
    results = clients.get(0).query(params);
    assertEquals(1, results.getResults().getNumFound());
    
    UpdateRequest uReq = new UpdateRequest();
    //uReq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    uReq.deleteById(Long.toString(docId)).process(clients.get(0));
    
    commit();
    
    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());
    return docId;
  }

  private void testDeleteByQueryDistrib() throws Exception {
    del("*:*");
    commit();
    assertEquals(0, query(cloudClient).getResults().getNumFound());
  }

  private long testIndexQueryDeleteHierarchical(long docId) throws Exception {
    //index
    int topDocsNum = atLeast(10);
    int childsNum = 5+random().nextInt(5);
    for (int i = 0; i < topDocsNum; ++i) {
      UpdateRequest uReq = new UpdateRequest();
      SolrInputDocument topDocument = new SolrInputDocument();
      topDocument.addField("id", docId++);
      topDocument.addField("type_s", "parent");
      topDocument.addField(i + "parent_f1_s", "v1");
      topDocument.addField(i + "parent_f2_s", "v2");
      
      
      for (int index = 0; index < childsNum; ++index) {
        docId = addChildren("child", topDocument, index, false, docId);
      }
      
      uReq.add(topDocument);
      uReq.process(cloudClient);
      uReq.process(controlClient);
    }
    
    commit();
    checkShardConsistency();
    assertDocCounts(VERBOSE);
    
    //query
    // parents
    SolrQuery query = new SolrQuery("type_s:parent");
    QueryResponse results = cloudClient.query(query);
    assertEquals(topDocsNum, results.getResults().getNumFound());
    
    //childs 
    query = new SolrQuery("type_s:child");
    results = cloudClient.query(query);
    assertEquals(topDocsNum * childsNum, results.getResults().getNumFound());
    
    //grandchilds
    query = new SolrQuery("type_s:grand");
    results = cloudClient.query(query);
    //each topDoc has t childs where each child has x = 0 + 2 + 4 + ..(t-1)*2 grands
    //x = 2 * (1 + 2 + 3 +.. (t-1)) => arithmetic summ of t-1 
    //x = 2 * ((t-1) * t / 2) = t * (t - 1)
    assertEquals(topDocsNum * childsNum * (childsNum - 1), results.getResults().getNumFound());
    
    //delete
    del("*:*");
    commit();
    
    return docId;
  }
  
  private long addChildren(String prefix, SolrInputDocument topDocument, int childIndex, boolean lastLevel, long docId) {
    SolrInputDocument childDocument = new SolrInputDocument();
    childDocument.addField("id", docId++);
    childDocument.addField("type_s", prefix);
    for (int index = 0; index < childIndex; ++index) {
      childDocument.addField(childIndex + prefix + index + "_s", childIndex + "value"+ index);
    }   
  
    if (!lastLevel) {
      for (int i = 0; i < childIndex * 2; ++i) {
        docId = addChildren("grand", childDocument, i, true, docId);
      }
    }
    topDocument.addChildDocument(childDocument);
    return docId;
  }
  
  
  private long testIndexingDocPerRequestWithHttpSolrClient(long docId) throws Exception {
    int docs = random().nextInt(TEST_NIGHTLY ? 4013 : 97) + 1;
    for (int i = 0; i < docs; i++) {
      UpdateRequest uReq;
      uReq = new UpdateRequest();
      docId = addDoc(docId, uReq);
      
      uReq.process(cloudClient);
      uReq.process(controlClient);
      
    }
    commit();
    
    checkShardConsistency();
    assertDocCounts(VERBOSE);
    
    return docId++;
  }
  
  private long testIndexingBatchPerRequestWithHttpSolrClient(long docId) throws Exception {
    
    // remove collection
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", "collection1");
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
  
    cloudClient.request(request);
    
    controlClient.deleteByQuery("*:*");
    controlClient.commit();
    
    // somtimes we use an oversharded collection
    createCollection(null, "collection2", 7, 3, 100000, cloudClient, null, "conf1");
    cloudClient.setDefaultCollection("collection2");
    waitForRecoveriesToFinish("collection2", false);
    
    class IndexThread extends Thread {
      Integer name;
      
      public IndexThread(Integer name) {
        this.name = name;
      }
      
      @Override
      public void run() {
        int rnds = random().nextInt(TEST_NIGHTLY ? 25 : 3) + 1;
        for (int i = 0; i < rnds; i++) {
          UpdateRequest uReq;
          uReq = new UpdateRequest();
          int cnt = random().nextInt(TEST_NIGHTLY ? 3313 : 350) + 1;
          for (int j = 0; j <cnt; j++) {
            addDoc("thread" + name + "_" + i + "_" + j, uReq);
          }
          
          try {
            uReq.process(cloudClient);
            uReq.process(controlClient);
          } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
          }


        }
      }
    };
    List<Thread> threads = new ArrayList<>();

    int nthreads = random().nextInt(TEST_NIGHTLY ? 4 : 2) + 1;
    for (int i = 0; i < nthreads; i++) {
      IndexThread thread = new IndexThread(i);
      threads.add(thread);
      thread.start();
    }
    
    for (Thread thread : threads) {
      thread.join();
    }
    
    commit();
    
    waitForRecoveriesToFinish("collection2", false);
    
    printLayout();
    
    SolrQuery query = new SolrQuery("*:*");
    long controlCount = controlClient.query(query).getResults()
        .getNumFound();
    long cloudCount = cloudClient.query(query).getResults().getNumFound();

    
    CloudInspectUtil.compareResults(controlClient, cloudClient);
    
    assertEquals("Control does not match cloud", controlCount, cloudCount);
    System.out.println("DOCS:" + controlCount);

    return docId;
  }

  private long addDoc(long docId, UpdateRequest uReq) {
    addDoc(Long.toString(docId++), uReq);
    return docId;
  }
  
  private long addDoc(String docId, UpdateRequest uReq) {
    SolrInputDocument doc1 = new SolrInputDocument();
    
    uReq.add(doc1);
    addFields(doc1, "id", docId, "text_t", "some text so that it not's negligent work to parse this doc, even though it's still a pretty short doc");
    return -1;
  }
  
  private long testConcurrentIndexing(long docId) throws Exception {
    QueryResponse results = query(cloudClient);
    long beforeCount = results.getResults().getNumFound();
    int cnt = TEST_NIGHTLY ? 2933 : 313;
    try (ConcurrentUpdateSolrClient concurrentClient = getConcurrentUpdateSolrClient(
        ((HttpSolrClient) clients.get(0)).getBaseURL(), 10, 2, 120000)) {
      for (int i = 0; i < cnt; i++) {
        index_specific(concurrentClient, id, docId++, "text_t", "some text so that it not's negligent work to parse this doc, even though it's still a pretty short doc");
      }
      concurrentClient.blockUntilFinished();
      
      commit();

      checkShardConsistency();
      assertDocCounts(VERBOSE);
    }
    results = query(cloudClient);
    assertEquals(beforeCount + cnt, results.getResults().getNumFound());
    return docId;
  }
  
  private void testOptimisticUpdate(QueryResponse results) throws Exception {
    SolrDocument doc = results.getResults().get(0);
    Long version = (Long) doc.getFieldValue(VERSION_FIELD);
    Integer theDoc = (Integer) doc.getFieldValue("id");
    UpdateRequest uReq = new UpdateRequest();
    SolrInputDocument doc1 = new SolrInputDocument();
    uReq.setParams(new ModifiableSolrParams());
    uReq.getParams().set(VERSION_FIELD, Long.toString(version));
    addFields(doc1, "id", theDoc, t1, "theupdatestuff");
    uReq.add(doc1);
    
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    commit();
    
    // updating the old version should fail...
    SolrInputDocument doc2 = new SolrInputDocument();
    uReq = new UpdateRequest();
    uReq.setParams(new ModifiableSolrParams());
    uReq.getParams().set(VERSION_FIELD, Long.toString(version));
    addFields(doc2, "id", theDoc, t1, "thenewupdatestuff");
    uReq.add(doc2);
    
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    commit();
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", t1 + ":thenewupdatestuff");
    QueryResponse res = clients.get(0).query(params);
    assertEquals(0, res.getResults().getNumFound());
    
    params = new ModifiableSolrParams();
    params.add("q", t1 + ":theupdatestuff");
    res = clients.get(0).query(params);
    assertEquals(1, res.getResults().getNumFound());
  }

  private QueryResponse query(SolrClient client) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery("*:*");
    return client.query(query);
  }
  
  protected SolrInputDocument addRandFields(SolrInputDocument sdoc) {
    return sdoc;
  }

}
