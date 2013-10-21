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

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.zookeeper.CreateMode;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * Super basic testing, no shard restarting or anything.
 */
@Slow
public class FullSolrCloudDistribCmdsTest extends AbstractFullDistribZkTestBase {
  
  
  @BeforeClass
  public static void beforeSuperClass() {
  }
  
  public FullSolrCloudDistribCmdsTest() {
    super();
    shardCount = 4;
    sliceCount = 2;
  }
  
  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
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
    
    testIndexingWithSuss();
    
    // TODO: testOptimisticUpdate(results);
    
    testDeleteByQueryDistrib();
    
    testThatCantForwardToLeaderFails();
  }

  private void testThatCantForwardToLeaderFails() throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    ZkNodeProps props = zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard1");
    
    chaosMonkey.stopShard("shard1");

    // fake that the leader is still advertised
    String leaderPath = ZkStateReader.getShardLeadersPath(DEFAULT_COLLECTION, "shard1");
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000);
    int fails = 0;
    try {
      zkClient.makePath(leaderPath, ZkStateReader.toJSON(props),
          CreateMode.EPHEMERAL, true);
      for (int i = 200; i < 210; i++) {
        try {
          index_specific(cloudClient, id, i);
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
  }

  private long addTwoDocsInOneRequest(long docId) throws
      Exception {
    QueryResponse results;
    UpdateRequest uReq;
    uReq = new UpdateRequest();
    //uReq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    SolrInputDocument doc1 = new SolrInputDocument();

    addFields(doc1, "id", docId++);
    uReq.add(doc1);
    SolrInputDocument doc2 = new SolrInputDocument();
    addFields(doc2, "id", docId++);
    uReq.add(doc2);
    
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
  
  private void testIndexingWithSuss() throws Exception {
    ConcurrentUpdateSolrServer suss = new ConcurrentUpdateSolrServer(
        ((HttpSolrServer) clients.get(0)).getBaseURL(), 3, 1);
    try {
      suss.setConnectionTimeout(15000);
      suss.setSoTimeout(30000);
      for (int i = 100; i < 150; i++) {
        index_specific(suss, id, i);
      }
      suss.blockUntilFinished();
      
      commit();
      
      checkShardConsistency();
    } finally {
      suss.shutdown();
    }
  }
  
  private void testOptimisticUpdate(QueryResponse results) throws Exception {
    SolrDocument doc = results.getResults().get(0);
    Long version = (Long) doc.getFieldValue(VersionInfo.VERSION_FIELD);
    Integer theDoc = (Integer) doc.getFieldValue("id");
    UpdateRequest uReq = new UpdateRequest();
    SolrInputDocument doc1 = new SolrInputDocument();
    uReq.setParams(new ModifiableSolrParams());
    uReq.getParams().set(DistributedUpdateProcessor.VERSION_FIELD, Long.toString(version));
    addFields(doc1, "id", theDoc, t1, "theupdatestuff");
    uReq.add(doc1);
    
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    commit();
    
    // updating the old version should fail...
    SolrInputDocument doc2 = new SolrInputDocument();
    uReq = new UpdateRequest();
    uReq.setParams(new ModifiableSolrParams());
    uReq.getParams().set(DistributedUpdateProcessor.VERSION_FIELD, Long.toString(version));
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

  private QueryResponse query(SolrServer server) throws SolrServerException {
    SolrQuery query = new SolrQuery("*:*");
    return server.query(query);
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
