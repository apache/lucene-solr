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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

/**
 * Super basic testing, no shard restarting or anything.
 */
public class BasicSolrCloudTest extends FullSolrCloudTest {
  
  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    
  }
  
  public BasicSolrCloudTest() {
    super();
    shardCount = 4;
    sliceCount = 2;
  }
  
  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    waitForRecoveriesToFinish(VERBOSE);
    
    // add a doc, update it, and delete it
    
    long docId = 99999999L;
    indexr("id", docId, t1, "originalcontent");
    
    commit();
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("distrib", "true");
    params.add("q", t1 + ":originalcontent");
    QueryResponse results = clients.get(0).query(params);
    assertEquals(1, results.getResults().getNumFound());
    System.out.println("results:" + results);
    
    // update doc
    indexr("id", docId, t1, "updatedcontent");
    
    commit();
    
    assertDocCounts(VERBOSE);
    
    results = clients.get(0).query(params);
    System.out.println("results1:" + results.getResults());
    assertEquals(0, results.getResults().getNumFound());
    
    params.set("q", t1 + ":updatedcontent");
    
    results = clients.get(0).query(params);
    assertEquals(1, results.getResults().getNumFound());
    
    UpdateRequest uReq = new UpdateRequest();
    //uReq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    uReq.deleteById(Long.toString(docId)).process(clients.get(0));
    
    commit();
    
    System.out.println("results2:" + results.getResults());
    
    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());
    
    // add 2 docs in a request
    uReq = new UpdateRequest();
    //uReq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    SolrInputDocument doc1 = new SolrInputDocument();

    System.out.println("add doc1:" + doc1);
    addFields(doc1, "id", docId++);
    uReq.add(doc1);
    SolrInputDocument doc2 = new SolrInputDocument();
    System.out.println("add doc2:" + doc2);
    addFields(doc2, "id", docId++);
    uReq.add(doc2);
    
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    commit();
    
    checkShardConsistency();
    
    System.out.println("controldocs: " + query(controlClient).getResults().getNumFound());
    System.out.println("clouddocs: " + query(cloudClient).getResults().getNumFound());
    
    assertDocCounts(VERBOSE);
    
    results = query(cloudClient);
    assertEquals(2, results.getResults().getNumFound());
    
    // two deletes
    System.out.println("delete:" + Long.toString(docId-1));
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
    System.out.println("added doc:" + docId);
    uReq.add(doc1);
    doc2 = new SolrInputDocument();
    addFields(doc2, "id", docId++);
    System.out.println("added doc:" + docId);
    uReq.add(doc2);
 
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    uReq = new UpdateRequest();
    System.out.println("delete doc:" + (docId - 2));
    uReq.deleteById(Long.toString(docId - 2)).process(cloudClient);
    controlClient.deleteById(Long.toString(docId - 2));
    
    commit();
    
    assertDocCounts(VERBOSE);
    
    checkShardConsistency();
    
    results = query(controlClient);
    assertEquals(2, results.getResults().getNumFound());
    
    results = query(cloudClient);
    assertEquals(2, results.getResults().getNumFound());
  }

  private QueryResponse query(SolrServer server) throws SolrServerException {
    SolrQuery query = new SolrQuery("*:*");
    query.setParam("distrib", true);
    return server.query(query);
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
