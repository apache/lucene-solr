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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;

/**
 * Super basic testing, no shard restarting or anything.
 */
public class BasicFullDistributedZkTest extends FullDistributedZkTest {
  
  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    
  }
  
  public BasicFullDistributedZkTest() {
    super();
    shardCount = 4;
    sliceCount = 2;
  }
  
  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    del("*:*");
    
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
    
    assertDocCounts();
    
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

    addFields(doc1, "id", docId++);
    uReq.add(doc1);
    SolrInputDocument doc2 = new SolrInputDocument();
    addFields(doc2, "id", docId++);
    uReq.add(doc2);
    
    uReq.process(cloudClient);
    uReq.process(controlClient);
    
    commit();
    
    results = cloudClient.query(new SolrQuery("*:*"));
    assertEquals(2, results.getResults().getNumFound());
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
