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

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * Super basic testing, no shard restarting or anything.
 */
@Ignore("distrib delete not working yet")
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
    // GRRRRR - this is needed because it takes a while for all the shards to learn about the cluster state
    Thread.sleep(5000);
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    del("*:*");
    
    // add a doc, update it, and delete it
    
    String docId = "99999999";
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
    uReq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    uReq.deleteById(docId).process(clients.get(0));
    
    commit();
    
    System.out.println("results2:" + results.getResults());
    
    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
