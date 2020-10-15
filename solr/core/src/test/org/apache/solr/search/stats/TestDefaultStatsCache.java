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
package org.apache.solr.search.stats;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestDefaultStatsCache extends BaseDistributedSearchTestCase {
  private int docId = 0;
  
  @Override
  public void distribSetUp() throws Exception {
    System.setProperty("metricsEnabled", "true");
    super.distribSetUp();
    System.setProperty("solr.statsCache", LocalStatsCache.class.getName());
  }
  
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    System.clearProperty("solr.statsCache");
  }

  @Test 
  public void test() throws Exception {
    del("*:*");
    commit();
    String aDocId=null;
    for (int i = 0; i < clients.size(); i++) {
      int shard = i + 1;
      for (int j = 0; j <= i; j++) {
        int currentId = docId++;
        index_specific(i, id,currentId , "a_t", "one two three",
            "shard_i", shard);
        aDocId = rarely() ? currentId+"":aDocId;
      }
    }
    commit();
    handle.clear();
    handle.put("QTime", SKIPVAL);   
    handle.put("timestamp", SKIPVAL);
    
    if (aDocId != null) {
      dfQuery("q", "id:"+aDocId, "debugQuery", "true", "fl", "*,score");
    }
    dfQuery("q", "a_t:one", "debugQuery", "true", "fl", "*,score");
    
    // add another document
    for (int i = 0; i < clients.size(); i++) {
      int shard = i + 1;
      for (int j = 0; j <= i; j++) {
        int currentId = docId++;
        index_specific(i, id, currentId, "a_t", "one two three four five",
            "shard_i", shard);
        aDocId = rarely() ? currentId+"":aDocId;
      }
    }
    commit();

    if (aDocId != null) {
      dfQuery("q", "{!cache=false}id:"+aDocId,"debugQuery", "true", "fl", "*,score");
    }
    dfQuery("q", "a_t:one a_t:four", "debugQuery", "true", "fl", "*,score");
  }
  
  // in this case, as the number of shards increases, per-shard scores begin to
  // diverge due to the different docFreq-s per shard.
  protected void checkResponse(QueryResponse controlRsp, QueryResponse shardRsp) {
    SolrDocumentList shardList = shardRsp.getResults();
    SolrDocumentList controlList = controlRsp.getResults();
    assertEquals(controlList.getNumFound(), shardList.getNumFound());
    Float shardScore = (Float) shardList.get(0).getFieldValue("score");
    Float controlScore = (Float) controlList.get(0).getFieldValue("score");
    if (clients.size() == 1) {
      // only one shard
      assertEquals(controlScore, shardScore);
    }
  }
  
  protected void dfQuery(Object... q) throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    
    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    
    final QueryResponse controlRsp = controlClient.query(params);
    
    // query a random server
    params.set("shards", shards);
    int which = r.nextInt(clients.size());
    SolrClient client = clients.get(which);
    QueryResponse rsp = client.query(params);
    checkResponse(controlRsp, rsp);
  }
}
