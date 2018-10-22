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

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Ignore;

import java.util.Iterator;

@Ignore("Abstract calls should not executed as test")
public abstract class TestBaseStatsCache extends TestDefaultStatsCache {

  protected abstract String getStatsCacheClassName();

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("solr.statsCache", getStatsCacheClassName());
  }

  public void distribTearDown() throws Exception {
    super.distribTearDown();
    System.clearProperty("solr.statsCache");
  }
  
  // in this case, as the number of shards increases, per-shard scores should
  // remain identical
  @Override
  protected void checkResponse(QueryResponse controlRsp, QueryResponse shardRsp) {
    System.out.println("======================= Control Response =======================");
    System.out.println(controlRsp);
    System.out.println("");
    System.out.println("");
    System.out.println("======================= Shard Response =======================");
    System.out.println("");
    System.out.println(shardRsp);
    SolrDocumentList shardList = shardRsp.getResults();
    SolrDocumentList controlList = controlRsp.getResults();
    
    assertEquals(controlList.size(), shardList.size());
    
    assertEquals(controlList.getNumFound(), shardList.getNumFound());
    Iterator<SolrDocument> it = controlList.iterator();
    Iterator<SolrDocument> it2 = shardList.iterator();
    while (it.hasNext()) {
      SolrDocument controlDoc = it.next();
      SolrDocument shardDoc = it2.next();
      assertEquals(controlDoc.getFieldValue("score"), shardDoc.getFieldValue("score"));
    }
  }

}
