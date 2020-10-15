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
package org.apache.solr.search;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test for QueryComponent's distributed querying
 *
 * @see org.apache.solr.handler.component.QueryComponent
 */
public class MergeStrategyTest extends BaseDistributedSearchTestCase {

  public MergeStrategyTest() {
    stress = 0;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCore("solrconfig-plugcollector.xml", "schema15.xml");
  }

  @Test
  @ShardsFixed(num = 3)
  @SuppressWarnings({"unchecked"})
  public void test() throws Exception {
    del("*:*");

    index_specific(0,"id","1", "sort_i", "5");
    index_specific(0,"id","2", "sort_i", "50");
    index_specific(1,"id","5", "sort_i", "4");
    index_specific(1,"id","6", "sort_i", "10");
    index_specific(0,"id","7", "sort_i", "1");
    index_specific(1,"id","8", "sort_i", "2");
    index_specific(2,"id","9", "sort_i", "1000");
    index_specific(2,"id","10", "sort_i", "1500");
    index_specific(2,"id","11", "sort_i", "1300");
    index_specific(1,"id","12", "sort_i", "15");
    index_specific(1,"id","13", "sort_i", "16");

    commit();

    handle.put("explain", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("score", SKIPVAL);
    handle.put("wt", SKIP);
    handle.put("distrib", SKIP);
    handle.put("shards.qt", SKIP);
    handle.put("shards", SKIP);
    handle.put("q", SKIP);
    handle.put("maxScore", SKIPVAL);
    handle.put("_version_", SKIP);

    //Test mergeStrategy that uses score
    query("rq", "{!rank}", "q", "*:*", "rows","12",  "sort",  "sort_i asc", "fl","*,score");

    //Test without mergeStrategy
    query("q", "*:*", "rows","12", "sort", "sort_i asc");

    //Test mergeStrategy1 that uses a sort field.
    query("rq", "{!rank mergeStrategy=1}", "q", "*:*", "rows","12", "sort", "sort_i asc");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("rows", "12");
    params.add("rq", "{!rank}");
    params.add("sort", "sort_i asc");
    params.add("fl","*,score");
    setDistributedParams(params);
    QueryResponse rsp = queryServer(params);
    assertOrder(rsp,"10","11","9","2","13","12","6","1","5","8","7");

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("rows", "12");
    params.add("sort", "sort_i asc");
    params.add("fl","*,score");
    setDistributedParams(params);
    rsp = queryServer(params);
    assertOrder(rsp,"7","8","5","1","6","12","13","2","9","11","10");

    MergeStrategy m1 = new MergeStrategy() {
      @Override
      public void  merge(ResponseBuilder rb, ShardRequest sreq) {
      }

      public boolean mergesIds() {
        return true;
      }

      public boolean handlesMergeFields() { return false;}
      public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) {}

      @Override
      public int getCost() {
        return 1;
      }
    };

    MergeStrategy m2 = new MergeStrategy() {
      @Override
      public void merge(ResponseBuilder rb, ShardRequest sreq) {
      }

      public boolean mergesIds() {
        return true;
      }

      public boolean handlesMergeFields() { return false;}
      public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) {}

      @Override
      public int getCost() {
        return 100;
      }
    };

    MergeStrategy m3 = new MergeStrategy() {
      @Override
      public void merge(ResponseBuilder rb, ShardRequest sreq) {
      }

      public boolean mergesIds() {
        return false;
      }

      public boolean handlesMergeFields() { return false;}
      public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) {}

      @Override
      public int getCost() {
        return 50;
      }
    };

    MergeStrategy[] merges = {m1,m2,m3};
    Arrays.sort(merges, MergeStrategy.MERGE_COMP);
    assert(merges[0].getCost() == 1);
    assert(merges[1].getCost() == 50);
    assert(merges[2].getCost() == 100);
  }

  private void assertOrder(QueryResponse rsp, String ... docs) throws Exception {
    SolrDocumentList list = rsp.getResults();
    for(int i=0; i<docs.length; i++) {
      SolrDocument doc = list.get(i);
      Object o = doc.getFieldValue("id");
      if(!docs[i].equals(o)) {
        throw new Exception("Order is not correct:"+o+"!="+docs[i]);
      }
    }
  }
}
