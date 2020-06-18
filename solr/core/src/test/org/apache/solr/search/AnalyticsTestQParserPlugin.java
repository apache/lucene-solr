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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.client.solrj.request.QueryRequest;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.IterativeMergeStrategy;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.ShardResponse;

import java.util.List;
import java.util.concurrent.Future;
import java.io.IOException;

public class AnalyticsTestQParserPlugin extends QParserPlugin {


  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new TestAnalyticsQueryParser(query, localParams, params, req);
  }

  static class TestAnalyticsQueryParser extends QParser {

    public TestAnalyticsQueryParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(query, localParams, params, req);
    }

    public Query parse() throws SyntaxError {
      int base = localParams.getInt("base", 0);
      boolean iterate = localParams.getBool("iterate", false);
      if(iterate)
        return new TestAnalyticsQuery(base, new TestIterative());
      else
        return new TestAnalyticsQuery(base, new TestAnalyticsMergeStrategy());
    }
  }

  static class TestAnalyticsQuery extends AnalyticsQuery {

    private int base;

    public TestAnalyticsQuery(int base, MergeStrategy mergeStrategy) {
      super(mergeStrategy);
      this.base = base;
    }

    public DelegatingCollector getAnalyticsCollector(ResponseBuilder rb, IndexSearcher searcher) {
      return new TestAnalyticsCollector(base, rb);
    }
  }

  static class TestAnalyticsCollector extends DelegatingCollector {
    ResponseBuilder rb;
    int count;
    int base;

    public TestAnalyticsCollector(int base, ResponseBuilder rb) {
      this.base = base;
      this.rb = rb;
    }

    public void collect(int doc) throws IOException {
      ++count;
      leafDelegate.collect(doc);
    }

    @SuppressWarnings({"unchecked"})
    public void finish() throws IOException {
      @SuppressWarnings({"rawtypes"})
      NamedList analytics = new NamedList();
      rb.rsp.add("analytics", analytics);
      analytics.add("mycount", count+base);
      if(this.delegate instanceof DelegatingCollector) {
        ((DelegatingCollector)this.delegate).finish();
      }
    }
  }

  static class TestAnalyticsMergeStrategy implements MergeStrategy {

    public boolean mergesIds() {
      return false;
    }

    public boolean handlesMergeFields() {
      return false;
    }

    public int getCost() {
      return 100;
    }

    public void  handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) {
    }

    @SuppressWarnings({"unchecked"})
    public void merge(ResponseBuilder rb, ShardRequest shardRequest) {
      int count = 0;
      @SuppressWarnings({"rawtypes"})
      NamedList merged = new NamedList();

      for(ShardResponse shardResponse : shardRequest.responses) {
        @SuppressWarnings({"rawtypes"})
        NamedList response = shardResponse.getSolrResponse().getResponse();
        @SuppressWarnings({"rawtypes"})
        NamedList analytics = (NamedList)response.get("analytics");
        Integer c = (Integer)analytics.get("mycount");
        count += c.intValue();
      }

      merged.add("mycount", count);
      rb.rsp.add("analytics", merged);
    }
  }

  static class TestIterative extends IterativeMergeStrategy  {

    @SuppressWarnings({"unchecked"})
    public void process(ResponseBuilder rb, ShardRequest sreq) throws Exception {
      int count = 0;
      for(ShardResponse shardResponse : sreq.responses) {
        @SuppressWarnings({"rawtypes"})
        NamedList response = shardResponse.getSolrResponse().getResponse();
        @SuppressWarnings({"rawtypes"})
        NamedList analytics = (NamedList)response.get("analytics");
        Integer c = (Integer)analytics.get("mycount");
        count += c.intValue();
      }

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("distrib", "false");
      params.add("fq","{!count base="+count+"}");
      params.add("q","*:*");


      /*
      *  Call back to all the shards in the response and process the result.
       */

      QueryRequest request = new QueryRequest(params);
      List<Future<CallBack>> futures = callBack(sreq.responses, request);

      int nextCount = 0;

      for(Future<CallBack> future : futures) {
        QueryResponse response = future.get().getResponse();
        @SuppressWarnings({"rawtypes"})
        NamedList analytics = (NamedList)response.getResponse().get("analytics");
        Integer c = (Integer)analytics.get("mycount");
        nextCount += c.intValue();
      }

      @SuppressWarnings({"rawtypes"})
      NamedList merged = new NamedList();
      merged.add("mycount", nextCount);
      rb.rsp.add("analytics", merged);
    }
  }
}
