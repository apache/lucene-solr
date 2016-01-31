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

import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.ShardResponse;

import org.junit.Ignore;
import java.io.IOException;

@Ignore
public class TestAnalyticsQParserPlugin extends QParserPlugin {


  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new TestAnalyticsQueryParser(query, localParams, params, req);
  }

  class TestAnalyticsQueryParser extends QParser {

    public TestAnalyticsQueryParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(query, localParams, params, req);
    }

    public Query parse() throws SyntaxError {
      return new TestAnalyticsQuery(new TestAnalyticsMergeStrategy());
    }
  }

  class TestAnalyticsQuery extends AnalyticsQuery {

    public TestAnalyticsQuery(MergeStrategy mergeStrategy) {
      super(mergeStrategy);
    }

    public DelegatingCollector getAnalyticsCollector(ResponseBuilder rb, IndexSearcher searcher) {
      return new TestAnalyticsCollector(rb);
    }
  }

  class TestAnalyticsCollector extends DelegatingCollector {
    ResponseBuilder rb;
    int count;

    public TestAnalyticsCollector(ResponseBuilder rb) {
      this.rb = rb;
    }

    public void collect(int doc) throws IOException {
      ++count;
      leafDelegate.collect(doc);
    }

    public void finish() throws IOException {
      NamedList analytics = new NamedList();
      rb.rsp.add("analytics", analytics);
      analytics.add("mycount", count);
      if(this.delegate instanceof DelegatingCollector) {
        ((DelegatingCollector)this.delegate).finish();
      }
    }
  }

  class TestAnalyticsMergeStrategy implements MergeStrategy {

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

    public void merge(ResponseBuilder rb, ShardRequest shardRequest) {
      int count = 0;
      NamedList merged = new NamedList();

      for(ShardResponse shardResponse : shardRequest.responses) {
        NamedList response = shardResponse.getSolrResponse().getResponse();
        NamedList analytics = (NamedList)response.get("analytics");
        Integer c = (Integer)analytics.get("mycount");
        count += c.intValue();
      }

      merged.add("mycount", count);
      rb.rsp.add("analytics", merged);
    }
  }
}
