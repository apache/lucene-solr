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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrRequestInfo;

/**
 *  <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
 **/

public abstract class AnalyticsQuery extends ExtendedQueryBase implements PostFilter {

  public boolean getCache() {
    return false;
  }

  public int getCost() {
    return Math.max(super.getCost(), 100);
  }

  public boolean equals(Object o) {
    return this == o ;
  }

  public int hashCode() {
    return System.identityHashCode(this);
  }

  /**
  *  Use this constructor for single node analytics.
  * */
  public AnalyticsQuery() {

  }

  /**
   * Use this constructor for distributed analytics.
   * @param mergeStrategy defines the distributed merge strategy for this AnalyticsQuery
   **/

  public AnalyticsQuery(MergeStrategy mergeStrategy){
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    ResponseBuilder rb = info.getResponseBuilder();
    rb.addMergeStrategy(mergeStrategy);
  }

  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    ResponseBuilder rb = null;
    if(info != null) {
      rb = info.getResponseBuilder();
    }

    if(rb == null) {
      //This is the autowarming case.
      return new DelegatingCollector();
    } else {
      return getAnalyticsCollector(rb, searcher);
    }
  }

  public abstract DelegatingCollector getAnalyticsCollector(ResponseBuilder rb, IndexSearcher searcher);

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}