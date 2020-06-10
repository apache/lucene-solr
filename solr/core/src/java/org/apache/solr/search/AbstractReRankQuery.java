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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrRequestInfo;

public abstract class AbstractReRankQuery extends RankQuery {
  protected Query mainQuery;
  final protected int reRankDocs;
  final protected Rescorer reRankQueryRescorer;
  protected Set<BytesRef> boostedPriority;

  public AbstractReRankQuery(Query mainQuery, int reRankDocs, Rescorer reRankQueryRescorer) {
    this.mainQuery = mainQuery;
    this.reRankDocs = reRankDocs;
    this.reRankQueryRescorer = reRankQueryRescorer;
  }

  public RankQuery wrap(Query _mainQuery) {
    if(_mainQuery != null){
      this.mainQuery = _mainQuery;
    }
    return  this;
  }

  public MergeStrategy getMergeStrategy() {
    return null;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd, IndexSearcher searcher) throws IOException {
    if(this.boostedPriority == null) {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if(info != null) {
        Map context = info.getReq().getContext();
        this.boostedPriority = (Set<BytesRef>)context.get(QueryElevationComponent.BOOSTED);
      }
    }

    return new ReRankCollector(reRankDocs, len, reRankQueryRescorer, cmd, searcher, boostedPriority);
  }

  public Query rewrite(IndexReader reader) throws IOException {
    Query q = mainQuery.rewrite(reader);
    if (q != mainQuery) {
      return rewrite(q);
    }
    return super.rewrite(reader);
  }

  protected abstract Query rewrite(Query rewrittenMainQuery) throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException{
    final Weight mainWeight = mainQuery.createWeight(searcher, scoreMode, boost);
    return new ReRankWeight(mainQuery, reRankQueryRescorer, searcher, mainWeight);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
