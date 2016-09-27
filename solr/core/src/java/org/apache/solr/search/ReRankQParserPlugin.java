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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryRescorer;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;

/*
*
*  Syntax: q=*:*&rq={!rerank reRankQuery=$rqq reRankDocs=300 reRankWeight=3}
*
*/

public class ReRankQParserPlugin extends QParserPlugin {

  public static final String NAME = "rerank";
  private static Query defaultQuery = new MatchAllDocsQuery();

  public static final String RERANK_QUERY = "reRankQuery";

  public static final String RERANK_DOCS = "reRankDocs";
  public static final int RERANK_DOCS_DEFAULT = 200;

  public static final String RERANK_WEIGHT = "reRankWeight";
  public static final double RERANK_WEIGHT_DEFAULT = 2.0d;

  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new ReRankQParser(query, localParams, params, req);
  }

  private class ReRankQParser extends QParser  {

    public ReRankQParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(query, localParams, params, req);
    }

    public Query parse() throws SyntaxError {
      String reRankQueryString = localParams.get(RERANK_QUERY);
      if (reRankQueryString == null || reRankQueryString.trim().length() == 0)  {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, RERANK_QUERY+" parameter is mandatory");
      }
      QParser reRankParser = QParser.getParser(reRankQueryString, req);
      Query reRankQuery = reRankParser.parse();

      int reRankDocs  = localParams.getInt(RERANK_DOCS, RERANK_DOCS_DEFAULT);
      reRankDocs = Math.max(1, reRankDocs); //

      double reRankWeight = localParams.getDouble(RERANK_WEIGHT, RERANK_WEIGHT_DEFAULT);

      return new ReRankQuery(reRankQuery, reRankDocs, reRankWeight);
    }
  }

  private final class ReRankQueryRescorer extends QueryRescorer {

    final double reRankWeight;

    public ReRankQueryRescorer(Query reRankQuery, double reRankWeight) {
      super(reRankQuery);
      this.reRankWeight = reRankWeight;
    }

    @Override
    protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
      float score = firstPassScore;
      if (secondPassMatches) {
        score += reRankWeight * secondPassScore;
      }
      return score;
    }
  }

  private final class ReRankQuery extends RankQuery {
    private Query mainQuery = defaultQuery;
    final private Query reRankQuery;
    final private int reRankDocs;
    final private double reRankWeight;
    final private Rescorer reRankQueryRescorer;
    private Map<BytesRef, Integer> boostedPriority;

    public int hashCode() {
      return 31 * classHash() + mainQuery.hashCode()+reRankQuery.hashCode()+(int)reRankWeight+reRankDocs;
    }

    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(ReRankQuery rrq) {
      return mainQuery.equals(rrq.mainQuery) &&
             reRankQuery.equals(rrq.reRankQuery) &&
             reRankWeight == rrq.reRankWeight &&
             reRankDocs == rrq.reRankDocs;
    }

    public ReRankQuery(Query reRankQuery, int reRankDocs, double reRankWeight) {
      this.reRankQuery = reRankQuery;
      this.reRankDocs = reRankDocs;
      this.reRankWeight = reRankWeight;
      this.reRankQueryRescorer = new ReRankQueryRescorer(reRankQuery, reRankWeight);
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

    public TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd, IndexSearcher searcher) throws IOException {

      if(this.boostedPriority == null) {
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if(info != null) {
          Map context = info.getReq().getContext();
          this.boostedPriority = (Map<BytesRef, Integer>)context.get(QueryElevationComponent.BOOSTED_PRIORITY);
        }
      }

      return new ReRankCollector(reRankDocs, len, reRankQueryRescorer, cmd, searcher, boostedPriority);
    }

    @Override
    public String toString(String s) {
      final StringBuilder sb = new StringBuilder(100); // default initialCapacity of 16 won't be enough
      sb.append("{!").append(NAME);
      sb.append(" mainQuery='").append(mainQuery.toString()).append("' ");
      sb.append(RERANK_QUERY).append("='").append(reRankQuery.toString()).append("' ");
      sb.append(RERANK_DOCS).append('=').append(reRankDocs).append(' ');
      sb.append(RERANK_WEIGHT).append('=').append(reRankWeight).append('}');
      return sb.toString();
    }

    public Query rewrite(IndexReader reader) throws IOException {
      Query q = mainQuery.rewrite(reader);
      if (q != mainQuery) {
        return new ReRankQuery(reRankQuery, reRankDocs, reRankWeight).wrap(q);
      }
      return super.rewrite(reader);
    }

    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException{
      final Weight mainWeight = mainQuery.createWeight(searcher, needsScores, boost);
      return new ReRankWeight(mainQuery, reRankQueryRescorer, searcher, mainWeight);
    }
  }
}
