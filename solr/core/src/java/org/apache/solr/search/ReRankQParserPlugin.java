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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import com.carrotsearch.hppc.IntFloatHashMap;
import com.carrotsearch.hppc.IntIntHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryRescorer;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
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

  private class ReRankCollector extends TopDocsCollector {

    final private TopDocsCollector  mainCollector;
    final private IndexSearcher searcher;
    final private int reRankDocs;
    final private int length;
    final private Map<BytesRef, Integer> boostedPriority;
    final private Rescorer reRankQueryRescorer;


    public ReRankCollector(int reRankDocs,
                           int length,
                           Rescorer reRankQueryRescorer,
                           QueryCommand cmd,
                           IndexSearcher searcher,
                           Map<BytesRef, Integer> boostedPriority) throws IOException {
      super(null);
      this.reRankDocs = reRankDocs;
      this.length = length;
      this.boostedPriority = boostedPriority;
      Sort sort = cmd.getSort();
      if(sort == null) {
        this.mainCollector = TopScoreDocCollector.create(Math.max(this.reRankDocs, length));
      } else {
        sort = sort.rewrite(searcher);
        this.mainCollector = TopFieldCollector.create(sort, Math.max(this.reRankDocs, length), false, true, true);
      }
      this.searcher = searcher;
      this.reRankQueryRescorer = reRankQueryRescorer;
    }

    public int getTotalHits() {
      return mainCollector.getTotalHits();
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return mainCollector.getLeafCollector(context);
    }

    @Override
    public boolean needsScores() {
      return true;
    }

    public TopDocs topDocs(int start, int howMany) {

      try {

        TopDocs mainDocs = mainCollector.topDocs(0,  Math.max(reRankDocs, length));

        if(mainDocs.totalHits == 0 || mainDocs.scoreDocs.length == 0) {
          return mainDocs;
        }

        if(boostedPriority != null) {
          SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
          Map requestContext = null;
          if(info != null) {
            requestContext = info.getReq().getContext();
          }

          IntIntHashMap boostedDocs = QueryElevationComponent.getBoostDocs((SolrIndexSearcher)searcher, boostedPriority, requestContext);

          ScoreDoc[] mainScoreDocs = mainDocs.scoreDocs;
          ScoreDoc[] reRankScoreDocs = new ScoreDoc[Math.min(mainScoreDocs.length, reRankDocs)];
          System.arraycopy(mainScoreDocs,0,reRankScoreDocs,0,reRankScoreDocs.length);

          mainDocs.scoreDocs = reRankScoreDocs;

          TopDocs rescoredDocs = reRankQueryRescorer
              .rescore(searcher, mainDocs, mainDocs.scoreDocs.length);

          Arrays.sort(rescoredDocs.scoreDocs, new BoostedComp(boostedDocs, mainDocs.scoreDocs, rescoredDocs.getMaxScore()));

          //Lower howMany if we've collected fewer documents.
          howMany = Math.min(howMany, mainScoreDocs.length);

          if(howMany == rescoredDocs.scoreDocs.length) {
            return rescoredDocs; // Just return the rescoredDocs
          } else if(howMany > rescoredDocs.scoreDocs.length) {
            //We need to return more then we've reRanked, so create the combined page.
            ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
            System.arraycopy(mainScoreDocs, 0, scoreDocs, 0, scoreDocs.length); //lay down the initial docs
            System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, rescoredDocs.scoreDocs.length);//overlay the re-ranked docs.
            rescoredDocs.scoreDocs = scoreDocs;
            return rescoredDocs;
          } else {
            //We've rescored more then we need to return.
            ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
            System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, howMany);
            rescoredDocs.scoreDocs = scoreDocs;
            return rescoredDocs;
          }

        } else {

          ScoreDoc[] mainScoreDocs   = mainDocs.scoreDocs;

          /*
          *  Create the array for the reRankScoreDocs.
          */
          ScoreDoc[] reRankScoreDocs = new ScoreDoc[Math.min(mainScoreDocs.length, reRankDocs)];

          /*
          *  Copy the initial results into the reRankScoreDocs array.
          */
          System.arraycopy(mainScoreDocs, 0, reRankScoreDocs, 0, reRankScoreDocs.length);

          mainDocs.scoreDocs = reRankScoreDocs;

          TopDocs rescoredDocs = reRankQueryRescorer
              .rescore(searcher, mainDocs, mainDocs.scoreDocs.length);

          //Lower howMany to return if we've collected fewer documents.
          howMany = Math.min(howMany, mainScoreDocs.length);

          if(howMany == rescoredDocs.scoreDocs.length) {
            return rescoredDocs; // Just return the rescoredDocs
          } else if(howMany > rescoredDocs.scoreDocs.length) {

            //We need to return more then we've reRanked, so create the combined page.
            ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
            //lay down the initial docs
            System.arraycopy(mainScoreDocs, 0, scoreDocs, 0, scoreDocs.length);
            //overlay the rescoreds docs
            System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, rescoredDocs.scoreDocs.length);
            rescoredDocs.scoreDocs = scoreDocs;
            return rescoredDocs;
          } else {
            //We've rescored more then we need to return.
            ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
            System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, howMany);
            rescoredDocs.scoreDocs = scoreDocs;
            return rescoredDocs;
          }
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }

  }

  public class BoostedComp implements Comparator {
    IntFloatHashMap boostedMap;

    public BoostedComp(IntIntHashMap boostedDocs, ScoreDoc[] scoreDocs, float maxScore) {
      this.boostedMap = new IntFloatHashMap(boostedDocs.size()*2);

      for(int i=0; i<scoreDocs.length; i++) {
        final int idx;
        if((idx = boostedDocs.indexOf(scoreDocs[i].doc)) >= 0) {
          boostedMap.put(scoreDocs[i].doc, maxScore+boostedDocs.indexGet(idx));
        } else {
          break;
        }
      }
    }

    public int compare(Object o1, Object o2) {
      ScoreDoc doc1 = (ScoreDoc) o1;
      ScoreDoc doc2 = (ScoreDoc) o2;
      float score1 = doc1.score;
      float score2 = doc2.score;
      int idx;
      if((idx = boostedMap.indexOf(doc1.doc)) >= 0) {
        score1 = boostedMap.indexGet(idx);
      }

      if((idx = boostedMap.indexOf(doc2.doc)) >= 0) {
        score2 = boostedMap.indexGet(idx);
      }

      return -Float.compare(score1, score2);
    }
  }
}
