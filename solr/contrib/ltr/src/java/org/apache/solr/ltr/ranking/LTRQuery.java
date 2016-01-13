package org.apache.solr.ltr.ranking;

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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * The LTRQuery and LTRWeight wrap the main query to fetch matching docs. It
 * then provides its own TopDocsCollector, which goes through the top X docs and
 * reranks them using the provided reRankModel.
 */
public class LTRQuery extends RankQuery {
  private Query mainQuery = new MatchAllDocsQuery();
  private ModelQuery reRankModel;
  private int reRankDocs;
  private Map<BytesRef,Integer> boostedPriority;

  public LTRQuery(ModelQuery reRankModel, int reRankDocs) {
    this.reRankModel = reRankModel;
    this.reRankDocs = reRankDocs;
  }

  @Override
  public int hashCode() {
    return (mainQuery.hashCode() + reRankModel.hashCode() + reRankDocs);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    LTRQuery rrq = (LTRQuery) o;
    return (mainQuery.equals(rrq.mainQuery)
        && reRankModel.equals(rrq.reRankModel) && reRankDocs == rrq.reRankDocs);
  }

  @Override
  public RankQuery wrap(Query _mainQuery) {
    if (_mainQuery != null) {
      this.mainQuery = _mainQuery;
    }

    reRankModel.setOriginalQuery(mainQuery);

    return this;
  }

  @Override
  public MergeStrategy getMergeStrategy() {
    return null;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd,
      IndexSearcher searcher) throws IOException {

    if (this.boostedPriority == null) {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if (info != null) {
        Map context = info.getReq().getContext();
        this.boostedPriority = (Map<BytesRef,Integer>) context
            .get(QueryElevationComponent.BOOSTED_PRIORITY);
        // https://github.com/apache/lucene-solr/blob/5775be6e6242c0f7ec108b10ebbf9da3a7d07a4b/lucene/queries/src/java/org/apache/lucene/queries/function/valuesource/TFValueSource.java#L56
        // function query needs the searcher in the context
        context.put("searcher", searcher);
      }
    }

    return new LTRCollector(reRankDocs, reRankModel, cmd, searcher,
        boostedPriority);
    // return new LTRCollector(reRankDocs, reRankModel, cmd, searcher,
    // boostedPriority);
  }

  @Override
  public String toString(String field) {
    return "{!ltr mainQuery='" + mainQuery.toString() + "' reRankModel='"
        + reRankModel.toString() + "' reRankDocs=" + reRankDocs + "}";
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
    Weight mainWeight = mainQuery.createWeight(searcher, needsScores);
    return new LTRWeight(searcher, mainWeight, reRankModel);
  }

  /**
   * This is the weight for the main solr query in the LTRQuery. The only thing
   * this really does is have an explain using the reRankQuery.
   */
  public class LTRWeight extends Weight {
    private ModelQuery reRankModel;
    private Weight mainWeight;
    private IndexSearcher searcher;

    public LTRWeight(IndexSearcher searcher, Weight mainWeight,
        ModelQuery reRankModel) throws IOException {
      super(LTRQuery.this);
      this.reRankModel = reRankModel;
      this.mainWeight = mainWeight;
      this.searcher = searcher;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc)
        throws IOException {
      Explanation mainExplain = mainWeight.explain(context, doc);
      return new LTRRescorer(reRankModel).explain(searcher, mainExplain,
          context.docBase + doc);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      mainWeight.extractTerms(terms);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return mainWeight.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      mainWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return mainWeight.scorer(context);
    }
  }
}
