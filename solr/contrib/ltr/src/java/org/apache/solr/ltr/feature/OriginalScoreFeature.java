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
package org.apache.solr.ltr.feature;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.solr.ltr.DocInfo;
import org.apache.solr.request.SolrQueryRequest;
/**
 * This feature returns the original score that the document had before performing
 * the reranking.
 * Example configuration:
 * <pre>{
  "name":  "originalScore",
  "class": "org.apache.solr.ltr.feature.OriginalScoreFeature",
  "params": { }
}</pre>
 **/
public class OriginalScoreFeature extends Feature {

  public OriginalScoreFeature(String name, Map<String,Object> params) {
    super(name, params);
  }

  @Override
  public LinkedHashMap<String,Object> paramsToMap() {
    return defaultParamsToMap();
  }

  @Override
  protected void validate() throws FeatureException {
  }

  @Override
  public OriginalScoreWeight createWeight(IndexSearcher searcher,
      boolean needsScores, SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) throws IOException {
    return new OriginalScoreWeight(searcher, request, originalQuery, efi);

  }

  public class OriginalScoreWeight extends FeatureWeight {

    final Weight w;

    public OriginalScoreWeight(IndexSearcher searcher,
        SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi) throws IOException {
      super(OriginalScoreFeature.this, searcher, request, originalQuery, efi);
      w = searcher.createWeight(searcher.rewrite(originalQuery), ScoreMode.COMPLETE, 1);
    };


    @Override
    public String toString() {
      return "OriginalScoreFeature [query:" + originalQuery.toString() + "]";
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      w.extractTerms(terms);
    }

    @Override
    public FeatureScorer scorer(LeafReaderContext context) throws IOException {

      final Scorer originalScorer = w.scorer(context);
      return new OriginalScoreScorer(this, originalScorer);
    }

    public class OriginalScoreScorer extends FilterFeatureScorer {

      public OriginalScoreScorer(FeatureWeight weight, Scorer originalScorer) {
        super(weight, originalScorer);
      }

      @Override
      public float score() throws IOException {
        // This is done to improve the speed of feature extraction. Since this
        // was already scored in step 1
        // we shouldn't need to calc original score again.
        final DocInfo docInfo = getDocInfo();
        return (docInfo != null && docInfo.hasOriginalDocScore() ? docInfo.getOriginalDocScore() : in.score());
      }

    }

  }

}
