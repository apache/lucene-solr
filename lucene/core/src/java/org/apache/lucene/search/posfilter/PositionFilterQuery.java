package org.apache.lucene.search.posfilter;

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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class PositionFilterQuery extends Query {

  protected final Query innerQuery;
  protected final ScorerFilterFactory scorerFilterFactory;

  public PositionFilterQuery(Query innerQuery, ScorerFilterFactory scorerFilterFactory) {
    this.innerQuery = innerQuery;
    this.scorerFilterFactory = scorerFilterFactory;
  }

  protected static BooleanQuery buildBooleanQuery(Query... queries) {
    BooleanQuery bq = new BooleanQuery();
    for (Query q : queries) {
      bq.add(q, BooleanClause.Occur.MUST);
    }
    return bq;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    innerQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten =  innerQuery.rewrite(reader);
    if (rewritten != innerQuery) {
      return new PositionFilterQuery(rewritten, scorerFilterFactory);
    }
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new ScorerFilterWeight(innerQuery.createWeight(searcher), searcher);
  }

  @Override
  public String toString(String field) {
    return scorerFilterFactory.getName() + "[" + innerQuery.toString() + "]";
  }

  public class ScorerFilterWeight extends Weight {

    protected final Weight innerWeight;
    protected final Similarity similarity;
    protected final Similarity.SimWeight stats;

    public ScorerFilterWeight(Weight innerWeight, IndexSearcher searcher) throws IOException {
      this.innerWeight = innerWeight;
      this.similarity = searcher.getSimilarity();
      this.stats = getSimWeight(innerWeight.getQuery(), searcher);
    }

    private Similarity.SimWeight getSimWeight(Query query, IndexSearcher searcher)  throws IOException {
      TreeSet<Term> terms = new TreeSet<Term>();
      query.extractTerms(terms);
      if (terms.size() == 0)
        return null;
      int i = 0;
      TermStatistics[] termStats = new TermStatistics[terms.size()];
      for (Term term : terms) {
        TermContext state = TermContext.build(searcher.getTopReaderContext(), term);
        termStats[i] = searcher.termStatistics(term, state);
        i++;
      }
      final String field = terms.first().field(); // nocommit - should we be checking all filtered terms
      // are on the same field?
      return similarity.computeWeight(query.getBoost(), searcher.collectionStatistics(field), termStats);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, DocsEnum.FLAG_POSITIONS, context.reader().getLiveDocs());
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          Similarity.SimScorer docScorer = similarity.simScorer(stats, context);
          ComplexExplanation result = new ComplexExplanation();
          result.setDescription("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:");
          Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "phraseFreq=" + freq));
          result.addDetail(scoreExplanation);
          result.setValue(scoreExplanation.getValue());
          result.setMatch(true);
          return result;
        }
      }
      return new ComplexExplanation(false, 0.0f,
          "No matching term within position filter");
    }

    @Override
    public Query getQuery() {
      return PositionFilterQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return stats == null ? 1.0f : stats.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      if (stats != null)
        stats.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context, int flags, Bits acceptDocs) throws IOException {
      Scorer filteredScorer = innerWeight.scorer(context, flags | DocsEnum.FLAG_POSITIONS, acceptDocs);
      return filteredScorer == null ? null
                : scorerFilterFactory.scorer(filteredScorer, similarity.simScorer(stats, context));
    }
  }

  public static interface ScorerFilterFactory {

    public Scorer scorer(Scorer filteredScorer, Similarity.SimScorer simScorer);

    public String getName();

  }
}
