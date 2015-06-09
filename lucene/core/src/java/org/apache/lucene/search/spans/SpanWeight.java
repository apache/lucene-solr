package org.apache.lucene.search.spans;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.Bits;

/**
 * Expert-only.  Public for use by other weight implementations
 */
public class SpanWeight extends Weight {
  protected final Similarity similarity;
  protected final Map<Term,TermContext> termContexts;
  protected final SpanQuery query;
  protected Similarity.SimWeight stats;

  public SpanWeight(SpanQuery query, IndexSearcher searcher, boolean needsScores) throws IOException {
    super(query);
    this.similarity = searcher.getSimilarity(needsScores);
    this.query = query;

    termContexts = new HashMap<>();
    TreeSet<Term> terms = new TreeSet<>();
    query.extractTerms(terms);
    final IndexReaderContext context = searcher.getTopReaderContext();
    final TermStatistics termStats[] = new TermStatistics[terms.size()];
    int i = 0;
    for (Term term : terms) {
      TermContext state = TermContext.build(context, term);
      termStats[i] = searcher.termStatistics(term, state);
      termContexts.put(term, state);
      i++;
    }
    final String field = query.getField();
    if (field != null) {
      stats = similarity.computeWeight(query.getBoost(),
                                       searcher.collectionStatistics(query.getField()),
                                       termStats);
    }
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    query.extractTerms(terms);
  }

  @Override
  public float getValueForNormalization() throws IOException {
    return stats == null ? 1.0f : stats.getValueForNormalization();
  }

  @Override
  public void normalize(float queryNorm, float topLevelBoost) {
    if (stats != null) {
      stats.normalize(queryNorm, topLevelBoost);
    }
  }

  @Override
  public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
    if (stats == null) {
      return null;
    }
    Terms terms = context.reader().terms(query.getField());
    if (terms != null && terms.hasPositions() == false) {
      throw new IllegalStateException("field \"" + query.getField() + "\" was indexed without position data; cannot run SpanQuery (query=" + query + ")");
    }
    Spans spans = query.getSpans(context, acceptDocs, termContexts);
    return (spans == null) ? null : new SpanScorer(spans, this, similarity.simScorer(stats, context));
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    SpanScorer scorer = (SpanScorer) scorer(context, context.reader().getLiveDocs());
    if (scorer != null) {
      int newDoc = scorer.advance(doc);
      if (newDoc == doc) {
        float freq = scorer.sloppyFreq();
        SimScorer docScorer = similarity.simScorer(stats, context);
        Explanation freqExplanation = Explanation.match(freq, "phraseFreq=" + freq);
        Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
        return Explanation.match(scoreExplanation.getValue(),
            "weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:",
            scoreExplanation);
      }
    }

    return Explanation.noMatch("no matching term");
  }
}
