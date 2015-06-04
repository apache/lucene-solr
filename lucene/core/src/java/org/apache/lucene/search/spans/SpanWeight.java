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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Map;

/**
 * Expert-only.  Public for use by other weight implementations
 */
public abstract class SpanWeight extends Weight {

  protected final Similarity similarity;
  protected final Similarity.SimWeight simWeight;
  protected final SpanCollectorFactory collectorFactory;
  protected final String field;

  /**
   * Create a new SpanWeight
   * @param query the parent query
   * @param searcher the IndexSearcher to query against
   * @param termContexts a map of terms to termcontexts for use in building the similarity.  May
   *                     be null if scores are not required
   * @param collectorFactory a SpanCollectorFactory to be used for Span collection
   * @throws IOException on error
   */
  public SpanWeight(SpanQuery query, IndexSearcher searcher, Map<Term, TermContext> termContexts, SpanCollectorFactory collectorFactory) throws IOException {
    super(query);
    this.field = query.getField();
    this.similarity = searcher.getSimilarity();
    this.collectorFactory = collectorFactory;
    this.simWeight = buildSimWeight(query, searcher, termContexts);
  }

  private Similarity.SimWeight buildSimWeight(SpanQuery query, IndexSearcher searcher, Map<Term, TermContext> termContexts) throws IOException {
    if (termContexts == null || termContexts.size() == 0 || query.getField() == null)
      return null;
    TermStatistics[] termStats = new TermStatistics[termContexts.size()];
    int i = 0;
    for (Term term : termContexts.keySet()) {
      termStats[i] = searcher.termStatistics(term, termContexts.get(term));
      i++;
    }
    CollectionStatistics collectionStats = searcher.collectionStatistics(query.getField());
    return searcher.getSimilarity().computeWeight(query.getBoost(), collectionStats, termStats);
  }

  /**
   * Collect all TermContexts used by this Weight
   * @param contexts a map to add the TermContexts to
   */
  public abstract void extractTermContexts(Map<Term, TermContext> contexts);

  /**
   * Expert: Return a Spans object iterating over matches from this Weight
   * @param ctx a LeafReaderContext for this Spans
   * @param acceptDocs a bitset of documents to check
   * @param collector a SpanCollector to use for postings data collection
   * @return a Spans
   * @throws IOException on error
   */
  public abstract Spans getSpans(LeafReaderContext ctx, Bits acceptDocs, SpanCollector collector) throws IOException;

  /**
   * Expert: Return a Spans object iterating over matches from this Weight, without
   * collecting any postings data.
   * @param ctx a LeafReaderContext for this Spans
   * @param acceptDocs a bitset of documents to check
   * @return a Spans
   * @throws IOException on error
   */
  public final Spans getSpans(LeafReaderContext ctx, Bits acceptDocs) throws IOException {
    return getSpans(ctx, acceptDocs, collectorFactory.newCollector());
  }

  @Override
  public float getValueForNormalization() throws IOException {
    return simWeight == null ? 1.0f : simWeight.getValueForNormalization();
  }

  @Override
  public void normalize(float queryNorm, float topLevelBoost) {
    if (simWeight != null) {
      simWeight.normalize(queryNorm, topLevelBoost);
    }
  }

  @Override
  public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
    if (field == null) {
      return null;
    }
    Terms terms = context.reader().terms(field);
    if (terms != null && terms.hasPositions() == false) {
      throw new IllegalStateException("field \"" + field + "\" was indexed without position data; cannot run SpanQuery (query=" + parentQuery + ")");
    }
    Spans spans = getSpans(context, acceptDocs, collectorFactory.newCollector());
    Similarity.SimScorer simScorer = simWeight == null ? null : similarity.simScorer(simWeight, context);
    return (spans == null) ? null : new SpanScorer(spans, this, simScorer);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    SpanScorer scorer = (SpanScorer) scorer(context, context.reader().getLiveDocs());
    if (scorer != null) {
      int newDoc = scorer.advance(doc);
      if (newDoc == doc) {
        float freq = scorer.sloppyFreq();
        SimScorer docScorer = similarity.simScorer(simWeight, context);
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
