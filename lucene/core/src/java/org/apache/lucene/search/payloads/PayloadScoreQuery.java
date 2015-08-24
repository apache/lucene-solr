package org.apache.lucene.search.payloads;

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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

/**
 * A Query class that uses a {@link PayloadFunction} to modify the score of a
 * wrapped SpanQuery
 *
 * NOTE: In order to take advantage of this with the default scoring implementation
 * ({@link DefaultSimilarity}), you must override {@link DefaultSimilarity#scorePayload(int, int, int, BytesRef)},
 * which returns 1 by default.
 *
 * @see org.apache.lucene.search.similarities.Similarity.SimScorer#computePayloadFactor(int, int, int, BytesRef)
 */
public class PayloadScoreQuery extends SpanQuery {

  private final SpanQuery wrappedQuery;
  private final PayloadFunction function;

  /**
   * Creates a new PayloadScoreQuery
   * @param wrappedQuery the query to wrap
   * @param function a PayloadFunction to use to modify the scores
   */
  public PayloadScoreQuery(SpanQuery wrappedQuery, PayloadFunction function) {
    this.wrappedQuery = wrappedQuery;
    this.function = function;
  }

  @Override
  public String getField() {
    return wrappedQuery.getField();
  }

  @Override
  public String toString(String field) {
    return "PayloadSpanQuery[" + wrappedQuery.toString(field) + "; " + function.toString() + "]";
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    SpanWeight innerWeight = wrappedQuery.createWeight(searcher, needsScores);
    if (!needsScores)
      return innerWeight;
    return new PayloadSpanWeight(searcher, innerWeight);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PayloadScoreQuery)) return false;
    if (!super.equals(o)) return false;

    PayloadScoreQuery that = (PayloadScoreQuery) o;

    if (wrappedQuery != null ? !wrappedQuery.equals(that.wrappedQuery) : that.wrappedQuery != null) return false;
    return !(function != null ? !function.equals(that.function) : that.function != null);

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (wrappedQuery != null ? wrappedQuery.hashCode() : 0);
    result = 31 * result + (function != null ? function.hashCode() : 0);
    return result;
  }

  private class PayloadSpanWeight extends SpanWeight {

    private final SpanWeight innerWeight;

    public PayloadSpanWeight(IndexSearcher searcher, SpanWeight innerWeight) throws IOException {
      super(PayloadScoreQuery.this, searcher, null);
      this.innerWeight = innerWeight;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      innerWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
      return innerWeight.getSpans(ctx, requiredPostings.atLeast(Postings.PAYLOADS));
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Spans spans = getSpans(context, Postings.PAYLOADS);
      if (spans == null)
        return null;
      return new PayloadSpanScorer(spans, this, innerWeight.getSimScorer(context));
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      innerWeight.extractTerms(terms);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return innerWeight.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      innerWeight.normalize(queryNorm, topLevelBoost);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      PayloadSpanScorer scorer = (PayloadSpanScorer) scorer(context);
      if (scorer == null || scorer.advance(doc) != doc)
        return Explanation.noMatch("No match");

      SpanWeight innerWeight = ((PayloadSpanWeight)scorer.getWeight()).innerWeight;
      Explanation innerExpl = innerWeight.explain(context, doc);
      scorer.freq();  // force freq calculation
      Explanation payloadExpl = scorer.getPayloadExplanation();

      return Explanation.match(scorer.scoreCurrentDoc(), "PayloadSpanQuery, product of:", innerExpl, payloadExpl);
    }
  }

  private class PayloadSpanScorer extends SpanScorer implements SpanCollector {

    private int payloadsSeen;
    private float payloadScore;

    private PayloadSpanScorer(Spans spans, SpanWeight weight, Similarity.SimScorer docScorer) throws IOException {
      super(spans, weight, docScorer);
    }

    @Override
    protected void doStartCurrentDoc() {
      payloadScore = 0;
      payloadsSeen = 0;
    }

    @Override
    protected void doCurrentSpans() throws IOException {
      spans.collect(this);
    }

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      BytesRef payload = postings.getPayload();
      if (payload == null)
        return;
      float payloadFactor = docScorer.computePayloadFactor(docID(), spans.startPosition(), spans.endPosition(), payload);
      payloadScore = function.currentScore(docID(), getField(), spans.startPosition(), spans.endPosition(),
                                            payloadsSeen, payloadScore, payloadFactor);
      payloadsSeen++;
    }

    protected float getPayloadScore() {
      return function.docScore(docID(), getField(), payloadsSeen, payloadScore);
    }

    protected Explanation getPayloadExplanation() {
      return function.explain(docID(), getField(), payloadsSeen, payloadScore);
    }

    protected float getSpanScore() throws IOException {
      return super.scoreCurrentDoc();
    }

    @Override
    protected float scoreCurrentDoc() throws IOException {
      return getSpanScore() * getPayloadScore();
    }

    @Override
    public void reset() {

    }
  }

}
