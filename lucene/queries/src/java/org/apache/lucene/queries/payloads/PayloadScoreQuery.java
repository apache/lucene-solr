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
package org.apache.lucene.queries.payloads;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.FilterSpans;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

/**
 * A Query class that uses a {@link PayloadFunction} to modify the score of a wrapped SpanQuery
 *
 * NOTE: In order to take advantage of this with the default scoring implementation
 * ({@link ClassicSimilarity}), you must override {@link ClassicSimilarity#scorePayload(int, int, int, BytesRef)},
 * which returns 1 by default.
 *
 * @see org.apache.lucene.search.similarities.Similarity.SimScorer#computePayloadFactor(int, int, int, BytesRef)
 */
public class PayloadScoreQuery extends SpanQuery {

  private final SpanQuery wrappedQuery;
  private final PayloadFunction function;
  private final boolean includeSpanScore;

  /**
   * Creates a new PayloadScoreQuery
   * @param wrappedQuery the query to wrap
   * @param function a PayloadFunction to use to modify the scores
   * @param includeSpanScore include both span score and payload score in the scoring algorithm
   */
  public PayloadScoreQuery(SpanQuery wrappedQuery, PayloadFunction function, boolean includeSpanScore) {
    this.wrappedQuery = Objects.requireNonNull(wrappedQuery);
    this.function = Objects.requireNonNull(function);
    this.includeSpanScore = includeSpanScore;
  }

  /**
   * Creates a new PayloadScoreQuery that includes the underlying span scores
   * @param wrappedQuery the query to wrap
   * @param function a PayloadFunction to use to modify the scores
   */
  public PayloadScoreQuery(SpanQuery wrappedQuery, PayloadFunction function) {
    this(wrappedQuery, function, true);
  }

  @Override
  public String getField() {
    return wrappedQuery.getField();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query matchRewritten = wrappedQuery.rewrite(reader);
    if (wrappedQuery != matchRewritten && matchRewritten instanceof SpanQuery) {
      return new PayloadScoreQuery((SpanQuery)matchRewritten, function, includeSpanScore);
    }
    return super.rewrite(reader);
  }


  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("PayloadScoreQuery(");
    buffer.append(wrappedQuery.toString(field));
    buffer.append(", function: ");
    buffer.append(function.getClass().getSimpleName());
    buffer.append(", includeSpanScore: ");
    buffer.append(includeSpanScore);
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    SpanWeight innerWeight = wrappedQuery.createWeight(searcher, needsScores);
    if (!needsScores)
      return innerWeight;
    return new PayloadSpanWeight(searcher, innerWeight);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(PayloadScoreQuery other) {
    return wrappedQuery.equals(other.wrappedQuery) && 
           function.equals(other.function) && (includeSpanScore == other.includeSpanScore);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + Objects.hashCode(wrappedQuery);
    result = 31 * result + Objects.hashCode(function);
    result = 31 * result + Objects.hashCode(includeSpanScore);
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
    public SpanScorer scorer(LeafReaderContext context) throws IOException {
      Spans spans = getSpans(context, Postings.PAYLOADS);
      if (spans == null)
        return null;
      SimScorer docScorer = innerWeight.getSimScorer(context);
      PayloadSpans payloadSpans = new PayloadSpans(spans, docScorer);
      return new PayloadSpanScorer(this, payloadSpans, docScorer);
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
      PayloadSpanScorer scorer = (PayloadSpanScorer)scorer(context);
      if (scorer == null || scorer.iterator().advance(doc) != doc)
        return Explanation.noMatch("No match");

      scorer.freq();  // force freq calculation
      Explanation payloadExpl = scorer.getPayloadExplanation();

      if (includeSpanScore) {
        SpanWeight innerWeight = ((PayloadSpanWeight) scorer.getWeight()).innerWeight;
        Explanation innerExpl = innerWeight.explain(context, doc);
        return Explanation.match(scorer.scoreCurrentDoc(), "PayloadSpanQuery, product of:", innerExpl, payloadExpl);
      }

      return scorer.getPayloadExplanation();
    }
  }

  private class PayloadSpans extends FilterSpans implements SpanCollector {

    private final SimScorer docScorer;
    public int payloadsSeen;
    public float payloadScore;

    private PayloadSpans(Spans in, SimScorer docScorer) {
      super(in);
      this.docScorer = docScorer;
    }
    
    @Override
    protected AcceptStatus accept(Spans candidate) throws IOException {
      return AcceptStatus.YES;
    }
    
    @Override
    protected void doStartCurrentDoc() {
      payloadScore = 0;
      payloadsSeen = 0;
    }

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      BytesRef payload = postings.getPayload();
      if (payload == null)
        return;
      float payloadFactor = docScorer.computePayloadFactor(docID(), in.startPosition(), in.endPosition(), payload);
      payloadScore = function.currentScore(docID(), getField(), in.startPosition(), in.endPosition(),
                                            payloadsSeen, payloadScore, payloadFactor);
      payloadsSeen++;
    }

    @Override
    public void reset() {}

    @Override
    protected void doCurrentSpans() throws IOException {
      in.collect(this);
    }
  }

  private class PayloadSpanScorer extends SpanScorer {

    private final PayloadSpans spans;

    private PayloadSpanScorer(SpanWeight weight, PayloadSpans spans, Similarity.SimScorer docScorer) throws IOException {
      super(weight, spans, docScorer);
      this.spans = spans;
    }

    protected float getPayloadScore() {
      return function.docScore(docID(), getField(), spans.payloadsSeen, spans.payloadScore);
    }

    protected Explanation getPayloadExplanation() {
      return function.explain(docID(), getField(), spans.payloadsSeen, spans.payloadScore);
    }

    protected float getSpanScore() throws IOException {
      return super.scoreCurrentDoc();
    }

    @Override
    protected float scoreCurrentDoc() throws IOException {
      if (includeSpanScore)
        return getSpanScore() * getPayloadScore();
      return getPayloadScore();
    }

  }

}
