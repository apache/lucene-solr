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
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.spans.FilterSpans;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

/**
 * A Query class that uses a {@link PayloadFunction} to modify the score of a wrapped SpanQuery
 */
public class PayloadScoreQuery extends SpanQuery {

  private final SpanQuery wrappedQuery;
  private final PayloadFunction function;
  private final PayloadDecoder decoder;
  private final boolean includeSpanScore;

  /**
   * Creates a new PayloadScoreQuery
   * @param wrappedQuery the query to wrap
   * @param function a PayloadFunction to use to modify the scores
   * @param decoder a PayloadDecoder to convert payloads into float values
   * @param includeSpanScore include both span score and payload score in the scoring algorithm
   */
  public PayloadScoreQuery(SpanQuery wrappedQuery, PayloadFunction function, PayloadDecoder decoder, boolean includeSpanScore) {
    this.wrappedQuery = Objects.requireNonNull(wrappedQuery);
    this.function = Objects.requireNonNull(function);
    this.decoder = Objects.requireNonNull(decoder);
    this.includeSpanScore = includeSpanScore;
  }

  /**
   * Creates a new PayloadScoreQuery that includes the underlying span scores
   * @param wrappedQuery the query to wrap
   * @param function a PayloadFunction to use to modify the scores
   */
  public PayloadScoreQuery(SpanQuery wrappedQuery, PayloadFunction function, PayloadDecoder decoder) {
    this(wrappedQuery, function, decoder, true);
  }

  @Override
  public String getField() {
    return wrappedQuery.getField();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query matchRewritten = wrappedQuery.rewrite(reader);
    if (wrappedQuery != matchRewritten && matchRewritten instanceof SpanQuery) {
      return new PayloadScoreQuery((SpanQuery)matchRewritten, function, decoder, includeSpanScore);
    }
    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    wrappedQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
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
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    SpanWeight innerWeight = wrappedQuery.createWeight(searcher, scoreMode, boost);
    if (!scoreMode.needsScores())
      return innerWeight;
    return new PayloadSpanWeight(searcher, innerWeight, boost);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(PayloadScoreQuery other) {
    return wrappedQuery.equals(other.wrappedQuery) && 
           function.equals(other.function) && (includeSpanScore == other.includeSpanScore) &&
           Objects.equals(decoder, other.decoder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(wrappedQuery, function, decoder, includeSpanScore);
  }

  private class PayloadSpanWeight extends SpanWeight {

    private final SpanWeight innerWeight;

    public PayloadSpanWeight(IndexSearcher searcher, SpanWeight innerWeight, float boost) throws IOException {
      super(PayloadScoreQuery.this, searcher, null, boost);
      this.innerWeight = innerWeight;
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      innerWeight.extractTermStates(contexts);
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
      LeafSimScorer docScorer = innerWeight.getSimScorer(context);
      PayloadSpans payloadSpans = new PayloadSpans(spans, decoder);
      return new PayloadSpanScorer(this, payloadSpans, docScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return innerWeight.isCacheable(ctx);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      innerWeight.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      PayloadSpanScorer scorer = (PayloadSpanScorer)scorer(context);
      if (scorer == null || scorer.iterator().advance(doc) != doc)
        return Explanation.noMatch("No match");

      scorer.score();  // force freq calculation
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

    private final PayloadDecoder decoder;
    public int payloadsSeen;
    public float payloadScore;

    private PayloadSpans(Spans in, PayloadDecoder decoder) {
      super(in);
      this.decoder = decoder;
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
      float payloadFactor = decoder.computePayloadFactor(payload);
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

    private PayloadSpanScorer(SpanWeight weight, PayloadSpans spans, LeafSimScorer docScorer) throws IOException {
      super(weight, spans, docScorer);
      this.spans = spans;
    }

    protected float getPayloadScore() {
      float score = function.docScore(docID(), getField(), spans.payloadsSeen, spans.payloadScore);
      if (score >= 0 == false) {
        return 0;
      } else {
        return score;
      }
    }

    protected Explanation getPayloadExplanation() {
      Explanation expl = function.explain(docID(), getField(), spans.payloadsSeen, spans.payloadScore);
      if (expl.getValue().floatValue() < 0) {
        expl = Explanation.match(0, "truncated score, max of:", Explanation.match(0f, "minimum score"), expl);
      } else if (Float.isNaN(expl.getValue().floatValue())) {
        expl = Explanation.match(0, "payload score, computed as (score == NaN ? 0 : score) since NaN is an illegal score from:", expl);
      }
      return expl;
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
