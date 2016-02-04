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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.FilterSpans;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

/**
 * This class is very similar to
 * {@link org.apache.lucene.search.spans.SpanTermQuery} except that it factors
 * in the value of the payload located at each of the positions where the
 * {@link org.apache.lucene.index.Term} occurs.
 * <p>
 * NOTE: In order to take advantage of this with the default scoring implementation
 * ({@link DefaultSimilarity}), you must override {@link DefaultSimilarity#scorePayload(int, int, int, BytesRef)},
 * which returns 1 by default.
 * <p>
 * Payload scores are aggregated using a pluggable {@link PayloadFunction}.
 * @see org.apache.lucene.search.similarities.Similarity.SimScorer#computePayloadFactor(int, int, int, BytesRef)
 *
 * @deprecated use {@link PayloadScoreQuery} to wrap {@link SpanTermQuery}
 **/
@Deprecated
public class PayloadTermQuery extends SpanTermQuery {
  protected PayloadFunction function;
  private boolean includeSpanScore;

  public PayloadTermQuery(Term term, PayloadFunction function) {
    this(term, function, true);
  }

  public PayloadTermQuery(Term term, PayloadFunction function,
                                    boolean includeSpanScore) {
    super(term);
    this.function = Objects.requireNonNull(function);
    this.includeSpanScore = includeSpanScore;
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    TermContext context = TermContext.build(searcher.getTopReaderContext(), term);
    return new PayloadTermWeight(context, searcher, needsScores ? Collections.singletonMap(term, context) : null);
  }

  private static class PayloadTermCollector implements SpanCollector {

    BytesRef payload;

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      payload = postings.getPayload();
    }

    @Override
    public void reset() {
      payload = null;
    }

  }

  private class PayloadTermWeight extends SpanTermWeight {

    public PayloadTermWeight(TermContext context, IndexSearcher searcher, Map<Term, TermContext> terms)
        throws IOException {
      super(context, searcher, terms);
    }

    @Override
    public PayloadTermSpanScorer scorer(LeafReaderContext context) throws IOException {
      Spans spans = super.getSpans(context, Postings.PAYLOADS);
      if (spans == null) {
        return null;
      }
      Similarity.SimScorer simScorer = getSimScorer(context);
      PayloadSpans payloadSpans = new PayloadSpans(spans, simScorer);
      return new PayloadTermSpanScorer(payloadSpans, this, simScorer);
    }

    private class PayloadSpans extends FilterSpans {

      private final PayloadTermCollector payloadCollector = new PayloadTermCollector();
      private final SimScorer docScorer;
      float payloadScore;
      int payloadsSeen;

      protected PayloadSpans(Spans in, SimScorer docScorer) {
        super(in);
        this.docScorer = docScorer;
      }

      @Override
      protected AcceptStatus accept(Spans candidate) throws IOException {
        return AcceptStatus.YES;
      }

      @Override
      protected void doStartCurrentDoc() throws IOException {
        payloadScore = 0;
        payloadsSeen = 0;
      }

      @Override
      protected void doCurrentSpans() throws IOException {
        payloadCollector.reset();
        collect(payloadCollector);
        processPayload();
      }

      protected void processPayload() throws IOException {
        float payloadFactor = payloadCollector.payload == null ? 1F :
            docScorer.computePayloadFactor(docID(), startPosition(), endPosition(), payloadCollector.payload);
        payloadScore = function.currentScore(docID(), term.field(), startPosition(), endPosition(),
            payloadsSeen, payloadScore, payloadFactor);
        payloadsSeen++;
      }
    }

    protected class PayloadTermSpanScorer extends SpanScorer {
      private final PayloadSpans spans;

      public PayloadTermSpanScorer(PayloadSpans spans, SpanWeight weight, Similarity.SimScorer docScorer) throws IOException {
        super(weight, spans, docScorer);
        this.spans = spans;
      }

      /**
       * 
       * @return {@link #getSpanScore()} * {@link #getPayloadScore()}
       * @throws IOException if there is a low-level I/O error
       */
      @Override
      public float scoreCurrentDoc() throws IOException {
        return includeSpanScore ? getSpanScore() * getPayloadScore()
            : getPayloadScore();
      }

      /**
       * Returns the SpanScorer score only.
       * <p>
       * Should not be overridden without good cause!
       * 
       * @return the score for just the Span part w/o the payload
       * @throws IOException if there is a low-level I/O error
       * 
       * @see #score()
       */
      protected float getSpanScore() throws IOException {
        return super.scoreCurrentDoc();
      }

      /**
       * The score for the payload
       * 
       * @return The score, as calculated by
       *         {@link PayloadFunction#docScore(int, String, int, float)}
       */
      protected float getPayloadScore() {
        return function.docScore(docID(), term.field(), spans.payloadsSeen, spans.payloadScore);
      }
    }
    
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      PayloadTermSpanScorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          float freq = scorer.sloppyFreq();
          Explanation freqExplanation = Explanation.match(freq, "phraseFreq=" + freq);
          SimScorer docScorer = similarity.simScorer(simWeight, context);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          Explanation expl = Explanation.match(
              scoreExplanation.getValue(),
              "weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
          // now the payloads part
          // QUESTION: Is there a way to avoid this skipTo call? We need to know
          // whether to load the payload or not
          // GSI: I suppose we could toString the payload, but I don't think that
          // would be a good idea
          String field = ((SpanQuery)getQuery()).getField();
          Explanation payloadExpl = function.explain(doc, field, scorer.spans.payloadsSeen, scorer.spans.payloadScore);
          // combined
          if (includeSpanScore) {
            return Explanation.match(
                expl.getValue() * payloadExpl.getValue(),
                "btq, product of:", expl, payloadExpl);
          } else {
            return Explanation.match(payloadExpl.getValue(), "btq(includeSpanScore=false), result of:", payloadExpl);
          }
        }
      }
      
      return Explanation.noMatch("no matching term");
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + function.hashCode();
    result = prime * result + (includeSpanScore ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    PayloadTermQuery other = (PayloadTermQuery) obj;
    return (includeSpanScore == other.includeSpanScore)
         && function.equals(other.function);
  }

}
