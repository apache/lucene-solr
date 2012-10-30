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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SloppySimScorer;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * This class is very similar to
 * {@link org.apache.lucene.search.spans.SpanTermQuery} except that it factors
 * in the value of the payload located at each of the positions where the
 * {@link org.apache.lucene.index.Term} occurs.
 * <p/>
 * NOTE: In order to take advantage of this with the default scoring implementation
 * ({@link DefaultSimilarity}), you must override {@link DefaultSimilarity#scorePayload(int, int, int, BytesRef)},
 * which returns 1 by default.
 * <p/>
 * Payload scores are aggregated using a pluggable {@link PayloadFunction}.
 * @see org.apache.lucene.search.similarities.Similarity.SloppySimScorer#computePayloadFactor(int, int, int, BytesRef)
 **/
public class PayloadTermQuery extends SpanTermQuery {
  protected PayloadFunction function;
  private boolean includeSpanScore;

  public PayloadTermQuery(Term term, PayloadFunction function) {
    this(term, function, true);
  }

  public PayloadTermQuery(Term term, PayloadFunction function,
      boolean includeSpanScore) {
    super(term);
    this.function = function;
    this.includeSpanScore = includeSpanScore;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new PayloadTermWeight(this, searcher);
  }

  protected class PayloadTermWeight extends SpanWeight {

    public PayloadTermWeight(PayloadTermQuery query, IndexSearcher searcher)
        throws IOException {
      super(query, searcher);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
      return new PayloadTermSpanScorer((TermSpans) query.getSpans(context, acceptDocs, termContexts),
          this, similarity.sloppySimScorer(stats, context));
    }

    protected class PayloadTermSpanScorer extends SpanScorer {
      protected BytesRef payload;
      protected float payloadScore;
      protected int payloadsSeen;
      private final TermSpans termSpans;

      public PayloadTermSpanScorer(TermSpans spans, Weight weight, Similarity.SloppySimScorer docScorer) throws IOException {
        super(spans, weight, docScorer);
        termSpans = spans;
      }

      @Override
      protected boolean setFreqCurrentDoc() throws IOException {
        if (!more) {
          return false;
        }
        doc = spans.doc();
        freq = 0.0f;
        numMatches = 0;
        payloadScore = 0;
        payloadsSeen = 0;
        while (more && doc == spans.doc()) {
          int matchLength = spans.end() - spans.start();

          freq += docScorer.computeSlopFactor(matchLength);
          numMatches++;
          processPayload(similarity);

          more = spans.next();// this moves positions to the next match in this
                              // document
        }
        return more || (freq != 0);
      }

      protected void processPayload(Similarity similarity) throws IOException {
        if (termSpans.isPayloadAvailable()) {
          final DocsAndPositionsEnum postings = termSpans.getPostings();
          payload = postings.getPayload();
          if (payload != null) {
            payloadScore = function.currentScore(doc, term.field(),
                                                 spans.start(), spans.end(), payloadsSeen, payloadScore,
                                                 docScorer.computePayloadFactor(doc, spans.start(), spans.end(), payload));
          } else {
            payloadScore = function.currentScore(doc, term.field(),
                                                 spans.start(), spans.end(), payloadsSeen, payloadScore, 1F);
          }
          payloadsSeen++;

        } else {
          // zero out the payload?
        }
      }

      /**
       * 
       * @return {@link #getSpanScore()} * {@link #getPayloadScore()}
       * @throws IOException if there is a low-level I/O error
       */
      @Override
      public float score() throws IOException {

        return includeSpanScore ? getSpanScore() * getPayloadScore()
            : getPayloadScore();
      }

      /**
       * Returns the SpanScorer score only.
       * <p/>
       * Should not be overridden without good cause!
       * 
       * @return the score for just the Span part w/o the payload
       * @throws IOException if there is a low-level I/O error
       * 
       * @see #score()
       */
      protected float getSpanScore() throws IOException {
        return super.score();
      }

      /**
       * The score for the payload
       * 
       * @return The score, as calculated by
       *         {@link PayloadFunction#docScore(int, String, int, float)}
       */
      protected float getPayloadScore() {
        return function.docScore(doc, term.field(), payloadsSeen, payloadScore);
      }
    }
    
    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      PayloadTermSpanScorer scorer = (PayloadTermSpanScorer) scorer(context, true, false, context.reader().getLiveDocs());
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          float freq = scorer.sloppyFreq();
          SloppySimScorer docScorer = similarity.sloppySimScorer(stats, context);
          Explanation expl = new Explanation();
          expl.setDescription("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:");
          Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "phraseFreq=" + freq));
          expl.addDetail(scoreExplanation);
          expl.setValue(scoreExplanation.getValue());
          // now the payloads part
          // QUESTION: Is there a way to avoid this skipTo call? We need to know
          // whether to load the payload or not
          // GSI: I suppose we could toString the payload, but I don't think that
          // would be a good idea
          String field = ((SpanQuery)getQuery()).getField();
          Explanation payloadExpl = function.explain(doc, field, scorer.payloadsSeen, scorer.payloadScore);
          payloadExpl.setValue(scorer.getPayloadScore());
          // combined
          ComplexExplanation result = new ComplexExplanation();
          if (includeSpanScore) {
            result.addDetail(expl);
            result.addDetail(payloadExpl);
            result.setValue(expl.getValue() * payloadExpl.getValue());
            result.setDescription("btq, product of:");
          } else {
            result.addDetail(payloadExpl);
            result.setValue(payloadExpl.getValue());
            result.setDescription("btq(includeSpanScore=false), result of:");
          }
          result.setMatch(true); // LUCENE-1303
          return result;
        }
      }
      
      return new ComplexExplanation(false, 0.0f, "no matching term");
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((function == null) ? 0 : function.hashCode());
    result = prime * result + (includeSpanScore ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    PayloadTermQuery other = (PayloadTermQuery) obj;
    if (function == null) {
      if (other.function != null)
        return false;
    } else if (!function.equals(other.function))
      return false;
    if (includeSpanScore != other.includeSpanScore)
      return false;
    return true;
  }

}
