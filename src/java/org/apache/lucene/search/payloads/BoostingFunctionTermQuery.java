package org.apache.lucene.search.payloads;
/**
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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.SpanScorer;

import java.io.IOException;


/**
 * The score returned is based on the maximum payload score seen for the Term on the document, as opposed
 * to the average as implemented by {@link org.apache.lucene.search.payloads.BoostingTermQuery}.
 *
 **/
public class BoostingFunctionTermQuery extends SpanTermQuery  implements PayloadQuery{
  protected PayloadFunction function;
  private boolean includeSpanScore;

  public BoostingFunctionTermQuery(Term term, PayloadFunction function) {
    this(term, function, true);
  }

  public BoostingFunctionTermQuery(Term term, PayloadFunction function, boolean includeSpanScore) {
    super(term);
    this.function = function;
    this.includeSpanScore = includeSpanScore;
  }

  

  public Weight createWeight(Searcher searcher) throws IOException {
    return new BoostingFunctionTermWeight(this, searcher);
  }

  protected class BoostingFunctionTermWeight extends SpanWeight {

    public BoostingFunctionTermWeight(BoostingFunctionTermQuery query, Searcher searcher) throws IOException {
      super(query, searcher);
    }

    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      return new BoostingFunctionSpanScorer((TermSpans) query.getSpans(reader), this,
          similarity, reader.norms(query.getField()));
    }

    protected class BoostingFunctionSpanScorer extends SpanScorer {
      //TODO: is this the best way to allocate this?
      protected byte[] payload = new byte[256];
      protected TermPositions positions;
      protected float payloadScore;
      protected int payloadsSeen;

      public BoostingFunctionSpanScorer(TermSpans spans, Weight weight, Similarity similarity,
                                   byte[] norms) throws IOException {
        super(spans, weight, similarity, norms);
        positions = spans.getPositions();
      }

      protected boolean setFreqCurrentDoc() throws IOException {
        if (!more) {
          return false;
        }
        doc = spans.doc();
        freq = 0.0f;
        payloadScore = 0;
        payloadsSeen = 0;
        Similarity similarity1 = getSimilarity();
        while (more && doc == spans.doc()) {
          int matchLength = spans.end() - spans.start();

          freq += similarity1.sloppyFreq(matchLength);
          processPayload(similarity1);

          more = spans.next();//this moves positions to the next match in this document
        }
        return more || (freq != 0);
      }


      protected void processPayload(Similarity similarity) throws IOException {
        if (positions.isPayloadAvailable()) {
          payload = positions.getPayload(payload, 0);
          payloadScore = function.currentScore(doc, term.field(), spans.start(), spans.end(), payloadsSeen, payloadScore,
                  similarity.scorePayload(doc, term.field(), spans.start(), spans.end(), payload, 0, positions.getPayloadLength()));
          payloadsSeen++;

        } else {
          //zero out the payload?
        }
      }

      /**
       *
       * @return {@link #getSpanScore()} * {@link #getPayloadScore()}
       * @throws IOException
       */
      public float score() throws IOException {

        return includeSpanScore ? getSpanScore() * getPayloadScore() : getPayloadScore();
      }

      /**
       * Returns the SpanScorer score only.
       * <p/>
       * Should not be overriden without good cause!
       *
       * @return the score for just the Span part w/o the payload
       * @throws IOException
       *
       * @see #score()
       */
      protected float getSpanScore() throws IOException{
        return super.score();
      }

      /**
       * The score for the payload
       * @return The score, as calculated by {@link PayloadFunction#docScore(int, String, int, float)}
       */
      protected float getPayloadScore() {
        return function.docScore(doc, term.field(), payloadsSeen, payloadScore);
      }


      public Explanation explain(final int doc) throws IOException {
        ComplexExplanation result = new ComplexExplanation();
        Explanation nonPayloadExpl = super.explain(doc);
        result.addDetail(nonPayloadExpl);
        //QUESTION: Is there a way to avoid this skipTo call?  We need to know whether to load the payload or not
        Explanation payloadBoost = new Explanation();
        result.addDetail(payloadBoost);


        float payloadScore = getPayloadScore();
        payloadBoost.setValue(payloadScore);
        //GSI: I suppose we could toString the payload, but I don't think that would be a good idea
        payloadBoost.setDescription("scorePayload(...)");
        result.setValue(nonPayloadExpl.getValue() * payloadScore);
        result.setDescription("btq, product of:");
        result.setMatch(nonPayloadExpl.getValue()==0 ? Boolean.FALSE : Boolean.TRUE); // LUCENE-1303
        return result;
      }

    }
  }

  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((function == null) ? 0 : function.hashCode());
    result = prime * result + (includeSpanScore ? 1231 : 1237);
    return result;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    BoostingFunctionTermQuery other = (BoostingFunctionTermQuery) obj;
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
