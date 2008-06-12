package org.apache.lucene.search.payloads;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.TermSpans;

import java.io.IOException;

/**
 * Copyright 2004 The Apache Software Foundation
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The BoostingTermQuery is very similar to the {@link org.apache.lucene.search.spans.SpanTermQuery} except
 * that it factors in the value of the payload located at each of the positions where the
 * {@link org.apache.lucene.index.Term} occurs.
 * <p>
 * In order to take advantage of this, you must override {@link org.apache.lucene.search.Similarity#scorePayload(String, byte[],int,int)}
 * which returns 1 by default.
 * <p>
 * Payload scores are averaged across term occurrences in the document.  
 * 
 * @see org.apache.lucene.search.Similarity#scorePayload(String, byte[], int, int)
 */
public class BoostingTermQuery extends SpanTermQuery{


  public BoostingTermQuery(Term term) {
    super(term);
  }


  protected Weight createWeight(Searcher searcher) throws IOException {
    return new BoostingTermWeight(this, searcher);
  }

  protected class BoostingTermWeight extends SpanWeight implements Weight {


    public BoostingTermWeight(BoostingTermQuery query, Searcher searcher) throws IOException {
      super(query, searcher);
    }




    public Scorer scorer(IndexReader reader) throws IOException {
      return new BoostingSpanScorer((TermSpans)query.getSpans(reader), this, similarity,
              reader.norms(query.getField()));
    }

    protected class BoostingSpanScorer extends SpanScorer {

      //TODO: is this the best way to allocate this?
      byte[] payload = new byte[256];
      private TermPositions positions;
      protected float payloadScore;
      private int payloadsSeen;

      public BoostingSpanScorer(TermSpans spans, Weight weight,
                                Similarity similarity, byte[] norms) throws IOException {
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
          payloadScore += similarity.scorePayload(term.field(), payload, 0, positions.getPayloadLength());
          payloadsSeen++;

        } else {
          //zero out the payload?
        }

      }

      public float score() throws IOException {

        return super.score() * (payloadsSeen > 0 ? (payloadScore / payloadsSeen) : 1);
      }


      public Explanation explain(final int doc) throws IOException {
        ComplexExplanation result = new ComplexExplanation();
        Explanation nonPayloadExpl = super.explain(doc);
        result.addDetail(nonPayloadExpl);
        //QUESTION: Is there a wau to avoid this skipTo call?  We need to know whether to load the payload or not
        
        Explanation payloadBoost = new Explanation();
        result.addDetail(payloadBoost);
/*
        if (skipTo(doc) == true) {
          processPayload();
        }
*/

        float avgPayloadScore =  (payloadsSeen > 0 ? (payloadScore / payloadsSeen) : 1); 
        payloadBoost.setValue(avgPayloadScore);
        //GSI: I suppose we could toString the payload, but I don't think that would be a good idea 
        payloadBoost.setDescription("scorePayload(...)");
        result.setValue(nonPayloadExpl.getValue() * avgPayloadScore);
        result.setDescription("btq, product of:");
        result.setMatch(nonPayloadExpl.getValue()==0 ? Boolean.FALSE : Boolean.TRUE); // LUCENE-1303
        return result;
      }
    }

  }


  public boolean equals(Object o) {
    if (!(o instanceof BoostingTermQuery))
      return false;
    BoostingTermQuery other = (BoostingTermQuery) o;
    return (this.getBoost() == other.getBoost())
            && this.term.equals(other.term);
  }
}
