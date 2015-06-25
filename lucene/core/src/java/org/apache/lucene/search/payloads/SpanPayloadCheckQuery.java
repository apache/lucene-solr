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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spans.FilterSpans;
import org.apache.lucene.search.spans.FilterSpans.AcceptStatus;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.ToStringUtils;

/**
 * Only return those matches that have a specific payload at the given position.
 */
public class SpanPayloadCheckQuery extends SpanQuery {

  protected final Collection<byte[]> payloadToMatch;
  protected final SpanQuery match;

  /**
   * @param match The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.Collection} of payloads to match
   */
  public SpanPayloadCheckQuery(SpanQuery match, Collection<byte[]> payloadToMatch) {
    this.match = match;
    this.payloadToMatch = payloadToMatch;
  }

  @Override
  public String getField() {
    return match.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    SpanWeight matchWeight = match.createWeight(searcher, false);
    return new SpanPayloadCheckWeight(searcher, needsScores ? getTermContexts(matchWeight) : null, matchWeight);
  }

  /**
   * Weight that pulls its Spans using a PayloadSpanCollector
   */
  public class SpanPayloadCheckWeight extends SpanWeight {

    final SpanWeight matchWeight;

    public SpanPayloadCheckWeight(IndexSearcher searcher, Map<Term, TermContext> termContexts, SpanWeight matchWeight) throws IOException {
      super(SpanPayloadCheckQuery.this, searcher, termContexts);
      this.matchWeight = matchWeight;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      matchWeight.extractTerms(terms);
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      matchWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings) throws IOException {
      final PayloadSpanCollector collector = new PayloadSpanCollector();
      Spans matchSpans = matchWeight.getSpans(context, requiredPostings.atLeast(Postings.PAYLOADS));
      return (matchSpans == null) ? null : new FilterSpans(matchSpans) {
        @Override
        protected AcceptStatus accept(Spans candidate) throws IOException {
          collector.reset();
          candidate.collect(collector);
          return checkPayloads(collector.getPayloads());
        }
      };
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      if (field == null)
        return null;

      Terms terms = context.reader().terms(field);
      if (terms != null && terms.hasPositions() == false) {
        throw new IllegalStateException("field \"" + field + "\" was indexed without position data; cannot run SpanQuery (query=" + parentQuery + ")");
      }

      Spans spans = getSpans(context, Postings.PAYLOADS);
      Similarity.SimScorer simScorer = simWeight == null ? null : similarity.simScorer(simWeight, context);
      return (spans == null) ? null : new SpanScorer(spans, this, simScorer);
    }
  }

  /**
   * Check to see if the collected payloads match the required set.
   *
   * @param candidate a collection of payloads from the current Spans
   * @return whether or not the payloads match
   */
  protected AcceptStatus checkPayloads(Collection<byte[]> candidate) {
    if (candidate.size() == payloadToMatch.size()){
      //TODO: check the byte arrays are the same
      Iterator<byte[]> toMatchIter = payloadToMatch.iterator();
      //check each of the byte arrays, in order
      for (byte[] candBytes : candidate) {
        //if one is a mismatch, then return false
        if (Arrays.equals(candBytes, toMatchIter.next()) == false){
          return AcceptStatus.NO;
        }
      }
      //we've verified all the bytes
      return AcceptStatus.YES;
    } else {
      return AcceptStatus.NO;
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanPayCheck(");
    buffer.append(match.toString(field));
    buffer.append(", payloadRef: ");
    for (byte[] bytes : payloadToMatch) {
      ToStringUtils.byteArray(buffer, bytes);
      buffer.append(';');
    }
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public SpanPayloadCheckQuery clone() {
    SpanPayloadCheckQuery result = new SpanPayloadCheckQuery((SpanQuery) match.clone(), payloadToMatch);
    result.setBoost(getBoost());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (! super.equals(o)) {
      return false;
    }
    SpanPayloadCheckQuery other = (SpanPayloadCheckQuery)o;
    return this.payloadToMatch.equals(other.payloadToMatch);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = (h * 63) ^ payloadToMatch.hashCode();
    return h;
  }
}
