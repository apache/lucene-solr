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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;

/**
 * This class is very similar to
 * {@link org.apache.lucene.search.spans.SpanNearQuery} except that it factors
 * in the value of the payloads located at each of the positions where the
 * {@link org.apache.lucene.search.spans.TermSpans} occurs.
 * <p>
 * NOTE: In order to take advantage of this with the default scoring implementation
 * ({@link DefaultSimilarity}), you must override {@link DefaultSimilarity#scorePayload(int, int, int, BytesRef)},
 * which returns 1 by default.
 * <p>
 * Payload scores are aggregated using a pluggable {@link PayloadFunction}.
 *
 * @see org.apache.lucene.search.similarities.Similarity.SimScorer#computePayloadFactor(int, int, int, BytesRef)
 *
 * @deprecated use {@link PayloadScoreQuery} to wrap {@link SpanNearQuery}
 */
public class PayloadNearQuery extends SpanNearQuery {

  protected String fieldName;
  protected PayloadFunction function;

  public PayloadNearQuery(SpanQuery[] clauses, int slop, boolean inOrder) {
    this(clauses, slop, inOrder, new AveragePayloadFunction());
  }

  public PayloadNearQuery(SpanQuery[] clauses, int slop, boolean inOrder,
                          PayloadFunction function) {
    super(clauses, slop, inOrder);
    this.fieldName = Objects.requireNonNull(clauses[0].getField(), "all clauses must have same non null field");
    this.function = Objects.requireNonNull(function);
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    List<SpanWeight> subWeights = new ArrayList<>();
    for (SpanQuery q : clauses) {
      subWeights.add(q.createWeight(searcher, false));
    }
    return new PayloadNearSpanWeight(subWeights, searcher, needsScores ? getTermContexts(subWeights) : null);
  }

  @Override
  public PayloadNearQuery clone() {
    int sz = clauses.size();
    SpanQuery[] newClauses = new SpanQuery[sz];

    for (int i = 0; i < sz; i++) {
      newClauses[i] = (SpanQuery) clauses.get(i).clone();
    }
    PayloadNearQuery boostingNearQuery = new PayloadNearQuery(newClauses, slop,
        inOrder, function);
    boostingNearQuery.setBoost(getBoost());
    return boostingNearQuery;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("payloadNear([");
    Iterator<SpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("], ");
    buffer.append(slop);
    buffer.append(", ");
    buffer.append(inOrder);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + fieldName.hashCode();
    result = prime * result + function.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (! super.equals(obj)) {
      return false;
    }
    PayloadNearQuery other = (PayloadNearQuery) obj;
    return fieldName.equals(other.fieldName)
        && function.equals(other.function);
  }

  public class PayloadNearSpanWeight extends SpanNearWeight {

    public PayloadNearSpanWeight(List<SpanWeight> subWeights, IndexSearcher searcher, Map<Term, TermContext> terms)
        throws IOException {
      super(subWeights, searcher, terms);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Spans spans = super.getSpans(context, Postings.PAYLOADS);
      Similarity.SimScorer simScorer = simWeight == null ? null : similarity.simScorer(simWeight, context);
      return (spans == null) ? null : new PayloadNearSpanScorer(spans, this, simScorer);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      PayloadNearSpanScorer scorer = (PayloadNearSpanScorer) scorer(context);
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          Explanation freqExplanation = Explanation.match(freq, "phraseFreq=" + freq);
          SimScorer docScorer = similarity.simScorer(simWeight, context);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          Explanation expl = Explanation.match(
              scoreExplanation.getValue(),
              "weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
          String field = ((SpanQuery)getQuery()).getField();
          // now the payloads part
          Explanation payloadExpl = function.explain(doc, field, scorer.payloadsSeen, scorer.payloadScore);
          // combined
          return Explanation.match(
              expl.getValue() * payloadExpl.getValue(),
              "PayloadNearQuery, product of:",
              expl, payloadExpl);
        }
      }

      return Explanation.noMatch("no matching term");
    }
  }

  public class PayloadNearSpanScorer extends SpanScorer {
    Spans spans;
    protected float payloadScore;
    private int payloadsSeen;
    private final PayloadSpanCollector collector = new PayloadSpanCollector();

    protected PayloadNearSpanScorer(Spans spans, SpanWeight weight, Similarity.SimScorer docScorer) throws IOException {
      super(spans, weight, docScorer);
      this.spans = spans;
    }

    // TODO change the whole spans api to use bytesRef, or nuke spans
    BytesRef scratch = new BytesRef();

    /**
     * By default, uses the {@link PayloadFunction} to score the payloads, but
     * can be overridden to do other things.
     *
     * @param payLoads The payloads
     * @param start The start position of the span being scored
     * @param end The end position of the span being scored
     *
     * @see Spans
     */
    protected void processPayloads(Collection<byte[]> payLoads, int start, int end) {
      for (final byte[] thePayload : payLoads) {
        scratch.bytes = thePayload;
        scratch.offset = 0;
        scratch.length = thePayload.length;
        payloadScore = function.currentScore(docID(), fieldName, start, end,
            payloadsSeen, payloadScore, docScorer.computePayloadFactor(docID(),
                spans.startPosition(), spans.endPosition(), scratch));
        ++payloadsSeen;
      }
    }

    @Override
    protected void doStartCurrentDoc() throws IOException {
      payloadScore = 0;
      payloadsSeen = 0;
    }

    @Override
    protected void doCurrentSpans() throws IOException {
      collector.reset();
      spans.collect(collector);
      processPayloads(collector.getPayloads(), spans.startPosition(), spans.endPosition());
    }

    @Override
    public float scoreCurrentDoc() throws IOException {
      return super.scoreCurrentDoc()
          * function.docScore(docID(), fieldName, payloadsSeen, payloadScore);
    }
  }

}
