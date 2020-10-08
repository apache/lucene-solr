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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.spans.FilterSpans;
import org.apache.lucene.search.spans.FilterSpans.AcceptStatus;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Only return those matches that have a specific payload at the given position.
 */
public class SpanPayloadCheckQuery extends SpanQuery {

  protected final List<BytesRef> payloadToMatch;
  protected final SpanQuery match;
  protected String op = null;

  public String getOp() {
    return op;
  }

  public void setOp(String op) {
    this.op = op;
  }

  /**
   * @param match The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.List} of payloads to match
   */
  public SpanPayloadCheckQuery(SpanQuery match, List<BytesRef> payloadToMatch) {
    this.match = match;
    this.payloadToMatch = payloadToMatch;
  }

  @Override
  public String getField() {
    return match.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    SpanWeight matchWeight = match.createWeight(searcher, scoreMode, boost);
    return new SpanPayloadCheckWeight(searcher, scoreMode.needsScores() ? getTermStates(matchWeight) : null, matchWeight, boost);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query matchRewritten = match.rewrite(reader);
    if (match != matchRewritten && matchRewritten instanceof SpanQuery) {
      return new SpanPayloadCheckQuery((SpanQuery)matchRewritten, payloadToMatch);
    }
    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(match.getField())) {
      match.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }
  }

  /**
   * Weight that pulls its Spans using a PayloadSpanCollector
   */
  public class SpanPayloadCheckWeight extends SpanWeight {

    final SpanWeight matchWeight;

    public SpanPayloadCheckWeight(IndexSearcher searcher, Map<Term, TermStates> termStates, SpanWeight matchWeight, float boost) throws IOException {
      super(SpanPayloadCheckQuery.this, searcher, termStates, boost);
      this.matchWeight = matchWeight;
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      matchWeight.extractTermStates(contexts);
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings) throws IOException {
      // create the correct checker for the operation.
      final PayloadChecker collector = checkerForOp(op);
      Spans matchSpans = matchWeight.getSpans(context, requiredPostings.atLeast(Postings.PAYLOADS));
      return (matchSpans == null) ? null : new FilterSpans(matchSpans) {
        @Override
        protected AcceptStatus accept(Spans candidate) throws IOException {
          collector.reset();
          candidate.collect(collector);
          return collector.match();
        }
      };
    }

    private PayloadChecker checkerForOp(String operation) {
      if (operation == null) {
        return new EQPayloadChecker(null);
      }
      switch (operation) {
        case "gt":
          return new GTPayloadChecker(operation);
        case "gte":
          return new GTEPayloadChecker(operation);
        case "lt":
          return new LTPayloadChecker(operation);
        case "lte":
          return new LTEPayloadChecker(operation);
        case "eq":
          return new EQPayloadChecker(operation);
        default:
          throw new IllegalArgumentException("Unknown operation :" + operation);
      }
    }

    @Override
    public SpanScorer scorer(LeafReaderContext context) throws IOException {
      if (field == null)
        return null;

      Terms terms = context.reader().terms(field);
      if (terms != null && !terms.hasPositions()) {
        throw new IllegalStateException("field \"" + field + "\" was indexed without position data; cannot run SpanQuery (query=" + parentQuery + ")");
      }

      final Spans spans = getSpans(context, Postings.PAYLOADS);
      if (spans == null) {
        return null;
      }
      final LeafSimScorer docScorer = getSimScorer(context);
      return new SpanScorer(this, spans, docScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return matchWeight.isCacheable(ctx);
    }

  }

  private abstract class FloatPayloadChecker extends PayloadChecker {
    public FloatPayloadChecker(String op) {
      super(op);
    }

    @Override
    protected boolean comparePayload(BytesRef source, BytesRef payload) {
      return floatCompare(decodeFloat(payload.bytes, payload.offset), decodeFloat(source.bytes, source.offset));
    }
    private float decodeFloat(byte[] bytes, int offset) {
      return Float.intBitsToFloat(((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16)
          | ((bytes[offset + 2] & 0xFF) <<  8) | (bytes[offset + 3] & 0xFF));
    }
    protected abstract boolean floatCompare(float val, float threshold);
  }

  private class LTPayloadChecker extends FloatPayloadChecker {
    public LTPayloadChecker(String op) {
      super(op);
    }

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val < thresh);
    }
  }

  private class LTEPayloadChecker extends FloatPayloadChecker {
    public LTEPayloadChecker(String op) {
      super(op);
    }

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val <= thresh);
    }
  }

  private class GTPayloadChecker extends FloatPayloadChecker {
    public GTPayloadChecker(String op) {
      super(op);
    }

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val > thresh);
    }
  }

  private class GTEPayloadChecker extends FloatPayloadChecker {
    public GTEPayloadChecker(String op) {
      super(op);
    }

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val >= thresh);
    }
  }
  private class EQPayloadChecker extends FloatPayloadChecker {
    public EQPayloadChecker(String op) {
      super(op);
    }

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return false; // never used
    }

    @Override
    protected boolean comparePayload(BytesRef source, BytesRef payload) {
        return source.bytesEquals(payload);
    }
  }

  private abstract class PayloadChecker implements SpanCollector {

    int upto = 0;
    boolean matches = true;
    // The checking operation lt,gt,lte,gte , null is eq
    String op;

    public PayloadChecker(String op) {
      this.op = op;
    }

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      if (!matches)
        return;
      if (upto >= payloadToMatch.size()) {
        matches = false;
        return;
      }
      BytesRef payload = postings.getPayload();
      if (payloadToMatch.get(upto) == null) {
        matches = payload == null;
        upto++;
        return;
      }
      if (payload == null) {
        matches = false;
        upto++;
        return;
      }
      matches = comparePayload(payloadToMatch.get(upto), payload);
      upto++;
    }

    protected abstract  boolean comparePayload(BytesRef source, BytesRef payload);

    AcceptStatus match() {
      return matches && upto == payloadToMatch.size() ? AcceptStatus.YES : AcceptStatus.NO;
    }

    @Override
    public void reset() {
      this.upto = 0;
      this.matches = true;
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("SpanPayloadCheckQuery(");
    buffer.append(match.toString(field));
    buffer.append(", payloadRef: ");
    for (BytesRef bytes : payloadToMatch) {
      buffer.append(Term.toString(bytes));
      buffer.append(';');
    }
    buffer.append(")");
    return buffer.toString();
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass") // because actually it does even if ides are easily confused
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
        payloadToMatch.equals(((SpanPayloadCheckQuery) other).payloadToMatch) &&
        match.equals(((SpanPayloadCheckQuery) other).match) &&
        (op == null && (((SpanPayloadCheckQuery) other).op == null) ||
            (op != null && op.equals(((SpanPayloadCheckQuery) other).op)));
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + Objects.hashCode(match);
    result = 31 * result + Objects.hashCode(payloadToMatch);
    result = 31 * result + Objects.hashCode(op);
    return result;
  }
}