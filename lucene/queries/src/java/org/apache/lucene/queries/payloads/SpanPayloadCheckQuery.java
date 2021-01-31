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
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

/** Only return those matches that have a specific payload at the given position. */
public class SpanPayloadCheckQuery extends SpanQuery {

  protected final List<BytesRef> payloadToMatch;
  protected final SpanQuery match;
  protected MatchOperation operation = null;
  protected PayloadType payloadType = PayloadType.STRING;
  /** The payload type. This specifies the decoding of the ByteRef for the payload. */
  public static enum PayloadType {
    /** INT is for a 4 byte payload that is a packed integer */
    INT,
    /** FLOAT is a 4 byte payload decoded to a float(32bit). */
    FLOAT,
    /** STRING is a UTF8 encoded string, decoded from the byte array */
    STRING
  };

  /** The payload type. This specifies the decoding of the ByteRef for the payload. */
  public static enum MatchOperation {
    /** Checks for binary equality of the byte array (default) */
    EQ,
    /** GT Matches if the payload value is greater than the reference */
    GT,
    /** GTE Matches if the payload value is greater than or equal to the reference */
    GTE,
    /** LT Matches if the payload value is less than the reference */
    LT,
    /** LTE Matches if the payload value is less than or equal to the reference */
    LTE
  };

  
  /**
   * @param match The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.List} of payloads to match
   */
  public SpanPayloadCheckQuery(SpanQuery match, List<BytesRef> payloadToMatch) {
    this(match, payloadToMatch, PayloadType.STRING, MatchOperation.EQ);
  }

  /**
   * @param match The underlying {@link org.apache.lucene.search.spans.SpanQuery} to check
   * @param payloadToMatch The {@link java.util.List} of payloads to match
   * @param operation The equality check, lt, lte, gt, gte, or eq. Defaults to eq for equals)
   * @param payloadType specify if the format of the bytes in the payload (String, Integer, or
   *     Float)
   */
  public SpanPayloadCheckQuery(
      SpanQuery match, List<BytesRef> payloadToMatch, PayloadType payloadType, MatchOperation operation) {
    this.match = match;
    this.payloadToMatch = payloadToMatch;
    this.payloadType = payloadType;
    this.operation = operation;
  }

  @Override
  public String getField() {
    return match.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    SpanWeight matchWeight = match.createWeight(searcher, scoreMode, boost);
    return new SpanPayloadCheckWeight(
        searcher,
        scoreMode.needsScores() ? getTermStates(matchWeight) : null,
        matchWeight,
        boost,
        payloadType);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query matchRewritten = match.rewrite(reader);
    if (match != matchRewritten && matchRewritten instanceof SpanQuery) {
      return new SpanPayloadCheckQuery(
          (SpanQuery) matchRewritten, payloadToMatch, payloadType, operation);
    }
    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(match.getField())) {
      match.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }
  }

  /** Weight that pulls its Spans using a PayloadSpanCollector */
  public class SpanPayloadCheckWeight extends SpanWeight {

    final SpanWeight matchWeight;

    public SpanPayloadCheckWeight(
        IndexSearcher searcher,
        Map<Term, TermStates> termStates,
        SpanWeight matchWeight,
        float boost,
        PayloadType payloadType)
        throws IOException {
      super(SpanPayloadCheckQuery.this, searcher, termStates, boost);
      this.matchWeight = matchWeight;
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      matchWeight.extractTermStates(contexts);
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings)
        throws IOException {
      final PayloadChecker collector = new PayloadChecker();
      collector.payloadMatcher =
          PayloadMatcherFactory.createMatcherForOpAndType(payloadType, operation);
      Spans matchSpans = matchWeight.getSpans(context, requiredPostings.atLeast(Postings.PAYLOADS));
      return (matchSpans == null)
          ? null
          : new FilterSpans(matchSpans) {
            @Override
            protected AcceptStatus accept(Spans candidate) throws IOException {
              collector.reset();
              candidate.collect(collector);
              return collector.match();
            }
          };
    }

    @Override
    public SpanScorer scorer(LeafReaderContext context) throws IOException {
      if (field == null) {
        return null;
      }

      Terms terms = context.reader().terms(field);
      if (terms != null && terms.hasPositions() == false) {
        throw new IllegalStateException(
            "field \""
                + field
                + "\" was indexed without position data; cannot run SpanQuery (query="
                + parentQuery
                + ")");
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

  private class PayloadChecker implements SpanCollector {

    int upto = 0;
    boolean matches = true;
    PayloadMatcher payloadMatcher;

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      if (!matches) {
        return;
      }
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
      matches = payloadMatcher.comparePayload(payloadToMatch.get(upto), payload);
      upto++;
    }

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
    if (payloadType != null) {
      buffer.append(", payloadType:").append(payloadType).append(";");
    }
    if (operation != null) {
      buffer.append(", operation:").append(operation).append(";");
    }
    buffer.append(")");
    return buffer.toString();
  }

  @SuppressWarnings(
      "EqualsWhichDoesntCheckParameterClass") // because actually it does even if ides are easily
  // confused
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other)
        && payloadToMatch.equals(((SpanPayloadCheckQuery) other).payloadToMatch)
        && match.equals(((SpanPayloadCheckQuery) other).match)
        && (operation == null && (((SpanPayloadCheckQuery) other).operation == null)
            || (operation != null && operation.equals(((SpanPayloadCheckQuery) other).operation)))
        && (payloadType == null && (((SpanPayloadCheckQuery) other).payloadType == null)
            || (payloadType != null
                && payloadType.equals(((SpanPayloadCheckQuery) other).payloadType)));
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + Objects.hashCode(match);
    result = 31 * result + Objects.hashCode(payloadToMatch);
    result = 31 * result + Objects.hashCode(operation);
    result = 31 * result + Objects.hashCode(payloadType);
    return result;
  }
}
