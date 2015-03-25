package org.apache.lucene.search;

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
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Bits.MatchNoBits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;

/**
 * A range query that works on top of the doc values APIs. Such queries are
 * usually slow since they do not use an inverted index. However, in the
 * dense case where most documents match this query, it <b>might</b> be as
 * fast or faster than a regular {@link NumericRangeQuery}.
 * @lucene.experimental
 */
public final class DocValuesRangeQuery extends Query {

  /** Create a new numeric range query on a numeric doc-values field. The field
   *  must has been indexed with either {@link DocValuesType#NUMERIC} or
   *  {@link DocValuesType#SORTED_NUMERIC} doc values. */
  public static Query newLongRange(String field, Long lowerVal, Long upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeQuery(field, lowerVal, upperVal, includeLower, includeUpper);
  }

  /** Create a new numeric range query on a numeric doc-values field. The field
   *  must has been indexed with {@link DocValuesType#SORTED} or
   *  {@link DocValuesType#SORTED_SET} doc values. */
  public static Query newBytesRefRange(String field, BytesRef lowerVal, BytesRef upperVal, boolean includeLower, boolean includeUpper) {
    return new DocValuesRangeQuery(field, deepCopyOf(lowerVal), deepCopyOf(upperVal), includeLower, includeUpper);
  }

  private static BytesRef deepCopyOf(BytesRef b) {
    if (b == null) {
      return null;
    } else {
      return BytesRef.deepCopyOf(b);
    }
  }

  private final String field;
  private final Object lowerVal, upperVal;
  private final boolean includeLower, includeUpper;

  private DocValuesRangeQuery(String field, Object lowerVal, Object upperVal, boolean includeLower, boolean includeUpper) {
    this.field = Objects.requireNonNull(field);
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DocValuesRangeQuery == false) {
      return false;
    }
    final DocValuesRangeQuery that = (DocValuesRangeQuery) obj;
    return field.equals(that.field)
        && Objects.equals(lowerVal, that.lowerVal)
        && Objects.equals(upperVal, that.upperVal)
        && includeLower == that.includeLower
        && includeUpper == that.includeUpper
        && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, lowerVal, upperVal, includeLower, includeUpper, getBoost());
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field).append(':');
    }
    sb.append(includeLower ? '[' : '{');
    sb.append(lowerVal == null ? "*" : lowerVal.toString());
    sb.append(" TO ");
    sb.append(upperVal == null ? "*" : upperVal.toString());
    sb.append(includeUpper ? ']' : '}');
    sb.append(ToStringUtils.boost(getBoost()));
    return sb.toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (lowerVal == null && upperVal == null) {
      final FieldValueQuery rewritten = new FieldValueQuery(field);
      rewritten.setBoost(getBoost());
      return rewritten;
    }
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    if (lowerVal == null && upperVal == null) {
      throw new IllegalStateException("Both min and max values cannot be null, call rewrite first");
    }
    return new ConstantScoreWeight(DocValuesRangeQuery.this) {

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs, float score) throws IOException {

        final Bits docsWithField = context.reader().getDocsWithField(field);
        if (docsWithField == null || docsWithField instanceof MatchNoBits) {
          return null;
        }

        final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
        final TwoPhaseIterator twoPhaseRange;
        if (lowerVal instanceof Long || upperVal instanceof Long) {

          final SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);

          final long min;
          if (lowerVal == null) {
            min = Long.MIN_VALUE;
          } else if (includeLower) {
            min = (long) lowerVal;
          } else {
            min = 1 + (long) lowerVal;
          }

          final long max;
          if (upperVal == null) {
            max = Long.MAX_VALUE;
          } else if (includeUpper) {
            max = (long) upperVal;
          } else {
            max = -1 + (long) upperVal;
          }

          if (min > max) {
            return null;
          }

          twoPhaseRange = new TwoPhaseNumericRange(values, min, max, approximation, acceptDocs);

        } else if (lowerVal instanceof BytesRef || upperVal instanceof BytesRef) {

          final SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);

          final long minOrd;
          if (lowerVal == null) {
            minOrd = 0;
          } else {
            final long ord = values.lookupTerm((BytesRef) lowerVal);
            if (ord < 0) {
              minOrd = -1 - ord;
            } else if (includeLower) {
              minOrd = ord;
            } else {
              minOrd = ord + 1;
            }
          }

          final long maxOrd;
          if (upperVal == null) {
            maxOrd = values.getValueCount() - 1;
          } else {
            final long ord = values.lookupTerm((BytesRef) upperVal);
            if (ord < 0) {
              maxOrd = -2 - ord;
            } else if (includeUpper) {
              maxOrd = ord;
            } else {
              maxOrd = ord - 1;
            }
          }

          if (minOrd > maxOrd) {
            return null;
          }

          twoPhaseRange = new TwoPhaseOrdRange(values, minOrd, maxOrd, approximation, acceptDocs);

        } else {
          throw new AssertionError();
        }

        return new RangeScorer(this, twoPhaseRange, score);
      }

    };
  }

  private static class TwoPhaseNumericRange extends TwoPhaseIterator {

    private final SortedNumericDocValues values;
    private final long min, max;
    private final Bits acceptDocs;

    TwoPhaseNumericRange(SortedNumericDocValues values, long min, long max, DocIdSetIterator approximation, Bits acceptDocs) {
      super(approximation);
      this.values = values;
      this.min = min;
      this.max = max;
      this.acceptDocs = acceptDocs;
    }

    @Override
    public boolean matches() throws IOException {
      final int doc = approximation.docID();
      if (acceptDocs == null || acceptDocs.get(doc)) {
        values.setDocument(doc);
        final int count = values.count();
        for (int i = 0; i < count; ++i) {
          final long value = values.valueAt(i);
          if (value >= min && value <= max) {
            return true;
          }
        }
      }
      return false;
    }

  }

  private static class TwoPhaseOrdRange extends TwoPhaseIterator {

    private final SortedSetDocValues values;
    private final long minOrd, maxOrd;
    private final Bits acceptDocs;

    TwoPhaseOrdRange(SortedSetDocValues values, long minOrd, long maxOrd, DocIdSetIterator approximation, Bits acceptDocs) {
      super(approximation);
      this.values = values;
      this.minOrd = minOrd;
      this.maxOrd = maxOrd;
      this.acceptDocs = acceptDocs;
    }

    @Override
    public boolean matches() throws IOException {
      final int doc = approximation.docID();
      if (acceptDocs == null || acceptDocs.get(doc)) {
        values.setDocument(doc);
        for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
          if (ord >= minOrd && ord <= maxOrd) {
            return true;
          }
        }
      }
      return false;
    }

  }

  private static class RangeScorer extends Scorer {

    private final TwoPhaseIterator twoPhaseRange;
    private final DocIdSetIterator disi;
    private final float score;

    RangeScorer(Weight weight, TwoPhaseIterator twoPhaseRange, float score) {
      super(weight);
      this.twoPhaseRange = twoPhaseRange;
      this.disi = TwoPhaseIterator.asDocIdSetIterator(twoPhaseRange);
      this.score = score;
    }

    @Override
    public TwoPhaseIterator asTwoPhaseIterator() {
      return twoPhaseRange;
    }

    @Override
    public float score() throws IOException {
      return score;
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public long cost() {
      return disi.cost();
    }

  }

}
