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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;

/**
 * A range query that works on top of the doc values APIs. Such queries are
 * usually slow since they do not use an inverted index. However, in the
 * dense case where most documents match this query, it <b>might</b> be as
 * fast or faster than a regular {@link NumericRangeQuery}.
 *
 * <p>
 * <b>NOTE</b>: be very careful using this query: it is
 * typically much slower than using {@code TermsQuery},
 * but in certain specialized cases may be faster.
 *
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
    if (super.equals(obj) == false) {
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
    return 31 * super.hashCode() + Objects.hash(field, lowerVal, upperVal, includeLower, includeUpper);
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
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    if (lowerVal == null && upperVal == null) {
      return new FieldValueQuery(field);
    }
    return super.rewrite(reader);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    if (lowerVal == null && upperVal == null) {
      throw new IllegalStateException("Both min and max values cannot be null, call rewrite first");
    }
    return new RandomAccessWeight(DocValuesRangeQuery.this) {
      
      @Override
      protected Bits getMatchingDocs(final LeafReaderContext context) throws IOException {
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

          return new Bits() {

            @Override
            public boolean get(int doc) {
              values.setDocument(doc);
              final int count = values.count();
              for (int i = 0; i < count; ++i) {
                final long value = values.valueAt(i);
                if (value >= min && value <= max) {
                  return true;
                }
              }
              return false;
            }

            @Override
            public int length() {
              return context.reader().maxDoc();
            }

          };

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

          return new Bits() {

            @Override
            public boolean get(int doc) {
              values.setDocument(doc);
              for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                if (ord >= minOrd && ord <= maxOrd) {
                  return true;
                }
              }
              return false;
            }

            @Override
            public int length() {
              return context.reader().maxDoc();
            }

          };

        } else {
          throw new AssertionError();
        }
      }
    };
  }

}
