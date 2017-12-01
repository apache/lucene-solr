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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * Base class for producing {@link LongValues}
 *
 * To obtain a {@link LongValues} object for a leaf reader, clients should
 * call {@link #rewrite(IndexSearcher)} against the top-level searcher, and
 * then {@link #getValues(LeafReaderContext, DoubleValues)}.
 *
 * LongValuesSource objects for long and int-valued NumericDocValues fields can
 * be obtained by calling {@link #fromLongField(String)} and {@link #fromIntField(String)}.
 *
 * To obtain a LongValuesSource from a float or double-valued NumericDocValues field,
 * use {@link DoubleValuesSource#fromFloatField(String)} or {@link DoubleValuesSource#fromDoubleField(String)}
 * and then call {@link DoubleValuesSource#toLongValuesSource()}.
 */
public abstract class LongValuesSource implements SegmentCacheable {

  /**
   * Returns a {@link LongValues} instance for the passed-in LeafReaderContext and scores
   *
   * If scores are not needed to calculate the values (ie {@link #needsScores() returns false}, callers
   * may safely pass {@code null} for the {@code scores} parameter.
   */
  public abstract LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException;

  /**
   * Return true if document scores are needed to calculate values
   */
  public abstract boolean needsScores();

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /**
   * Return a LongValuesSource specialised for the given IndexSearcher
   *
   * Implementations should assume that this will only be called once.
   * IndexSearcher-independent implementations can just return {@code this}
   */
  public abstract LongValuesSource rewrite(IndexSearcher searcher) throws IOException;

  /**
   * Create a sort field based on the value of this producer
   * @param reverse true if the sort should be decreasing
   */
  public SortField getSortField(boolean reverse) {
    return new LongValuesSortField(this, reverse);
  }

  /**
   * Convert to a DoubleValuesSource by casting long values to doubles
   */
  public DoubleValuesSource toDoubleValuesSource() {
    return new DoubleLongValuesSource(this);
  }

  private static class DoubleLongValuesSource extends DoubleValuesSource {

    private final LongValuesSource inner;

    private DoubleLongValuesSource(LongValuesSource inner) {
      this.inner = inner;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      LongValues v = inner.getValues(ctx, scores);
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return (double) v.longValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return v.advanceExact(doc);
        }
      };
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return inner.rewrite(searcher).toDoubleValuesSource();
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return inner.isCacheable(ctx);
    }

    @Override
    public String toString() {
      return "double(" + inner.toString() + ")";
    }

    @Override
    public boolean needsScores() {
      return inner.needsScores();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DoubleLongValuesSource that = (DoubleLongValuesSource) o;
      return Objects.equals(inner, that.inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
    }
  }

  /**
   * Creates a LongValuesSource that wraps a long-valued field
   */
  public static LongValuesSource fromLongField(String field) {
    return new FieldValuesSource(field);
  }

  /**
   * Creates a LongValuesSource that wraps an int-valued field
   */
  public static LongValuesSource fromIntField(String field) {
    return fromLongField(field);
  }

  /**
   * Creates a LongValuesSource that always returns a constant value
   */
  public static LongValuesSource constant(long value) {
    return new ConstantLongValuesSource(value);
  }

  private static class ConstantLongValuesSource extends LongValuesSource {

    private final long value;

    private ConstantLongValuesSource(long value) {
      this.value = value;
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return new LongValues() {
        @Override
        public long longValue() throws IOException {
          return value;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return true;
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConstantLongValuesSource that = (ConstantLongValuesSource) o;
      return value == that.value;
    }

    @Override
    public String toString() {
      return "constant(" + value + ")";
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

  }

  private static class FieldValuesSource extends LongValuesSource {

    final String field;

    private FieldValuesSource(String field) {
      this.field = field;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldValuesSource that = (FieldValuesSource) o;
      return Objects.equals(field, that.field);
    }

    @Override
    public String toString() {
      return "long(" + field + ")";
    }

    @Override
    public int hashCode() {
      return Objects.hash(field);
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      final NumericDocValues values = DocValues.getNumeric(ctx.reader(), field);
      return toLongValues(values);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }
  }

  private static class LongValuesSortField extends SortField {

    final LongValuesSource producer;

    public LongValuesSortField(LongValuesSource producer, boolean reverse) {
      super(producer.toString(), new LongValuesComparatorSource(producer), reverse);
      this.producer = producer;
    }

    @Override
    public boolean needsScores() {
      return producer.needsScores();
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder("<");
      buffer.append(getField()).append(">");
      if (reverse)
        buffer.append("!");
      return buffer.toString();
    }

    @Override
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      return new LongValuesSortField(producer.rewrite(searcher), reverse);
    }
  }

  private static class LongValuesHolder {
    LongValues values;
  }

  private static class LongValuesComparatorSource extends FieldComparatorSource {
    private final LongValuesSource producer;

    public LongValuesComparatorSource(LongValuesSource producer) {
      this.producer = producer;
    }

    @Override
    public FieldComparator<Long> newComparator(String fieldname, int numHits,
                                                 int sortPos, boolean reversed) {
      return new FieldComparator.LongComparator(numHits, fieldname, 0L){

        LeafReaderContext ctx;
        LongValuesHolder holder = new LongValuesHolder();

        @Override
        protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
          ctx = context;
          return asNumericDocValues(holder);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          holder.values = producer.getValues(ctx, DoubleValuesSource.fromScorer(scorer));
        }
      };
    }
  }

  private static LongValues toLongValues(NumericDocValues in) {
    return new LongValues() {
      @Override
      public long longValue() throws IOException {
        return in.longValue();
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return in.advanceExact(target);
      }

    };
  }

  private static NumericDocValues asNumericDocValues(LongValuesHolder in) {
    return new NumericDocValues() {
      @Override
      public long longValue() throws IOException {
        return in.values.longValue();
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        return in.values.advanceExact(target);
      }

      @Override
      public int docID() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long cost() {
        throw new UnsupportedOperationException();
      }
    };
  }

}
