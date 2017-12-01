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
import java.util.function.DoubleToLongFunction;
import java.util.function.LongToDoubleFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * Base class for producing {@link DoubleValues}
 *
 * To obtain a {@link DoubleValues} object for a leaf reader, clients should call
 * {@link #rewrite(IndexSearcher)} against the top-level searcher, and then
 * call {@link #getValues(LeafReaderContext, DoubleValues)} on the resulting
 * DoubleValuesSource.
 *
 * DoubleValuesSource objects for NumericDocValues fields can be obtained by calling
 * {@link #fromDoubleField(String)}, {@link #fromFloatField(String)}, {@link #fromIntField(String)}
 * or {@link #fromLongField(String)}, or from {@link #fromField(String, LongToDoubleFunction)} if
 * special long-to-double encoding is required.
 *
 * Scores may be used as a source for value calculations by wrapping a {@link Scorer} using
 * {@link #fromScorer(Scorer)} and passing the resulting DoubleValues to {@link #getValues(LeafReaderContext, DoubleValues)}.
 * The scores can then be accessed using the {@link #SCORES} DoubleValuesSource.
 */
public abstract class DoubleValuesSource implements SegmentCacheable {

  /**
   * Returns a {@link DoubleValues} instance for the passed-in LeafReaderContext and scores
   *
   * If scores are not needed to calculate the values (ie {@link #needsScores() returns false}, callers
   * may safely pass {@code null} for the {@code scores} parameter.
   */
  public abstract DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException;

  /**
   * Return true if document scores are needed to calculate values
   */
  public abstract boolean needsScores();

  /**
   * An explanation of the value for the named document.
   *
   * @param ctx the readers context to create the {@link Explanation} for.
   * @param docId the document's id relative to the given context's reader
   * @return an Explanation for the value
   * @throws IOException if an {@link IOException} occurs
   */
  public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
    DoubleValues dv = getValues(ctx, DoubleValuesSource.constant(scoreExplanation.getValue()).getValues(ctx, null));
    if (dv.advanceExact(docId))
      return Explanation.match((float) dv.doubleValue(), this.toString());
    return Explanation.noMatch(this.toString());
  }

  /**
   * Return a DoubleValuesSource specialised for the given IndexSearcher
   *
   * Implementations should assume that this will only be called once.
   * IndexReader-independent implementations can just return {@code this}
   *
   * Queries that use DoubleValuesSource objects should call rewrite() during
   * {@link Query#createWeight(IndexSearcher, boolean, float)} rather than during
   * {@link Query#rewrite(IndexReader)} to avoid IndexReader reference leakage
   */
  public abstract DoubleValuesSource rewrite(IndexSearcher reader) throws IOException;

  /**
   * Create a sort field based on the value of this producer
   * @param reverse true if the sort should be decreasing
   */
  public SortField getSortField(boolean reverse) {
    return new DoubleValuesSortField(this, reverse);
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /**
   * Convert to a LongValuesSource by casting the double values to longs
   */
  public final LongValuesSource toLongValuesSource() {
    return new LongDoubleValuesSource(this);
  }

  private static class LongDoubleValuesSource extends LongValuesSource {

    private final DoubleValuesSource inner;

    private LongDoubleValuesSource(DoubleValuesSource inner) {
      this.inner = inner;
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      DoubleValues in = inner.getValues(ctx, scores);
      return new LongValues() {
        @Override
        public long longValue() throws IOException {
          return (long) in.doubleValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return in.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return inner.isCacheable(ctx);
    }

    @Override
    public boolean needsScores() {
      return inner.needsScores();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LongDoubleValuesSource that = (LongDoubleValuesSource) o;
      return Objects.equals(inner, that.inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
    }

    @Override
    public String toString() {
      return "long(" + inner.toString() + ")";
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return inner.rewrite(searcher).toLongValuesSource();
    }

  }

  /**
   * Creates a DoubleValuesSource that wraps a generic NumericDocValues field
   *
   * @param field the field to wrap, must have NumericDocValues
   * @param decoder a function to convert the long-valued doc values to doubles
   */
  public static DoubleValuesSource fromField(String field, LongToDoubleFunction decoder) {
    return new FieldValuesSource(field, decoder);
  }

  /**
   * Creates a DoubleValuesSource that wraps a double-valued field
   */
  public static DoubleValuesSource fromDoubleField(String field) {
    return fromField(field, Double::longBitsToDouble);
  }

  /**
   * Creates a DoubleValuesSource that wraps a float-valued field
   */
  public static DoubleValuesSource fromFloatField(String field) {
    return fromField(field, (v) -> (double)Float.intBitsToFloat((int)v));
  }

  /**
   * Creates a DoubleValuesSource that wraps a long-valued field
   */
  public static DoubleValuesSource fromLongField(String field) {
    return fromField(field, (v) -> (double) v);
  }

  /**
   * Creates a DoubleValuesSource that wraps an int-valued field
   */
  public static DoubleValuesSource fromIntField(String field) {
    return fromLongField(field);
  }

  /**
   * A DoubleValuesSource that exposes a document's score
   *
   * If this source is used as part of a values calculation, then callers must not
   * pass {@code null} as the {@link DoubleValues} parameter on {@link #getValues(LeafReaderContext, DoubleValues)}
   */
  public static final DoubleValuesSource SCORES = new DoubleValuesSource() {
    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      assert scores != null;
      return scores;
    }

    @Override
    public boolean needsScores() {
      return true;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) {
      return scoreExplanation;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public String toString() {
      return "scores";
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) {
      return this;
    }
  };

  /**
   * Creates a DoubleValuesSource that always returns a constant value
   */
  public static DoubleValuesSource constant(double value) {
    return new ConstantValuesSource(value);
  }

  private static class ConstantValuesSource extends DoubleValuesSource {

    private final double value;

    private ConstantValuesSource(double value) {
      this.value = value;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) {
      return this;
    }


    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return value;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return true;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }


    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) {
      return Explanation.match((float) value, "constant(" + value + ")");
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ConstantValuesSource that = (ConstantValuesSource) o;
      return Double.compare(that.value, value) == 0;
    }

    @Override
    public String toString() {
      return "constant(" + value + ")";
    }

  }

  /**
   * Returns a DoubleValues instance that wraps scores returned by a Scorer
   */
  public static DoubleValues fromScorer(Scorer scorer) {
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return scorer.score();
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        assert scorer.docID() == doc;
        return true;
      }
    };
  }

  private static class FieldValuesSource extends DoubleValuesSource {

    final String field;
    final LongToDoubleFunction decoder;

    private FieldValuesSource(String field, LongToDoubleFunction decoder) {
      this.field = field;
      this.decoder = decoder;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldValuesSource that = (FieldValuesSource) o;
      return Objects.equals(field, that.field) &&
          Objects.equals(decoder, that.decoder);
    }

    @Override
    public String toString() {
      return "double(" + field + ")";
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, decoder);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      final NumericDocValues values = DocValues.getNumeric(ctx.reader(), field);
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return decoder.applyAsDouble(values.longValue());
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return values.advanceExact(target);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, field);
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
      DoubleValues values = getValues(ctx, null);
      if (values.advanceExact(docId))
        return Explanation.match((float) values.doubleValue(), this.toString());
      else
        return Explanation.noMatch(this.toString());
    }

    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

  }

  private static class DoubleValuesSortField extends SortField {

    final DoubleValuesSource producer;

    DoubleValuesSortField(DoubleValuesSource producer, boolean reverse) {
      super(producer.toString(), new DoubleValuesComparatorSource(producer), reverse);
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
      return new DoubleValuesSortField(producer.rewrite(searcher), reverse);
    }
  }

  private static class DoubleValuesHolder {
    DoubleValues values;
  }

  private static class DoubleValuesComparatorSource extends FieldComparatorSource {
    private final DoubleValuesSource producer;

    DoubleValuesComparatorSource(DoubleValuesSource producer) {
      this.producer = producer;
    }

    @Override
    public FieldComparator<Double> newComparator(String fieldname, int numHits,
                                               int sortPos, boolean reversed) {
      return new FieldComparator.DoubleComparator(numHits, fieldname, 0.0){

        LeafReaderContext ctx;
        DoubleValuesHolder holder = new DoubleValuesHolder();

        @Override
        protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
          ctx = context;
          return asNumericDocValues(holder, Double::doubleToLongBits);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          holder.values = producer.getValues(ctx, fromScorer(scorer));
        }
      };
    }
  }

  private static NumericDocValues asNumericDocValues(DoubleValuesHolder in, DoubleToLongFunction converter) {
    return new NumericDocValues() {
      @Override
      public long longValue() throws IOException {
        return converter.applyAsLong(in.values.doubleValue());
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

  /**
   * Create a DoubleValuesSource that returns the score of a particular query
   */
  public static DoubleValuesSource fromQuery(Query query) {
    return new QueryDoubleValuesSource(query);
  }

  private static class QueryDoubleValuesSource extends DoubleValuesSource {

    private final Query query;

    private QueryDoubleValuesSource(Query query) {
      this.query = query;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      QueryDoubleValuesSource that = (QueryDoubleValuesSource) o;
      return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
      return Objects.hash(query);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      throw new UnsupportedOperationException("This DoubleValuesSource must be rewritten");

    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return new WeightDoubleValuesSource(searcher.rewrite(query).createWeight(searcher, true, 1f));
    }

    @Override
    public String toString() {
      return "score(" + query.toString() + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class WeightDoubleValuesSource extends DoubleValuesSource {

    private final Weight weight;

    private WeightDoubleValuesSource(Weight weight) {
      this.weight = weight;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      Scorer scorer = weight.scorer(ctx);
      if (scorer == null)
        return DoubleValues.EMPTY;
      DocIdSetIterator it = scorer.iterator();
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return scorer.score();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          if (it.docID() > doc)
            return false;
          return it.docID() == doc || it.advance(doc) == doc;
        }
      };
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
      return weight.explain(ctx, docId);
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WeightDoubleValuesSource that = (WeightDoubleValuesSource) o;
      return Objects.equals(weight, that.weight);
    }

    @Override
    public int hashCode() {
      return Objects.hash(weight);
    }

    @Override
    public String toString() {
      return "score(" + weight.parentQuery.toString() + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

}
