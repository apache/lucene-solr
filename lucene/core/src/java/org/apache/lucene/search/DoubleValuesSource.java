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
import java.util.function.DoubleUnaryOperator;
import java.util.function.LongToDoubleFunction;
import java.util.function.ToDoubleBiFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;

/**
 * Base class for producing {@link DoubleValues}
 *
 * To obtain a {@link DoubleValues} object for a leaf reader, clients should
 * call {@link #getValues(LeafReaderContext, DoubleValues)}.
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
public abstract class DoubleValuesSource {

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
   * Create a sort field based on the value of this producer
   * @param reverse true if the sort should be decreasing
   */
  public SortField getSortField(boolean reverse) {
    return new DoubleValuesSortField(this, reverse);
  }

  /**
   * Convert to a LongValuesSource by casting the double values to longs
   */
  public final LongValuesSource toLongValuesSource() {
    return new LongValuesSource() {
      @Override
      public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        DoubleValues in = DoubleValuesSource.this.getValues(ctx, scores);
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
      public boolean needsScores() {
        return DoubleValuesSource.this.needsScores();
      }
    };
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
  };

  /**
   * Creates a DoubleValuesSource that always returns a constant value
   */
  public static DoubleValuesSource constant(double value) {
    return new DoubleValuesSource() {
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
      public String toString() {
        return "constant(" + value + ")";
      }
    };
  }

  /**
   * Creates a DoubleValuesSource that is a function of another DoubleValuesSource
   */
  public static DoubleValuesSource function(DoubleValuesSource in, DoubleUnaryOperator function) {
    return new DoubleValuesSource() {
      @Override
      public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        DoubleValues inputs = in.getValues(ctx, scores);
        return new DoubleValues() {
          @Override
          public double doubleValue() throws IOException {
            return function.applyAsDouble(inputs.doubleValue());
          }

          @Override
          public boolean advanceExact(int doc) throws IOException {
            return inputs.advanceExact(doc);
          }
        };
      }

      @Override
      public boolean needsScores() {
        return in.needsScores();
      }
    };
  }

  /**
   * Creates a DoubleValuesSource that is a function of another DoubleValuesSource and a score
   * @param in        the DoubleValuesSource to use as an input
   * @param function  a function of the form (source, score) == result
   */
  public static DoubleValuesSource scoringFunction(DoubleValuesSource in, ToDoubleBiFunction<Double, Double> function) {
    return new DoubleValuesSource() {
      @Override
      public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        DoubleValues inputs = in.getValues(ctx, scores);
        return new DoubleValues() {
          @Override
          public double doubleValue() throws IOException {
            return function.applyAsDouble(inputs.doubleValue(), scores.doubleValue());
          }

          @Override
          public boolean advanceExact(int doc) throws IOException {
            return inputs.advanceExact(doc);
          }
        };
      }

      @Override
      public boolean needsScores() {
        return true;
      }
    };
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
    public int hashCode() {
      return Objects.hash(field, decoder);
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      final NumericDocValues values = DocValues.getNumeric(ctx.reader(), field);
      final Bits matchingBits = DocValues.getDocsWithField(ctx.reader(), field);
      return toDoubleValues(values, matchingBits, decoder::applyAsDouble);
    }

    @Override
    public boolean needsScores() {
      return false;
    }
  }

  private static class DoubleValuesSortField extends SortField {

    final DoubleValuesSource producer;

    public DoubleValuesSortField(DoubleValuesSource producer, boolean reverse) {
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

  }

  private static class DoubleValuesHolder {
    DoubleValues values;
  }

  private static class DoubleValuesComparatorSource extends FieldComparatorSource {
    private final DoubleValuesSource producer;

    public DoubleValuesComparatorSource(DoubleValuesSource producer) {
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
          return asNumericDocValues(holder, missingValue, Double::doubleToLongBits);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          holder.values = producer.getValues(ctx, fromScorer(scorer));
        }
      };
    }
  }

  private static DoubleValues toDoubleValues(NumericDocValues in, Bits matchingBits, LongToDoubleFunction map) {
    return new DoubleValues() {

      int current = -1;

      @Override
      public double doubleValue() throws IOException {
        return map.applyAsDouble(in.get(current));
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        current = target;
        return matchingBits.get(target);
      }

    };
  }

  private static NumericDocValues asNumericDocValues(DoubleValuesHolder in, Double missingValue, DoubleToLongFunction converter) {
    long missing = converter.applyAsLong(missingValue);
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        try {
          if (in.values.advanceExact(docID))
            return converter.applyAsLong(in.values.doubleValue());
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        return missing;
      }

    };
  }

}
