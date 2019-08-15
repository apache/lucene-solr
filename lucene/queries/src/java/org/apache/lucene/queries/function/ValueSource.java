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
package org.apache.lucene.queries.function;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.search.SortField;

/**
 * Instantiates {@link FunctionValues} for a particular reader.
 * <br>
 * Often used when creating a {@link FunctionQuery}.
 *
 *
 */
@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
public abstract class ValueSource {

  /**
   * Gets the values for this reader and the context that was previously
   * passed to createWeight().  The values must be consumed in a forward
   * docID manner, and you must call this method again to iterate through
   * the values again.
   */
  public abstract FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException;

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

  /**
   * description of field, used in explain()
   */
  public abstract String description();

  @Override
  public String toString() {
    return description();
  }


  /**
   * Implementations should propagate createWeight to sub-ValueSources which can optionally store
   * weight info in the context. The context object will be passed to getValues()
   * where this info can be retrieved.
   */
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
  }

  /**
   * Returns a new non-threadsafe context map.
   */
  public static Map newContext(IndexSearcher searcher) {
    Map context = new IdentityHashMap();
    context.put("searcher", searcher);
    return context;
  }

  private static class ScoreAndDoc extends Scorable {

    int current = -1;
    float score = 0;

    @Override
    public int docID() {
      return current;
    }

    @Override
    public float score() {
      return score;
    }
  }

  /**
   * Expose this ValueSource as a LongValuesSource
   */
  public LongValuesSource asLongValuesSource() {
    return new WrappedLongValuesSource(this);
  }

  private static class WrappedLongValuesSource extends LongValuesSource {

    private final ValueSource in;

    private WrappedLongValuesSource(ValueSource in) {
      this.in = in;
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      Map context = new IdentityHashMap<>();
      ScoreAndDoc scorer = new ScoreAndDoc();
      context.put("scorer", scorer);
      final FunctionValues fv = in.getValues(context, ctx);
      return new LongValues() {

        @Override
        public long longValue() throws IOException {
          return fv.longVal(scorer.current);
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          scorer.current = doc;
          if (scores != null && scores.advanceExact(doc))
            scorer.score = (float) scores.doubleValue();
          else
            scorer.score = 0;
          return fv.exists(doc);
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WrappedLongValuesSource that = (WrappedLongValuesSource) o;
      return Objects.equals(in, that.in);
    }

    @Override
    public int hashCode() {
      return Objects.hash(in);
    }

    @Override
    public String toString() {
      return in.toString();
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return this;
    }

  }

  /**
   * Expose this ValueSource as a DoubleValuesSource
   */
  public DoubleValuesSource asDoubleValuesSource() {
    return new WrappedDoubleValuesSource(this, null);
  }

  static class WrappedDoubleValuesSource extends DoubleValuesSource {

    final ValueSource in;
    final IndexSearcher searcher;

    private WrappedDoubleValuesSource(ValueSource in, IndexSearcher searcher) {
      this.in = in;
      this.searcher = searcher;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      Map context = new HashMap<>();
      ScoreAndDoc scorer = new ScoreAndDoc();
      context.put("scorer", scorer);
      context.put("searcher", searcher);
      FunctionValues fv = in.getValues(context, ctx);
      return new DoubleValues() {

        @Override
        public double doubleValue() throws IOException {
          return fv.doubleVal(scorer.current);
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          scorer.current = doc;
          if (scores != null && scores.advanceExact(doc)) {
            scorer.score = (float) scores.doubleValue();
          }
          else
            scorer.score = 0;
          // ValueSource will return values even if exists() is false, generally a default
          // of some kind.  To preserve this behaviour with the iterator, we need to always
          // return 'true' here.
          return true;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return true;  // be on the safe side
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
      Map context = new HashMap<>();
      ScoreAndDoc scorer = new ScoreAndDoc();
      scorer.score = scoreExplanation.getValue().floatValue();
      context.put("scorer", scorer);
      context.put("searcher", searcher);
      FunctionValues fv = in.getValues(context, ctx);
      return fv.explain(docId);
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return new WrappedDoubleValuesSource(in, searcher);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WrappedDoubleValuesSource that = (WrappedDoubleValuesSource) o;
      return Objects.equals(in, that.in);
    }

    @Override
    public int hashCode() {
      return Objects.hash(in);
    }

    @Override
    public String toString() {
      return in.toString();
    }

  }

  public static ValueSource fromDoubleValuesSource(DoubleValuesSource in) {
    return new FromDoubleValuesSource(in);
  }

  private static class FromDoubleValuesSource extends ValueSource {

    final DoubleValuesSource in;

    private FromDoubleValuesSource(DoubleValuesSource in) {
      this.in = in;
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      Scorable scorer = (Scorable) context.get("scorer");
      DoubleValues scores = scorer == null ? null : DoubleValuesSource.fromScorer(scorer);

      IndexSearcher searcher = (IndexSearcher) context.get("searcher");
      DoubleValues inner;
      if (searcher != null)
        inner = in.rewrite(searcher).getValues(readerContext, scores);
      else
        inner = in.getValues(readerContext, scores);

      return new FunctionValues() {
        @Override
        public String toString(int doc) throws IOException {
          return in.toString();
        }

        @Override
        public float floatVal(int doc) throws IOException {
          if (inner.advanceExact(doc) == false)
            return 0;
          return (float) inner.doubleValue();
        }

        @Override
        public double doubleVal(int doc) throws IOException {
          if (inner.advanceExact(doc) == false)
            return 0;
          return inner.doubleValue();
        }

        @Override
        public boolean exists(int doc) throws IOException {
          return inner.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FromDoubleValuesSource that = (FromDoubleValuesSource) o;
      return Objects.equals(in, that.in);
    }

    @Override
    public int hashCode() {
      return Objects.hash(in);
    }

    @Override
    public String description() {
      return in.toString();
    }

  }

  //
  // Sorting by function
  //

  /**
   * EXPERIMENTAL: This method is subject to change.
   * <p>
   * Get the SortField for this ValueSource.  Uses the {@link #getValues(java.util.Map, org.apache.lucene.index.LeafReaderContext)}
   * to populate the SortField.
   *
   * @param reverse true if this is a reverse sort.
   * @return The {@link org.apache.lucene.search.SortField} for the ValueSource
   */
  public SortField getSortField(boolean reverse) {
    return new ValueSourceSortField(reverse);
  }

  class ValueSourceSortField extends SortField {
    public ValueSourceSortField(boolean reverse) {
      super(description(), SortField.Type.REWRITEABLE, reverse);
    }

    @Override
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      Map context = newContext(searcher);
      createWeight(context, searcher);
      return new SortField(getField(), new ValueSourceComparatorSource(context), getReverse());
    }
  }

  class ValueSourceComparatorSource extends FieldComparatorSource {
    private final Map context;

    public ValueSourceComparatorSource(Map context) {
      this.context = context;
    }

    @Override
    public FieldComparator<Double> newComparator(String fieldname, int numHits,
                                         int sortPos, boolean reversed) {
      return new ValueSourceComparator(context, numHits);
    }
  }

  /**
   * Implement a {@link org.apache.lucene.search.FieldComparator} that works
   * off of the {@link FunctionValues} for a ValueSource
   * instead of the normal Lucene FieldComparator that works off of a FieldCache.
   */
  class ValueSourceComparator extends SimpleFieldComparator<Double> {
    private final double[] values;
    private FunctionValues docVals;
    private double bottom;
    private final Map fcontext;
    private double topValue;

    ValueSourceComparator(Map fcontext, int numHits) {
      this.fcontext = fcontext;
      values = new double[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Double.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Double.compare(bottom, docVals.doubleVal(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values[slot] = docVals.doubleVal(doc);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      docVals = getValues(fcontext, context);
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(final Double value) {
      this.topValue = value.doubleValue();
    }

    @Override
    public Double value(int slot) {
      return values[slot];
    }

    @Override
    public int compareTop(int doc) throws IOException {
      final double docValue = docVals.doubleVal(doc);
      return Double.compare(topValue, docValue);
    }
  }
}
