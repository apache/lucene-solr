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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.search.SortField;

/**
 * Instantiates {@link FunctionValues} for a particular reader.
 * <br>
 * Often used when creating a {@link FunctionQuery}.
 *
 *
 */
public abstract class ValueSource {

  /**
   * Gets the values for this reader and the context that was previously
   * passed to createWeight()
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

  private static class FakeScorer extends Scorer {

    int current = -1;
    float score = 0;

    FakeScorer() {
      super(null);
    }

    @Override
    public int docID() {
      return current;
    }

    @Override
    public float score() throws IOException {
      return score;
    }

    @Override
    public int freq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Expose this ValueSource as a LongValuesSource
   */
  public LongValuesSource asLongValuesSource() {
    return new LongValuesSource() {
      @Override
      public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        Map context = new IdentityHashMap<>();
        FakeScorer scorer = new FakeScorer();
        context.put("scorer", scorer);
        final FunctionValues fv = ValueSource.this.getValues(context, ctx);
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
      public boolean needsScores() {
        return false;
      }
    };
  }

  /**
   * Expose this ValueSource as a DoubleValuesSource
   */
  public DoubleValuesSource asDoubleValuesSource() {
    return new DoubleValuesSource() {
      @Override
      public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        Map context = new HashMap<>();
        FakeScorer scorer = new FakeScorer();
        context.put("scorer", scorer);
        FunctionValues fv = ValueSource.this.getValues(context, ctx);
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
            return fv.exists(doc);
          }
        };
      }

      @Override
      public boolean needsScores() {
        return true;  // be on the safe side
      }
    };
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
    public int compareBottom(int doc) {
      return Double.compare(bottom, docVals.doubleVal(doc));
    }

    @Override
    public void copy(int slot, int doc) {
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
    public int compareTop(int doc) {
      final double docValue = docVals.doubleVal(doc);
      return Double.compare(topValue, docValue);
    }
  }
}
