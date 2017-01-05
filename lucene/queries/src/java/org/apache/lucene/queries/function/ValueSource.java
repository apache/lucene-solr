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
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
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
                                         int sortPos, boolean reversed) throws IOException {
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
