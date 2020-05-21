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

package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.function.IntFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.schema.SchemaField;

/**
 * Accumulates stats separated by slot number for the fields with {@link org.apache.lucene.index.DocValues}
 */
public abstract class DocValuesAcc extends SlotAcc {
  SchemaField sf;

  public DocValuesAcc(FacetContext fcontext, SchemaField sf) throws IOException {
    super(fcontext);
    this.sf = sf;
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    if (advanceExact(doc)) {
      collectValues(doc, slot);
    }
  }

  protected abstract void collectValues(int doc, int slot) throws IOException;

  /**
   * Wrapper to {@code org.apache.lucene.index.DocValuesIterator#advanceExact(int)}
   * returns whether or not given {@code doc} has value
   */
  protected abstract boolean advanceExact(int doc) throws IOException;


  /**
   * Accumulator for {@link NumericDocValues}
   */
  abstract class NumericDVAcc extends DocValuesAcc {
    NumericDocValues values;

  public NumericDVAcc(FacetContext fcontext, SchemaField sf) throws IOException {
      super(fcontext, sf);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getNumeric(readerContext.reader(), sf.getName());
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }
  }

  /**
   * Accumulator for {@link SortedNumericDocValues}
   */
  abstract static class SortedNumericDVAcc extends DocValuesAcc {
    SortedNumericDocValues values;

  public SortedNumericDVAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getSortedNumeric(readerContext.reader(), sf.getName());
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }
  }

  abstract static class LongSortedNumericDVAcc extends SortedNumericDVAcc {
    long[] result;
    long initialValue;

  public LongSortedNumericDVAcc(FacetContext fcontext, SchemaField sf, int numSlots, long initialValue) throws IOException {
      super(fcontext, sf, numSlots);
      this.result = new long[numSlots];
      this.initialValue = initialValue;
      if (initialValue != 0) {
        Arrays.fill(result, initialValue);
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Long.compare(result[slotA], result[slotB]);
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      return result[slotNum];
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(result, initialValue);
    }

    @Override
    public void resize(Resizer resizer) {
    this.result = resizer.resize(result, initialValue);
    }

  }

  abstract static class DoubleSortedNumericDVAcc extends SortedNumericDVAcc {
    double[] result;
    double initialValue;

  public DoubleSortedNumericDVAcc(FacetContext fcontext, SchemaField sf, int numSlots, double initialValue) throws IOException {
      super(fcontext, sf, numSlots);
      this.result = new double[numSlots];
      this.initialValue = initialValue;
      if (initialValue != 0) {
        Arrays.fill(result, initialValue);
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(result[slotA], result[slotB]);
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      return result[slotNum];
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(result, initialValue);
    }

    @Override
    public void resize(Resizer resizer) {
    this.result = resizer.resize(result, initialValue);
    }

    /**
     * converts given long value to double based on field type
     */
    protected double getDouble(long val) {
      switch (sf.getType().getNumberType()) {
        case INTEGER:
        case LONG:
        case DATE:
          return val;
        case FLOAT:
          return NumericUtils.sortableIntToFloat((int) val);
        case DOUBLE:
          return NumericUtils.sortableLongToDouble(val);
        default:
          // this would never happen
          return 0.0d;
      }
    }

  }

  /**
   * Base class for standard deviation and variance computation for fields with {@link SortedNumericDocValues}
   */
  abstract static class SDVSortedNumericAcc extends DoubleSortedNumericDVAcc {
    int[] counts;
    double[] sum;

  public SDVSortedNumericAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
      this.counts = new int[numSlots];
      this.sum = new double[numSlots];
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      for (int i = 0, count = values.docValueCount(); i < count; i++) {
        double val = getDouble(values.nextValue());
        result[slot] += val * val;
        sum[slot] += val;
        counts[slot]++;
      }
    }

    protected abstract double computeVal(int slot);

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(computeVal(slotA), computeVal(slotB));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
        ArrayList lst = new ArrayList(3);
        lst.add(counts[slot]);
        lst.add(result[slot]);
        lst.add(sum[slot]);
        return lst;
      } else {
        return computeVal(slot);
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      Arrays.fill(counts, 0);
      Arrays.fill(sum, 0);
    }

    @Override
    public void resize(Resizer resizer) {
      super.resize(resizer);
    this.counts = resizer.resize(counts, 0);
    this.sum = resizer.resize(sum, 0);
    }
  }

  /**
   * Accumulator for {@link SortedDocValues}
   */
  abstract class SortedDVAcc extends DocValuesAcc {
    SortedDocValues values;

  public SortedDVAcc(FacetContext fcontext, SchemaField sf) throws IOException {
      super(fcontext, sf);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getSorted(readerContext.reader(), sf.getName());
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }
  }

  /**
   * Accumulator for {@link SortedSetDocValues}
   */
  abstract static class SortedSetDVAcc extends DocValuesAcc {
    SortedSetDocValues values;

  public SortedSetDVAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getSortedSet(readerContext.reader(), sf.getName());
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }
  }

  abstract static class LongSortedSetDVAcc extends SortedSetDVAcc {
    long[] result;
    long initialValue;

  public LongSortedSetDVAcc(FacetContext fcontext, SchemaField sf, int numSlots, long initialValue) throws IOException {
      super(fcontext, sf, numSlots);
      result = new long[numSlots];
      this.initialValue = initialValue;
      if (initialValue != 0) {
        Arrays.fill(result, initialValue);
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Long.compare(result[slotA], result[slotB]);
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      return result[slotNum];
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(result, initialValue);
    }

    @Override
    public void resize(Resizer resizer) {
    this.result = resizer.resize(result, initialValue);
    }
  }

  abstract static class DoubleSortedSetDVAcc extends SortedSetDVAcc {
    double[] result;
    double initialValue;

  public DoubleSortedSetDVAcc(FacetContext fcontext, SchemaField sf, int numSlots, long initialValue) throws IOException {
      super(fcontext, sf, numSlots);
      result = new double[numSlots];
      this.initialValue = initialValue;
      if (initialValue != 0) {
        Arrays.fill(result, initialValue);
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(result[slotA], result[slotB]);
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      return result[slotNum];
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(result, initialValue);
    }

    @Override
    public void resize(Resizer resizer) {
    this.result = resizer.resize(result, initialValue);
    }
  }

  /**
   * Base class for standard deviation and variance computation for fields with {@link SortedSetDocValues}
   */
  abstract static class SDVSortedSetAcc extends DoubleSortedSetDVAcc {
    int[] counts;
    double[] sum;

  public SDVSortedSetAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
      this.counts = new int[numSlots];
      this.sum = new double[numSlots];
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      long ord;
      while ((ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        BytesRef term = values.lookupOrd(ord);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date ? ((Date) obj).getTime() : ((Number) obj).doubleValue();
        result[slot] += val * val;
        sum[slot] += val;
        counts[slot]++;
      }
    }

    protected abstract double computeVal(int slot);

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(computeVal(slotA), computeVal(slotB));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
        ArrayList lst = new ArrayList(3);
        lst.add(counts[slot]);
        lst.add(result[slot]);
        lst.add(sum[slot]);
        return lst;
      } else {
        return computeVal(slot);
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      Arrays.fill(counts, 0);
      Arrays.fill(sum, 0);
    }

    @Override
    public void resize(Resizer resizer) {
      super.resize(resizer);
    this.counts = resizer.resize(counts, 0);
    this.sum = resizer.resize(sum, 0);
    }
  }
}
