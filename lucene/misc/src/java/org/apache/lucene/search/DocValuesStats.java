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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** Holds statistics for a DocValues field. */
public abstract class DocValuesStats<T> {

  private int missing = 0;
  private int count = 0;
  private Bits docsWithField;

  protected final String field;

  protected T min;
  protected T max;

  protected DocValuesStats(String field, T initialMin, T initialMax) {
    this.field = field;
    this.min = initialMin;
    this.max = initialMax;
  }

  /**
   * Called after {@link #accumulate(int)} was processed and verified that the document has a value for
   * the field. Implementations should update the statistics based on the value of the current document.
   *
   * @param doc
   *          the document to read the value of for updating statistics.
   * @param count
   *          the updated number of documents with value for this field.
   */
  protected abstract void doAccumulate(int doc, int count) throws IOException;

  /**
   * Initializes this object with the given reader context. Returns whether stats can be computed for this segment (i.e.
   * it does have the requested DocValues field).
   */
  protected boolean init(LeafReaderContext context) throws IOException {
    docsWithField = context.reader().getDocsWithField(field);
    return docsWithField == null;
  }

  final void accumulate(int doc) throws IOException {
    if (docsWithField.get(doc)) {
      ++count;
      doAccumulate(doc, count);
    } else {
      ++missing;
    }
  }

  final void addMissing() {
    ++missing;
  }

  /** The field for which these stats were computed. */
  public final String field() {
    return field;
  }

  /** The number of documents which have a value of the field. */
  public final int count() {
    return count;
  }

  /** The number of documents which do not have a value of the field. */
  public final int missing() {
    return missing;
  }

  /** The minimum value of the field. Undefined when {@link #count()} is zero. */
  public final T min() {
    return min;
  }

  /** The maximum value of the field. Undefined when {@link #count()} is zero. */
  public final T max() {
    return max;
  }

  /** Holds statistics for a numeric DocValues field. */
  public static abstract class NumericDocValuesStats<T extends Number> extends DocValuesStats<T> {

    protected double mean = 0.0;
    protected double variance = 0.0;

    protected NumericDocValues ndv;

    protected NumericDocValuesStats(String field, T initialMin, T initialMax) {
      super(field, initialMin, initialMax);
    }

    @Override
    protected final boolean init(LeafReaderContext context) throws IOException {
      super.init(context);
      ndv = context.reader().getNumericDocValues(field);
      return ndv != null;
    }

    /** The mean of all values of the field. */
    public final double mean() {
      return mean;
    }

    /** Returns the variance of all values of the field. */
    public final double variance() {
      int count = count();
      return count > 0 ? variance / count : 0;
    }

    /** Returns the stdev of all values of the field. */
    public final double stdev() {
      return Math.sqrt(variance());
    }

    /** Returns the sum of values of the field. Note that if the values are large, the {@code sum} might overflow. */
    public abstract T sum();
  }

  /** Holds DocValues statistics for a numeric field storing {@code long} values. */
  public static final class LongDocValuesStats extends NumericDocValuesStats<Long> {

    // To avoid boxing 'long' to 'Long' while the sum is computed, declare it as private variable.
    private long sum = 0;

    public LongDocValuesStats(String field) {
      super(field, Long.MAX_VALUE, Long.MIN_VALUE);
    }

    @Override
    protected void doAccumulate(int doc, int count) throws IOException {
      long val = ndv.get(doc);
      if (val > max) {
        max = val;
      }
      if (val < min) {
        min = val;
      }
      sum += val;
      double oldMean = mean;
      mean += (val - mean) / count;
      variance += (val - mean) * (val - oldMean);
    }

    @Override
    public Long sum() {
      return sum;
    }
  }

  /** Holds DocValues statistics for a numeric field storing {@code double} values. */
  public static final class DoubleDocValuesStats extends NumericDocValuesStats<Double> {

    // To avoid boxing 'double' to 'Double' while the sum is computed, declare it as private variable.
    private double sum = 0;

    public DoubleDocValuesStats(String field) {
      super(field, Double.MAX_VALUE, Double.MIN_VALUE);
    }

    @Override
    protected void doAccumulate(int doc, int count) throws IOException {
      double val = Double.longBitsToDouble(ndv.get(doc));
      if (Double.compare(val, max) > 0) {
        max = val;
      }
      if (Double.compare(val, min) < 0) {
        min = val;
      }
      sum += val;
      double oldMean = mean;
      mean += (val - mean) / count;
      variance += (val - mean) * (val - oldMean);
    }

    @Override
    public Double sum() {
      return sum;
    }
  }

  /** Holds statistics for a sorted-numeric DocValues field. */
  public static abstract class SortedNumericDocValuesStats<T extends Number> extends DocValuesStats<T> {

    protected long valuesCount = 0;
    protected double mean = 0.0;
    protected double variance = 0.0;

    protected SortedNumericDocValues sndv;

    protected SortedNumericDocValuesStats(String field, T initialMin, T initialMax) {
      super(field, initialMin, initialMax);
    }

    @Override
    protected final boolean init(LeafReaderContext context) throws IOException {
      super.init(context);
      sndv = context.reader().getSortedNumericDocValues(field);
      return sndv != null;
    }

    /** The mean of all values of the field. */
    public final double mean() {
      return mean;
    }

    /** Returns the variance of all values of the field. */
    public final double variance() {
      int count = count();
      return count > 0 ? variance / count : 0;
    }

    /** Returns the stdev of all values of the field. */
    public final double stdev() {
      return Math.sqrt(variance());
    }

    /** Returns the total number of values for this field. */
    public final long valuesCount() {
      return valuesCount;
    }

    /** Returns the sum of values of the field. Note that if the values are large, the {@code sum} might overflow. */
    public abstract T sum();
  }

  /** Holds DocValues statistics for a sorted-numeric field storing {@code long} values. */
  public static final class SortedLongDocValuesStats extends SortedNumericDocValuesStats<Long> {

    // To avoid boxing 'long' to 'Long' while the sum is computed, declare it as private variable.
    private long sum = 0;

    public SortedLongDocValuesStats(String field) {
      super(field, Long.MAX_VALUE, Long.MIN_VALUE);
    }

    @Override
    protected void doAccumulate(int doc, int count) throws IOException {
      sndv.setDocument(doc);
      int numValues = sndv.count();
      for (int i = 0; i < numValues; i++) {
        long val = sndv.valueAt(i);
        if (val > max) {
          max = val;
        }
        if (val < min) {
          min = val;
        }
        sum += val;
        double oldMean = mean;
        // for correct "running average computation", increase valuesCount with each value, rather than once before the
        // loop stats.
        ++valuesCount;
        mean += (val - mean) / valuesCount;
        variance += (val - mean) * (val - oldMean);
      }
    }

    @Override
    public Long sum() {
      return sum;
    }
  }

  /** Holds DocValues statistics for a sorted-numeric field storing {@code double} values. */
  public static final class SortedDoubleDocValuesStats extends SortedNumericDocValuesStats<Double> {

    // To avoid boxing 'double' to 'Double' while the sum is computed, declare it as private variable.
    private double sum = 0;

    public SortedDoubleDocValuesStats(String field) {
      super(field, Double.MAX_VALUE, Double.MIN_VALUE);
    }

    @Override
    protected void doAccumulate(int doc, int count) throws IOException {
      sndv.setDocument(doc);
      int numValues = sndv.count();
      for (int i = 0; i < numValues; i++) {
        double val = Double.longBitsToDouble(sndv.valueAt(i));
        if (Double.compare(val, max) > 0) {
          max = val;
        }
        if (Double.compare(val, min) < 0) {
          min = val;
        }
        sum += val;
        double oldMean = mean;
        // for correct "running average computation", increase valuesCount with each value, rather than once before the
        // loop stats.
        ++valuesCount;
        mean += (val - mean) / valuesCount;
        variance += (val - mean) * (val - oldMean);
      }
    }

    @Override
    public Double sum() {
      return sum;
    }
  }

  private static BytesRef copyFrom(BytesRef src, BytesRef dest) {
    if (dest == null) {
      return BytesRef.deepCopyOf(src);
    }

    dest.bytes = ArrayUtil.grow(dest.bytes, src.length);
    System.arraycopy(src.bytes, src.offset, dest.bytes, 0, src.length);
    dest.offset = 0;
    dest.length = src.length;
    return dest;
  }

  /** Holds statistics for a sorted DocValues field. */
  public static class SortedDocValuesStats extends DocValuesStats<BytesRef> {

    protected SortedDocValues sdv;

    protected SortedDocValuesStats(String field) {
      super(field, null, null);
    }

    @Override
    protected final boolean init(LeafReaderContext context) throws IOException {
      super.init(context);
      sdv = context.reader().getSortedDocValues(field);
      return sdv != null;
    }

    @Override
    protected void doAccumulate(int doc, int count) throws IOException {
      BytesRef val = sdv.get(doc);
      if (max == null || val.compareTo(max) > 0) {
        max = copyFrom(val, max);
      }
      if (min == null || val.compareTo(min) < 0) {
        min = copyFrom(val, min);
      }
    }
  }

  /** Holds statistics for a sorted-set DocValues field. */
  public static class SortedSetDocValuesStats extends DocValuesStats<BytesRef> {

    protected SortedSetDocValues ssdv;

    protected SortedSetDocValuesStats(String field) {
      super(field, null, null);
    }

    @Override
    protected final boolean init(LeafReaderContext context) throws IOException {
      super.init(context);
      ssdv = context.reader().getSortedSetDocValues(field);
      return ssdv != null;
    }

    @Override
    protected void doAccumulate(int doc, int count) throws IOException {
      ssdv.setDocument(doc);
      long ord;
      while ((ord = ssdv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        BytesRef val = ssdv.lookupOrd(ord);
        if (max == null || val.compareTo(max) > 0) {
          max = copyFrom(val, max);
        }
        if (min == null || val.compareTo(min) < 0) {
          min = copyFrom(val, min);
        }
      }
    }
  }

}