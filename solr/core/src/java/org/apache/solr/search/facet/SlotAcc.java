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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Accumulates statistics separated by a slot number. 
 * There is a separate statistic per slot. The slot is usually an ordinal into a set of values, e.g. tracking a count
 * frequency <em>per term</em>.
 * Sometimes there doesn't need to be a slot distinction, in which case there is just one nominal slot.
 */
public abstract class SlotAcc implements Closeable {
  String key; // todo...
  protected final FacetContext fcontext;
  protected LeafReaderContext currentReaderContext;
  protected int currentDocBase;

  public SlotAcc(FacetContext fcontext) {
    this.fcontext = fcontext;
  }

  /**
   * NOTE: this currently detects when it is being reused and calls resetIterators by comparing reader ords
   * with previous calls to setNextReader.  For this reason, current users must call setNextReader
   * in segment order.  Failure to do so will cause worse performance.
   */
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    LeafReaderContext lastReaderContext = currentReaderContext;
    currentReaderContext = readerContext;
    currentDocBase = currentReaderContext.docBase;
    if (lastReaderContext == null || lastReaderContext.ord >= currentReaderContext.ord) {
      // if we went backwards (or reused) a reader, we need to reset iterators that can be used only once.
      resetIterators();
    }
  }

  /**
   * All subclasses must override this method to collect documents.  This method is called by the 
   * default impl of {@link #collect(DocSet,int,IntFunction)} but it's also neccessary if this accumulator 
   * is used for sorting.
   *
   * @param doc Single Segment docId (relative to the current {@link LeafReaderContext} to collect
   * @param slot The slot number to collect this document in
   * @param slotContext A callback that can be used for Accumulators that would like additional info 
   *        about the current slot -- the {@link IntFunction} is only garunteed to be valid for 
   *        the current slot, and the {@link SlotContext} returned is only valid for the duration 
   *        of the <code>collect()</code> call.
   */
  public abstract void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException;

  /**
   * Bulk collection of all documents in a slot.  The default implementation calls {@link #collect(int,int,IntFunction)}
   *
   * @param docs (global) Documents to collect
   * @param slot The slot number to collect these documents in
   * @param slotContext A callback that can be used for Accumulators that would like additional info 
   *        about the current slot -- the {@link IntFunction} is only garunteed to be valid for 
   *        the current slot, and the {@link SlotContext} returned is only valid for the duration 
   *        of the <code>collect()</code> call.
   */
  public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    int count = 0;
    SolrIndexSearcher searcher = fcontext.searcher;

    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext();) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          if (ctx == null) {
            // should be impossible
            throw new RuntimeException("INTERNAL FACET ERROR");
          }
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        assert doc >= ctx.docBase;
        setNextReader(ctx);
      }
      count++;
      collect(doc - segBase, slot, slotContext); // per-seg collectors
    }
    return count;
  }

  public abstract int compare(int slotA, int slotB);

  public abstract Object getValue(int slotNum) throws IOException;

  public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
    if (key == null) return;
    Object val = getValue(slotNum);
    if (val != null) {
      bucket.add(key, val);
    }
  }

  /** Called to reset the acc to a fresh state, ready for reuse */
  public abstract void reset() throws IOException;

  /** Typically called from setNextReader to reset docValue iterators */
  protected void resetIterators() throws IOException {};

  public abstract void resize(Resizer resizer);

  @Override
  public void close() throws IOException {}

  public static abstract class Resizer {
    public abstract int getNewSize();

    public abstract int getNewSlot(int oldSlot);

    public double[] resize(double[] old, double defaultValue) {
      double[] values = new double[getNewSize()];
      if (defaultValue != 0) {
        Arrays.fill(values, 0, values.length, defaultValue);
      }
      for (int i = 0; i < old.length; i++) {
        double val = old[i];
        if (val != defaultValue) {
          int newSlot = getNewSlot(i);
          if (newSlot >= 0) {
            values[newSlot] = val;
          }
        }
      }
      return values;
    }

    public int[] resize(int[] old, int defaultValue) {
      int[] values = new int[getNewSize()];
      if (defaultValue != 0) {
        Arrays.fill(values, 0, values.length, defaultValue);
      }
      for (int i = 0; i < old.length; i++) {
        int val = old[i];
        if (val != defaultValue) {
          int newSlot = getNewSlot(i);
          if (newSlot >= 0) {
            values[newSlot] = val;
          }
        }
      }
      return values;
    }

    public long[] resize(long[] old, long defaultValue) {
      long[] values = new long[getNewSize()];
      if (defaultValue != 0) {
        Arrays.fill(values, 0, values.length, defaultValue);
      }
      for (int i = 0; i < old.length; i++) {
        long val = old[i];
        if (val != defaultValue) {
          int newSlot = getNewSlot(i);
          if (newSlot >= 0) {
            values[newSlot] = val;
          }
        }
      }
      return values;
    }

    public FixedBitSet resize(FixedBitSet old) {
      FixedBitSet values = new FixedBitSet(getNewSize());
      int oldSize = old.length();

      for(int oldSlot = 0;;) {
        oldSlot = values.nextSetBit(oldSlot);
        if (oldSlot == DocIdSetIterator.NO_MORE_DOCS) break;
        int newSlot = getNewSlot(oldSlot);
        values.set(newSlot);
        if (++oldSlot >= oldSize) break;
      }

      return values;
    }

    public <T> T[] resize(T[] old, T defaultValue) {
      T[] values = (T[]) Array.newInstance(old.getClass().getComponentType(), getNewSize());
      if (defaultValue != null) {
        Arrays.fill(values, 0, values.length, defaultValue);
      }
      for (int i = 0; i < old.length; i++) {
        T val = old[i];
        if (val != defaultValue) {
          int newSlot = getNewSlot(i);
          if (newSlot >= 0) {
            values[newSlot] = val;
          }
        }
      }
      return values;
    }

  } // end class Resizer

  /**
   * Incapsulates information about the current slot, for Accumulators that may want 
   * additional info during collection.
   */
  public static final class SlotContext {
    private final Query slotQuery;
    public SlotContext(Query slotQuery) {
      this.slotQuery = slotQuery;
    }
    public Query getSlotQuery() {
      return slotQuery;
    }
  }
}

// TODO: we should really have a decoupled value provider...
// This would enhance reuse and also prevent multiple lookups of same value across diff stats
abstract class FuncSlotAcc extends SlotAcc {
  protected final ValueSource valueSource;
  protected FunctionValues values;

  public FuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(fcontext);
    this.valueSource = values;
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    super.setNextReader(readerContext);
    values = valueSource.getValues(fcontext.qcontext, readerContext);
  }
}

// have a version that counts the number of times a Slot has been hit? (for avg... what else?)

// TODO: make more sense to have func as the base class rather than double?
// double-slot-func -> func-slot -> slot -> acc
// double-slot-func -> double-slot -> slot -> acc

abstract class DoubleFuncSlotAcc extends FuncSlotAcc {
  double[] result; // TODO: use DoubleArray
  double initialValue;

  public DoubleFuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    this(values, fcontext, numSlots, 0);
  }

  public DoubleFuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots, double initialValue) {
    super(values, fcontext, numSlots);
    this.initialValue = initialValue;
    result = new double[numSlots];
    if (initialValue != 0) {
      reset();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Double.compare(result[slotA], result[slotB]);
  }

  @Override
  public Object getValue(int slot) {
    return result[slot];
  }

  @Override
  public void reset() {
    Arrays.fill(result, initialValue);
  }

  @Override
  public void resize(Resizer resizer) {
    result = resizer.resize(result, initialValue);
  }
}

abstract class LongFuncSlotAcc extends FuncSlotAcc {
  long[] result;
  long initialValue;

  public LongFuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots, long initialValue) {
    super(values, fcontext, numSlots);
    this.initialValue = initialValue;
    result = new long[numSlots];
    if (initialValue != 0) {
      reset();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Long.compare(result[slotA], result[slotB]);
  }

  @Override
  public Object getValue(int slot) {
    return result[slot];
  }

  @Override
  public void reset() {
    Arrays.fill(result, initialValue);
  }

  @Override
  public void resize(Resizer resizer) {
    result = resizer.resize(result, initialValue);
  }
}

abstract class IntSlotAcc extends SlotAcc {
  int[] result; // use LongArray32
  int initialValue;

  public IntSlotAcc(FacetContext fcontext, int numSlots, int initialValue) {
    super(fcontext);
    this.initialValue = initialValue;
    result = new int[numSlots];
    if (initialValue != 0) {
      reset();
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Integer.compare(result[slotA], result[slotB]);
  }

  @Override
  public Object getValue(int slot) {
    return result[slot];
  }

  @Override
  public void reset() {
    Arrays.fill(result, initialValue);
  }

  @Override
  public void resize(Resizer resizer) {
    result = resizer.resize(result, initialValue);
  }
}

class SumSlotAcc extends DoubleFuncSlotAcc {
  public SumSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
  }

  public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
    double val = values.doubleVal(doc); // todo: worth trying to share this value across multiple stats that need it?
    result[slotNum] += val;
  }
}

class SumsqSlotAcc extends DoubleFuncSlotAcc {
  public SumsqSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
  }

  @Override
  public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
    double val = values.doubleVal(doc);
    val = val * val;
    result[slotNum] += val;
  }
}


class AvgSlotAcc extends DoubleFuncSlotAcc {
  int[] counts;

  public AvgSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
    counts = new int[numSlots];
  }

  @Override
  public void reset() {
    super.reset();
    for (int i = 0; i < counts.length; i++) {
      counts[i] = 0;
    }
  }

  @Override
  public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
    double val = values.doubleVal(doc);
    if (val != 0 || values.exists(doc)) {
      result[slotNum] += val;
      counts[slotNum] += 1;
    }
  }

  private double avg(double tot, int count) {
    return count == 0 ? 0 : tot / count; // returns 0 instead of NaN.. todo - make configurable? if NaN, we need to
                                         // handle comparisons though...
  }

  private double avg(int slot) {
    return avg(result[slot], counts[slot]); // calc once and cache in result?
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Double.compare(avg(slotA), avg(slotB));
  }

  @Override
  public Object getValue(int slot) {
    if (fcontext.isShard()) {
      ArrayList lst = new ArrayList(2);
      lst.add(counts[slot]);
      lst.add(result[slot]);
      return lst;
    } else {
      return avg(slot);
    }
  }

  @Override
  public void resize(Resizer resizer) {
    super.resize(resizer);
    counts = resizer.resize(counts, 0);
  }
}

class VarianceSlotAcc extends DoubleFuncSlotAcc {
  int[] counts;
  double[] sum;

  public VarianceSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
    counts = new int[numSlots];
    sum = new double[numSlots];
  }

  @Override
  public void reset() {
    super.reset();
    Arrays.fill(counts, 0);
    Arrays.fill(sum, 0);
  }

  @Override
  public void resize(Resizer resizer) {
    super.resize(resizer);
    this.counts = resizer.resize(this.counts, 0);
    this.sum = resizer.resize(this.sum, 0);
  }

  private double variance(double sumSq, double sum, int count) {
    double val = count == 0 ? 0 : (sumSq / count) - Math.pow(sum / count, 2);
    return val;
  }

  private double variance(int slot) {
    return variance(result[slot], sum[slot], counts[slot]); // calc once and cache in result?
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Double.compare(this.variance(slotA), this.variance(slotB));
  }

  @Override
  public Object getValue(int slot) {
    if (fcontext.isShard()) {
      ArrayList lst = new ArrayList(3);
      lst.add(counts[slot]);
      lst.add(result[slot]);
      lst.add(sum[slot]);
      return lst;
    } else {
      return this.variance(slot);
    }
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    double val = values.doubleVal(doc);
    if (values.exists(doc)) {
      counts[slot]++;
      result[slot] += val * val;
      sum[slot] += val;
    }
  }
}

class StddevSlotAcc extends DoubleFuncSlotAcc {
  int[] counts;
  double[] sum;

  public StddevSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
    super(values, fcontext, numSlots);
    counts = new int[numSlots];
    sum = new double[numSlots];
  }

  @Override
  public void reset() {
    super.reset();
    Arrays.fill(counts, 0);
    Arrays.fill(sum, 0);
  }

  @Override
  public void resize(Resizer resizer) {
    super.resize(resizer);
    this.counts = resizer.resize(this.counts, 0);
    this.result = resizer.resize(this.result, 0);
  }

  private double stdDev(double sumSq, double sum, int count) {
    double val = count == 0 ? 0 : Math.sqrt((sumSq / count) - Math.pow(sum / count, 2)); 
    return val;
  }

  private double stdDev(int slot) {
    return stdDev(result[slot], sum[slot], counts[slot]); // calc once and cache in result?
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Double.compare(this.stdDev(slotA), this.stdDev(slotB));
  }

  @Override
  public Object getValue(int slot) {
    if (fcontext.isShard()) {
      ArrayList lst = new ArrayList(3);
      lst.add(counts[slot]);
      lst.add(result[slot]);
      lst.add(sum[slot]);
      return lst;
    } else {
      return this.stdDev(slot);
    }
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    double val = values.doubleVal(doc);
    if (values.exists(doc)) {
      counts[slot]++;
      result[slot] += val * val;
      sum[slot] += val;
    }
  }
}

abstract class CountSlotAcc extends SlotAcc {
  public CountSlotAcc(FacetContext fcontext) {
    super(fcontext);
  }

  public abstract void incrementCount(int slot, int count);

  public abstract int getCount(int slot);
}

class CountSlotArrAcc extends CountSlotAcc {
  int[] result;

  public CountSlotArrAcc(FacetContext fcontext, int numSlots) {
    super(fcontext);
    result = new int[numSlots];
  }

  @Override
  public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) {
    // TODO: count arrays can use fewer bytes based on the number of docs in
    // the base set (that's the upper bound for single valued) - look at ttf?
    result[slotNum]++;
  }

  @Override
  public int compare(int slotA, int slotB) {
    return Integer.compare(result[slotA], result[slotB]);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return result[slotNum];
  }

  public void incrementCount(int slot, int count) {
    result[slot] += count;
  }

  public int getCount(int slot) {
    return result[slot];
  }

  // internal and expert
  int[] getCountArray() {
    return result;
  }

  @Override
  public void reset() {
    Arrays.fill(result, 0);
  }

  @Override
  public void resize(Resizer resizer) {
    result = resizer.resize(result, 0);
  }
}

class SortSlotAcc extends SlotAcc {
  public SortSlotAcc(FacetContext fcontext) {
    super(fcontext);
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    // no-op
  }

  public int compare(int slotA, int slotB) {
    return slotA - slotB;
  }

  @Override
  public Object getValue(int slotNum) {
    return slotNum;
  }

  @Override
  public void reset() {
    // no-op
  }

  @Override
  public void resize(Resizer resizer) {
    // sort slot only works with direct-mapped accumulators
    throw new UnsupportedOperationException();
  }
}
