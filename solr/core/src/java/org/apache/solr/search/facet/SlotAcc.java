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
import java.util.Collections;
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

  @Override public String toString() { return key; }
  
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
   * default impl of {@link #collect(DocSet, int, IntFunction)} but it's also neccessary if this accumulator
   * is used for sorting.
   *
   * @param doc         Single Segment docId (relative to the current {@link LeafReaderContext} to collect
   * @param slot        The slot number to collect this document in
   * @param slotContext A callback that can be used for Accumulators that would like additional info
   *                    about the current slot -- the {@link IntFunction} is only garunteed to be valid for
   *                    the current slot, and the {@link SlotContext} returned is only valid for the duration
   *                    of the <code>collect()</code> call.
   */
  public abstract void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException;

  /**
   * Bulk collection of all documents in a slot.  The default implementation calls {@link #collect(int, int, IntFunction)}
   *
   * @param docs        (global) Documents to collect
   * @param slot        The slot number to collect these documents in
   * @param slotContext A callback that can be used for Accumulators that would like additional info
   *                    about the current slot -- the {@link IntFunction} is only garunteed to be valid for
   *                    the current slot, and the {@link SlotContext} returned is only valid for the duration
   *                    of the <code>collect()</code> call.
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
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
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

  /**
   * Called to reset the acc to a fresh state, ready for reuse
   */
  public abstract void reset() throws IOException;

  /**
   * Typically called from setNextReader to reset docValue iterators
   */
  protected void resetIterators() throws IOException {
  }

  ;

  public abstract void resize(Resizer resizer);

  @Override
  public void close() throws IOException {
  }

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

      for (int oldSlot = 0; ; ) {
        oldSlot = values.nextSetBit(oldSlot);
        if (oldSlot == DocIdSetIterator.NO_MORE_DOCS) break;
        int newSlot = getNewSlot(oldSlot);
        values.set(newSlot);
        if (++oldSlot >= oldSize) break;
      }

      return values;
    }

    public <T> T[] resize(T[] old, T defaultValue) {
      @SuppressWarnings({"unchecked"})
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
  public static class SlotContext {
    private final Query slotQuery;

    public SlotContext(Query slotQuery) {
      this.slotQuery = slotQuery;
    }

    /**
     * behavior of this method is undefined if {@link #isAllBuckets} returns <code>true</code>
     */
    public Query getSlotQuery() {
      return slotQuery;
    }

    /** 
     * @return true if and only if this slot corrisponds to the <code>allBuckets</code> bucket.
     * @see #getSlotQuery 
     */
    public boolean isAllBuckets() {
      return false;
    }
  }


  // TODO: we should really have a decoupled value provider...
// This would enhance reuse and also prevent multiple lookups of same value across diff stats
  public abstract static class FuncSlotAcc extends SlotAcc {
    protected final ValueSource valueSource;
    protected FunctionValues values;

  public FuncSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(fcontext);
      this.valueSource = values;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = valueSource.getValues(fcontext.qcontext, readerContext);
    }
  }

// have a version that counts the number of times a Slot has been hit? (for avg... what else?)

// TODO: make more sense to have func as the base class rather than double?
// double-slot-func -> func-slot -> slot -> acc
// double-slot-func -> double-slot -> slot -> acc

  public abstract static class DoubleFuncSlotAcc extends FuncSlotAcc {
    protected double[] result; // TODO: use DoubleArray
    protected double initialValue;

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

  public abstract static class LongFuncSlotAcc extends FuncSlotAcc {
    protected long[] result;
    protected long initialValue;

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

  public abstract static class IntSlotAcc extends SlotAcc {
    protected int[] result; // use LongArray32
    protected int initialValue;

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

  static class SumSlotAcc extends DoubleFuncSlotAcc {
    public SumSlotAcc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots);
    }

    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
      double val = values.doubleVal(doc); // todo: worth trying to share this value across multiple stats that need it?
      result[slotNum] += val;
    }
  }

  static class SumsqSlotAcc extends DoubleFuncSlotAcc {
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


  static class AvgSlotAcc extends DoubleFuncSlotAcc {
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

    private double avg(int slot) {
      return AggUtil.avg(result[slot], counts[slot]); // calc once and cache in result?
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(avg(slotA), avg(slotB));
    }

    @Override
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
      ArrayList<Object> lst = new ArrayList<>(2);
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

  static class VarianceSlotAcc extends DoubleFuncSlotAcc {
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

    private double variance(int slot) {
    return AggUtil.uncorrectedVariance(result[slot], sum[slot], counts[slot]); // calc once and cache in result?
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(this.variance(slotA), this.variance(slotB));
    }

    @Override
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
      ArrayList<Object> lst = new ArrayList<>(3);
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

  static class StddevSlotAcc extends DoubleFuncSlotAcc {
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

    private double stdDev(int slot) {
    return AggUtil.uncorrectedStdDev(result[slot], sum[slot], counts[slot]); // calc once and cache in result?
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(this.stdDev(slotA), this.stdDev(slotB));
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
        ArrayList<Object> lst = new ArrayList<>(3);
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

  /**
   * Implemented by some SlotAccs if they are capable of being used for
   * sweep collecting in compatible facet processors
   * @see FacetFieldProcessor#registerSweepingAccIfSupportedByCollectAcc()
   */
  static interface SweepableSlotAcc<T extends SlotAcc> {
    /**
     * Called by processors if they support sweeping. Implementations will often
     * return self or null (the latter indicating that all necessary collection will
     * be covered by the "sweeping" data structures registered with the specified
     * baseSweepingAcc as a result of the call to this method).
     *
     * If an implementing instance chooses to replace itself with another {@link SlotAcc}, it must
     * call {@link SweepingCountSlotAcc#registerMapping(SlotAcc, SlotAcc)} on the specified
     * baseSweepingAcc to notify it of the mapping from original SlotAcc to the SlotAcc that should
     * be used for purposes of read access. It is the responsibility of the specified {@link SweepingCountSlotAcc}
     * to ensure proper placement/accessibility of the SlotAcc to be used for read access.
     * 
     * The replacement SlotAcc registered via {@link SweepingCountSlotAcc#registerMapping(SlotAcc, SlotAcc)}
     * will be responsible for output via its {@link SlotAcc#setValues(SimpleOrderedMap, int)} method.
     * An implementer of this method may register such a replacement, and also return a non-null
     * SlotAcc to be used for normal collection (via {@link FacetFieldProcessor#collectAcc}). In this case,
     * the implementer should take care that the returned {@link SlotAcc} is different from the {@link SlotAcc}
     * registered for the purpose of output -- with the former overriding {@link SlotAcc#setValues(SimpleOrderedMap, int)}
     * as a no-op, to prevent setting duplicate values.
     *
     * @param baseSweepingAcc - never null, where the SlotAcc may register domains for sweep collection,
     * and must register mappings of new read-access SlotAccs that result from this call.
     * @return SlotAcc to be used for purpose of collection. If null then collect methods will
     * never be called on this SlotAcc.
     */
    public T registerSweepingAccs(SweepingCountSlotAcc baseSweepingAcc);
  }

  /**
   * A simple data structure to {@link DocSet} domains with an associated {@link CountSlotAcc}. This may be used
   * to support sweep count accumulation over different {@link DocSet} domains, but the concept is perfectly applicable
   * to encapsulating the relevant state for simple "non-sweep" collection as well (in which case {@link SweepCountAccStruct#docSet}
   * would be {@link FacetContext#base}, {@link SweepCountAccStruct#countAcc} would be {@link FacetProcessor#countAcc}, and
   * {@link SweepCountAccStruct#isBase} would trivially be "true"). 
   */
  static final class SweepCountAccStruct {
    final DocSet docSet;
    final boolean isBase;
    final CountSlotAcc countAcc;
    public SweepCountAccStruct(DocSet docSet, boolean isBase, CountSlotAcc countAcc) {
      this.docSet = docSet;
      this.isBase = isBase;
      this.countAcc = countAcc;
    }
    public SweepCountAccStruct(SweepCountAccStruct t, DocSet replaceDocSet) {
      this.docSet = replaceDocSet;
      this.isBase = t.isBase;
      this.countAcc = t.countAcc;
    }
    /**
     * Because sweep collection offloads "collect" methods to count accumulation code,
     * it is helpful to provide a read-only view over the backing {@link CountSlotAcc}
     * 
     * @return - a read-only view over {@link #countAcc}
     */
    public ReadOnlyCountSlotAcc roCountAcc() {
      return countAcc;
    }
    @Override public String toString() {
      return this.countAcc.toString();
    }
  }

  /**
   * Special CountSlotAcc used by processors that support sweeping to decide what to sweep over and how to "collect"
   * when doing the sweep.
   *
   * This class may be used by instances of {@link SweepableSlotAcc} to register DocSet domains (via {@link SweepingCountSlotAcc#add})
   * over which to sweep-collect facet counts.
   *
   * @see SweepableSlotAcc#registerSweepingAccs
   */
  static class SweepingCountSlotAcc extends CountSlotArrAcc {

    static final String SWEEP_COLLECTION_DEBUG_KEY = "sweep_collection";
    private final SimpleOrderedMap<Object> debug;
    private final FacetFieldProcessor p;
    final SweepCountAccStruct base;
    final List<SweepCountAccStruct> others = new ArrayList<>();
    private final List<SlotAcc> output = new ArrayList<>();

    SweepingCountSlotAcc(int numSlots, FacetFieldProcessor p) {
      super(p.fcontext, numSlots);
      this.p = p;
      this.base = new SweepCountAccStruct(fcontext.base, true, this);
      final FacetDebugInfo fdebug = fcontext.getDebugInfo();
      this.debug = null != fdebug ? new SimpleOrderedMap<>() : null;
      if (null != this.debug) {
        fdebug.putInfoItem(SWEEP_COLLECTION_DEBUG_KEY, debug);
        debug.add("base", key);
        debug.add("accs", new ArrayList<String>());
        debug.add("mapped", new ArrayList<String>());
      }
    }

    /**
     * Called by SweepableSlotAccs to register new DocSet domains for sweep collection
     * 
     * @param key
     *          assigned to the returned SlotAcc, and used for debugging
     * @param docs
     *          the domain over which to sweep
     * @param numSlots
     *          the number of slots
     * @return a read-only representation of the count acc which is guaranteed to be populated after sweep count
     *         collection
     */
    public ReadOnlyCountSlotAcc add(String key, DocSet docs, int numSlots) {
      final CountSlotAcc count = new CountSlotArrAcc(fcontext, numSlots);
      count.key = key;
      final SweepCountAccStruct ret = new SweepCountAccStruct(docs, false, count);
      if (null != debug) {
        @SuppressWarnings("unchecked")
        List<String> accsDebug = (List<String>) debug.get("accs");
        accsDebug.add(ret.toString());
      }
      others.add(ret);
      return ret.roCountAcc();
    }

    /**
     * When a {@link SweepableSlotAcc} replaces itself (for the purpose of collection) with a different {@link SlotAcc}
     * instance, it must register that replacement by calling this method with itself as the fromAcc param, and with the
     * new replacement {@link SlotAcc} as the toAcc param. The two SlotAccs must have the same {@link SlotAcc#key}.
     * 
     * It is the responsibility of this method to insure that {@link FacetFieldProcessor} references to fromAcc (other than
     * those within {@link FacetFieldProcessor#collectAcc}, which are set directly by the return value of
     * {@link SweepableSlotAcc#registerSweepingAccs(SweepingCountSlotAcc)}) are replaced
     * by references to toAcc. Such references would include, e.g., {@link FacetFieldProcessor#sortAcc}.
     * 
     * It is also this method's responsibility to insure that read access to toAcc (via toAcc's {@link SlotAcc#setValues(SimpleOrderedMap, int)}
     * method) is provided via this instance's {@link #setValues(SimpleOrderedMap, int)} method.
     * 
     * @param fromAcc - the {@link SlotAcc} to be replaced (this will normally be the caller of this method).
     * @param toAcc - the replacement {@link SlotAcc}
     * 
     * @see SweepableSlotAcc#registerSweepingAccs(SweepingCountSlotAcc)
     */
    public void registerMapping(SlotAcc fromAcc, SlotAcc toAcc) {
      assert fromAcc.key.equals(toAcc.key);
      output.add(toAcc);
      if (p.sortAcc == fromAcc) {
        p.sortAcc = toAcc;
      }
      if (null != debug) {
        @SuppressWarnings("unchecked")
        List<String> mappedDebug = (List<String>) debug.get("mapped");
        mappedDebug.add(fromAcc.toString());
      }
    }

    /**
     * Always populates the bucket with the current count for that slot. If the count is positive, or if
     * <code>processEmpty==true</code>, then this method also populates the values from mapped "output" accumulators.
     *
     * @see #setSweepValues
     */
    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      super.setValues(bucket, slotNum);
      if (0 < getCount(slotNum) || fcontext.processor.freq.processEmpty) {
        setSweepValues(bucket, slotNum);
      }
    }

    /**
     * Populates the bucket with the values from all mapped "output" accumulators for the specified slot.
     *
     * This method exists because there are some contexts (namely SpecialSlotAcc, for allBuckets, etc.) in which "base"
     * count is tracked differently, via getSpecialCount(). For such cases, we need a method that allows the caller to
     * directly coordinate calling {@link SlotAcc#setValues} on the sweeping output accs, while avoiding the inclusion
     * of {@link CountSlotAcc#setValues CountSlotAcc.setValues}
     */
    public void setSweepValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      for (SlotAcc acc : output) {
        acc.setValues(bucket, slotNum);
      }
    }

    /**
     * Helper method for code that wants to operating in a sweeping manner even if the current processor
     * is not using sweeping.
     *
     * @returns struct that wraps the {@link FacetContext#base} unless the {@link FacetProcessor#countAcc} is a {@link SweepingCountSlotAcc}
     */
    public static SweepCountAccStruct baseStructOf(FacetProcessor<?> processor) {
      if (processor.countAcc instanceof SweepingCountSlotAcc) {
        return ((SweepingCountSlotAcc) processor.countAcc).base;
      }
      return new SweepCountAccStruct(processor.fcontext.base, true, processor.countAcc);
    }
    /**
     * Helper method for code that wants to operating in a sweeping manner even if the current processor
     * is not using sweeping
     *
     * @returns empty list unless the {@link FacetProcessor#countAcc} is a {@link SweepingCountSlotAcc}
     */
    public static List<SweepCountAccStruct> otherStructsOf(FacetProcessor<?> processor) {
      if (processor.countAcc instanceof SweepingCountSlotAcc) {
        return ((SweepingCountSlotAcc) processor.countAcc).others;
      }
      return Collections.emptyList();
    }
  }

  abstract static class CountSlotAcc extends SlotAcc implements ReadOnlyCountSlotAcc {
    public CountSlotAcc(FacetContext fcontext) {
      super(fcontext);
      // assume we are the 'count' by default unless/untill our creator overrides this
      this.key = "count";
    }

  public abstract void incrementCount(int slot, int count);

  public abstract int getCount(int slot);
  }

  /**
   * This CountSlotAcc exists as a /dev/null sink for callers of collect(...) and other "write"-type
   * methods. It should be used in contexts where "read"-type access methods will never be called.
   */
  static final CountSlotAcc DEV_NULL_SLOT_ACC = new CountSlotAcc(null) {

    @Override
    public void resize(Resizer resizer) {
      // No-op
    }

    @Override
    public void reset() throws IOException {
      // No-op
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      // No-op
    }

    @Override
    public void incrementCount(int slot, int count) {
      // No-op
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      // No-op
    }

    @Override
    public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      return docs.size(); // dressed up no-op
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public int compare(int slotA, int slotB) {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public int getCount(int slot) {
      throw new UnsupportedOperationException("not supported");
    }
  };

  static class CountSlotArrAcc extends CountSlotAcc {
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

  static class SortSlotAcc extends SlotAcc {
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
}
