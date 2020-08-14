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
import java.text.ParseException;
import java.util.function.IntFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSetUtil;
import org.apache.solr.search.facet.SlotAcc.SlotContext;

/**
 * Facets numbers into a hash table.  The number is either a raw numeric DocValues value, or
 * a term global ordinal integer.
 * Limitations:
 * <ul>
 *   <li>doesn't handle prefix, but could easily be added</li>
 *   <li>doesn't handle mincount==0 -- you're better off with an array alg</li>
 * </ul>
 */
class FacetFieldProcessorByHashDV extends FacetFieldProcessor {
  static int MAXIMUM_STARTING_TABLE_SIZE=1024;  // must be a power of two, non-final to support setting by tests

  /** a hash table with long keys (what we're counting) and integer values (counts) */
  private static class LongCounts {

    static final float LOAD_FACTOR = 0.7f;

    long[] vals;
    int[] counts;  // maintain the counts here since we need them to tell if there was actually a value anyway
    int[] oldToNewMapping;

    int cardinality;
    int threshold;

    /** sz must be a power of two */
    LongCounts(int sz) {
      vals = new long[sz];
      counts = new int[sz];
      threshold = (int) (sz * LOAD_FACTOR);
    }

    /** Current number of slots in the hash table */
    int numSlots() {
      return vals.length;
    }

    private int hash(long val) {
      // For floats: exponent bits start at bit 23 for single precision,
      // and bit 52 for double precision.
      // Many values will only have significant bits just to the right of that,
      // and the leftmost bits will all be zero.

      // For now, lets just settle to get first 8 significant mantissa bits of double or float in the lowest bits of our hash
      // The upper bits of our hash will be irrelevant.
      int h = (int) (val + (val >>> 44) + (val >>> 15));
      return h;
    }

    /** returns the slot */
    int add(long val) {
      if (cardinality >= threshold) {
        rehash();
      }

      int h = hash(val);
      for (int slot = h & (vals.length-1);  ;slot = (slot + ((h>>7)|1)) & (vals.length-1)) {
        int count = counts[slot];
        if (count == 0) {
          counts[slot] = 1;
          vals[slot] = val;
          cardinality++;
          return slot;
        } else if (vals[slot] == val) {
          // val is already in the set
          counts[slot] = count + 1;
          return slot;
        }
      }
    }

    protected void rehash() {
      long[] oldVals = vals;
      int[] oldCounts = counts;  // after retrieving the count, this array is reused as a mapping to new array
      int newCapacity = vals.length << 1;
      vals = new long[newCapacity];
      counts = new int[newCapacity];
      threshold = (int) (newCapacity * LOAD_FACTOR);

      for (int i=0; i<oldVals.length; i++) {
        int count = oldCounts[i];
        if (count == 0) {
          oldCounts[i] = -1;
          continue;
        }

        long val = oldVals[i];

        int h = hash(val);
        int slot = h & (vals.length-1);
        while (counts[slot] != 0) {
          slot = (slot + ((h>>7)|1)) & (vals.length-1);
        }
        counts[slot] = count;
        vals[slot] = val;
        oldCounts[i] = slot;
      }

      oldToNewMapping = oldCounts;
    }

    int cardinality() {
      return cardinality;
    }

  }

  /** A hack instance of Calc for Term ordinals in DocValues. */
  // TODO consider making FacetRangeProcessor.Calc facet top level; then less of a hack?
  private class TermOrdCalc extends FacetRangeProcessor.Calc {

    IntFunction<BytesRef> lookupOrdFunction; // set in collectDocs()!

    TermOrdCalc() throws IOException {
      super(sf);
    }

    @Override
    public long bitsToSortableBits(long globalOrd) {
      return globalOrd;
    }

    /** To be returned in "buckets"/"val" */
    @Override
    public Comparable bitsToValue(long globalOrd) {
      BytesRef bytesRef = lookupOrdFunction.apply((int) globalOrd);
      // note FacetFieldProcessorByArray.findTopSlots also calls SchemaFieldType.toObject
      return sf.getType().toObject(sf, bytesRef).toString();
    }

    @Override
    public String formatValue(Comparable val) {
      return (String) val;
    }

    @Override
    protected Comparable parseStr(String rawval) throws ParseException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Comparable parseAndAddGap(Comparable value, String gap) throws ParseException {
      throw new UnsupportedOperationException();
    }

  }

  FacetRangeProcessor.Calc calc;
  LongCounts table;
  int allBucketsSlot = -1;

  FacetFieldProcessorByHashDV(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
    if (freq.mincount == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          getClass()+" doesn't support mincount=0");
    }
    if (freq.prefix != null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          getClass()+" doesn't support prefix"); // yet, but it could
    }
    FieldInfo fieldInfo = fcontext.searcher.getFieldInfos().fieldInfo(sf.getName());
    if (fieldInfo != null &&
        fieldInfo.getDocValuesType() != DocValuesType.NUMERIC &&
        fieldInfo.getDocValuesType() != DocValuesType.SORTED &&
        fieldInfo.getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          getClass()+" only support single valued number/string with docValues");
    }
  }

  @Override
  public void process() throws IOException {
    super.process();
    response = calcFacets();
    table = null;//gc
  }

  private SimpleOrderedMap<Object> calcFacets() throws IOException {

    if (sf.getType().getNumberType() != null) {
      calc = FacetRangeProcessor.getNumericCalc(sf);
    } else {
      calc = new TermOrdCalc(); // kind of a hack
    }

    // TODO: Use the number of indexed terms, if present, as an estimate!
    //    Even for NumericDocValues, we could check for a terms index for an estimate.
    //    Our estimation should aim high to avoid expensive rehashes.

    int possibleValues = fcontext.base.size();
    // size smaller tables so that no resize will be necessary
    int currHashSize = BitUtil.nextHighestPowerOfTwo((int) (possibleValues * (1 / LongCounts.LOAD_FACTOR) + 1));
    currHashSize = Math.min(currHashSize, MAXIMUM_STARTING_TABLE_SIZE);
    table = new LongCounts(currHashSize) {
      @Override
      protected void rehash() {
        super.rehash();
        doRehash(this);
        oldToNewMapping = null; // allow for gc
      }
    };

    // note: these methods/phases align with FacetFieldProcessorByArray's

    createCollectAcc();

    collectDocs();

    return super.findTopSlots(table.numSlots(), table.cardinality(),
        slotNum -> calc.bitsToValue(table.vals[slotNum]), // getBucketValFromSlotNum
        val -> calc.formatValue(val)); // getFieldQueryVal
  }

  private void createCollectAcc() throws IOException {
    int numSlots = table.numSlots();

    if (freq.allBuckets) {
      allBucketsSlot = numSlots++;
    }

    indexOrderAcc = new SlotAcc(fcontext) {
      @Override
      public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      }

      @Override
      public int compare(int slotA, int slotB) {
        long s1 = calc.bitsToSortableBits(table.vals[slotA]);
        long s2 = calc.bitsToSortableBits(table.vals[slotB]);
        return Long.compare(s1, s2);
      }

      @Override
      public Object getValue(int slotNum) throws IOException {
        return null;
      }

      @Override
      public void reset() {
      }

      @Override
      public void resize(Resizer resizer) {
      }
    };

    countAcc = new CountSlotAcc(fcontext) {
      @Override
      public void incrementCount(int slot, int count) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getCount(int slot) {
        return table.counts[slot];
      }

      @Override
      public Object getValue(int slotNum) {
        return getCount(slotNum);
      }

      @Override
      public void reset() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int compare(int slotA, int slotB) {
        return Integer.compare( table.counts[slotA], table.counts[slotB] );
      }

      @Override
      public void resize(Resizer resizer) {
        throw new UnsupportedOperationException();
      }
    };

    // we set the countAcc & indexAcc first so generic ones won't be created for us.
    super.createCollectAcc(fcontext.base.size(), numSlots);

    if (freq.allBuckets) {
      allBucketsAcc = new SpecialSlotAcc(fcontext, collectAcc, allBucketsSlot, otherAccs, 0);
    }
  }

  private void collectDocs() throws IOException {
    if (calc instanceof TermOrdCalc) { // Strings

      // TODO support SortedSetDocValues
      SortedDocValues globalDocValues = FieldUtil.getSortedDocValues(fcontext.qcontext, sf, null);
      ((TermOrdCalc)calc).lookupOrdFunction = ord -> {
        try {
          return globalDocValues.lookupOrd(ord);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };

      DocSetUtil.collectSortedDocSet(fcontext.base, fcontext.searcher.getIndexReader(), new SimpleCollector() {
          SortedDocValues docValues = globalDocValues; // this segment/leaf. NN
          LongValues toGlobal = LongValues.IDENTITY; // this segment to global ordinal. NN

          @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE_NO_SCORES; }

          @Override
          protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
            setNextReaderFirstPhase(ctx);
            if (globalDocValues instanceof MultiDocValues.MultiSortedDocValues) {
              MultiDocValues.MultiSortedDocValues multiDocValues = (MultiDocValues.MultiSortedDocValues) globalDocValues;
              docValues = multiDocValues.values[ctx.ord];
              toGlobal = multiDocValues.mapping.getGlobalOrds(ctx.ord);
            }
          }

          @Override
          public void collect(int segDoc) throws IOException {
            if (docValues.advanceExact(segDoc)) {
              long val = toGlobal.get(docValues.ordValue());
              collectValFirstPhase(segDoc, val);
            }
          }
        });

    } else { // Numeric:

      if (sf.multiValued()) {
        DocSetUtil.collectSortedDocSet(fcontext.base, fcontext.searcher.getIndexReader(), new SimpleCollector() {
          SortedNumericDocValues values = null; //NN

          @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE_NO_SCORES; }

          @Override
          protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
            setNextReaderFirstPhase(ctx);
            values = DocValues.getSortedNumeric(ctx.reader(), sf.getName());
          }

          @Override
          public void collect(int segDoc) throws IOException {
            if (values.advanceExact(segDoc)) {
              long l = values.nextValue(); // This document must have at least one value
              collectValFirstPhase(segDoc, l);
              for (int i = 1, count = values.docValueCount(); i < count; i++) {
                long lnew = values.nextValue();
                if (lnew != l) { // Skip the value if it's equal to the last one, we don't want to double-count it
                  collectValFirstPhase(segDoc, lnew);
                }
                l = lnew;
              }

            }
          }
        });
      } else {
        DocSetUtil.collectSortedDocSet(fcontext.base, fcontext.searcher.getIndexReader(), new SimpleCollector() {
          NumericDocValues values = null; //NN

          @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE_NO_SCORES; }

          @Override
          protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
            setNextReaderFirstPhase(ctx);
            values = DocValues.getNumeric(ctx.reader(), sf.getName());
          }

          @Override
          public void collect(int segDoc) throws IOException {
            if (values.advanceExact(segDoc)) {
              collectValFirstPhase(segDoc, values.longValue());
            }
          }
        });
      }
    }
  }

  private void collectValFirstPhase(int segDoc, long val) throws IOException {
    int slot = table.add(val); // this can trigger a rehash

    // Our countAcc is virtual, so this is not needed:
    // countAcc.incrementCount(slot, 1);

    super.collectFirstPhase(segDoc, slot, slotContext);
  }

  /**
   * SlotContext to use during all {@link SlotAcc} collection.
   *
   * This avoids a memory allocation for each invocation of collectValFirstPhase.
   */
  private IntFunction<SlotContext> slotContext = (slotNum) -> {
    long val = table.vals[slotNum];
    Comparable value = calc.bitsToValue(val);
    return new SlotContext(sf.getType().getFieldQuery(null, sf, calc.formatValue(value)));
  };

  private void doRehash(LongCounts table) {
    if (collectAcc == null && allBucketsAcc == null) return;

    // Our "count" acc is backed by the hash table and will already be rehashed
    // otherAccs don't need to be rehashed

    int newTableSize = table.numSlots();
    int numSlots = newTableSize;
    final int oldAllBucketsSlot = allBucketsSlot;
    if (oldAllBucketsSlot >= 0) {
      allBucketsSlot = numSlots++;
    }

    final int finalNumSlots = numSlots;
    final int[] mapping = table.oldToNewMapping;

    SlotAcc.Resizer resizer = new SlotAcc.Resizer() {
      @Override
      public int getNewSize() {
        return finalNumSlots;
      }

      @Override
      public int getNewSlot(int oldSlot) {
        if (oldSlot < mapping.length) {
          return mapping[oldSlot];
        }
        if (oldSlot == oldAllBucketsSlot) {
          return allBucketsSlot;
        }
        return -1;
      }
    };

    // NOTE: resizing isn't strictly necessary for missing/allBuckets... we could just set the new slot directly
    if (collectAcc != null) {
      collectAcc.resize(resizer);
    }
    if (allBucketsAcc != null) {
      allBucketsAcc.resize(resizer);
    }
  }
}
