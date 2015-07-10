package org.apache.solr.search.facet;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;

class FacetFieldProcessorNumeric extends FacetFieldProcessor {
  static int MAXIMUM_STARTING_TABLE_SIZE=1024;  // must be a power of two, non-final to support setting by tests

  static class LongCounts {

    static final float LOAD_FACTOR = 0.7f;

    long numAdds;
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
    public int numSlots() {
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

      numAdds++;
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



  FacetFieldProcessorNumeric(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  int missingSlot = -1;
  int allBucketsSlot = -1;


  @Override
  public void process() throws IOException {
    super.process();
    response = calcFacets();
  }

  private void doRehash(LongCounts table) {
    if (collectAcc == null && missingAcc == null && allBucketsAcc == null) return;

    // Our "count" acc is backed by the hash table and will already be rehashed
    // otherAccs don't need to be rehashed

    int newTableSize = table.numSlots();
    int numSlots = newTableSize;
    final int oldMissingSlot = missingSlot;
    final int oldAllBucketsSlot = allBucketsSlot;
    if (oldMissingSlot >= 0) {
      missingSlot = numSlots++;
    }
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
        if (oldSlot == oldMissingSlot) {
          return missingSlot;
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
    if (missingAcc != null) {
      missingAcc.resize(resizer);
    }
    if (allBucketsAcc != null) {
      allBucketsAcc.resize(resizer);
    }
  }

  public SimpleOrderedMap<Object> calcFacets() throws IOException {


    final FacetRangeProcessor.Calc calc = FacetRangeProcessor.getNumericCalc(sf);


    // TODO: it would be really nice to know the number of unique values!!!!

    int possibleValues = fcontext.base.size();
    // size smaller tables so that no resize will be necessary
    int currHashSize = BitUtil.nextHighestPowerOfTwo((int) (possibleValues * (1 / LongCounts.LOAD_FACTOR) + 1));
    currHashSize = Math.min(currHashSize, MAXIMUM_STARTING_TABLE_SIZE);
    final LongCounts table = new LongCounts(currHashSize) {
      @Override
      protected void rehash() {
        super.rehash();
        doRehash(this);
        oldToNewMapping = null; // allow for gc
      }
    };

    int numSlots = currHashSize;

    int numMissing = 0;

    if (freq.missing) {
      missingSlot = numSlots++;
    }
    if (freq.allBuckets) {
      allBucketsSlot = numSlots++;
    }

    indexOrderAcc = new SlotAcc(fcontext) {
      @Override
      public void collect(int doc, int slot) throws IOException {
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
      public void collect(int doc, int slot) throws IOException {
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
    createCollectAcc(fcontext.base.size(), numSlots);

    if (freq.allBuckets) {
      allBucketsAcc = new SpecialSlotAcc(fcontext, collectAcc, allBucketsSlot, otherAccs, 0);
    }

    if (freq.missing) {
      // TODO: optimize case when missingSlot can be contiguous with other slots
      missingAcc = new SpecialSlotAcc(fcontext, collectAcc, missingSlot, otherAccs, 1);
    }

    NumericDocValues values = null;
    Bits docsWithField = null;

    // TODO: factor this code out so it can be shared...
    final List<LeafReaderContext> leaves = fcontext.searcher.getIndexReader().leaves();
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = fcontext.base.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        assert doc >= ctx.docBase;
        setNextReaderFirstPhase(ctx);

        values = DocValues.getNumeric(ctx.reader(), sf.getName());
        docsWithField = DocValues.getDocsWithField(ctx.reader(), sf.getName());
      }

      int segDoc = doc - segBase;
      long val = values.get(segDoc);
      if (val == 0 && !docsWithField.get(segDoc)) {
        if (missingAcc != null) {
          missingAcc.collect(segDoc, -1);
        }
      } else {
        int slot = table.add(val);  // this can trigger a rehash rehash

        // countAcc.incrementCount(slot, 1);
        // our countAcc is virtual, so this is not needed

        collectFirstPhase(segDoc, slot);
      }
    }


    //
    // collection done, time to find the top slots
    //

    int numBuckets = 0;
    List<Object> bucketVals = null;
    if (freq.numBuckets && fcontext.isShard()) {
      bucketVals = new ArrayList(100);
    }

    int off = fcontext.isShard() ? 0 : (int) freq.offset;
    // add a modest amount of over-request if this is a shard request
    int lim = freq.limit >= 0 ? (fcontext.isShard() ? (int)(freq.limit*1.1+4) : (int)freq.limit) : Integer.MAX_VALUE;

    int maxsize = (int)(freq.limit > 0 ?  freq.offset + lim : Integer.MAX_VALUE - 1);
    maxsize = Math.min(maxsize, table.cardinality);

    final int sortMul = freq.sortDirection.getMultiplier();

    PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxsize) {
      @Override
      protected boolean lessThan(Slot a, Slot b) {
        // TODO: sort-by-index-order
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? (indexOrderAcc.compare(a.slot, b.slot) > 0) : cmp < 0;
      }
    };

    // TODO: create a countAcc that wrapps the table so we can reuse more code?

    Slot bottom = null;
    for (int i=0; i<table.counts.length; i++) {
      int count = table.counts[i];
      if (count < effectiveMincount) {
        // either not a valid slot, or count not high enough
        continue;
      }
      numBuckets++;  // can be different from the table cardinality if mincount > 1

      long val = table.vals[i];
      if (bucketVals != null && bucketVals.size()<100) {
        bucketVals.add( calc.bitsToValue(val) );
      }

      if (bottom == null) {
        bottom = new Slot();
      }
      bottom.slot = i;

      bottom = queue.insertWithOverflow(bottom);
    }


    SimpleOrderedMap res = new SimpleOrderedMap();
    if (freq.numBuckets) {
      if (!fcontext.isShard()) {
        res.add("numBuckets", numBuckets);
      } else {
        SimpleOrderedMap map = new SimpleOrderedMap(2);
        map.add("numBuckets", numBuckets);
        map.add("vals", bucketVals);
        res.add("numBuckets", map);
      }
    }

    if (freq.allBuckets) {
      SimpleOrderedMap<Object> allBuckets = new SimpleOrderedMap<>();
      // countAcc.setValues(allBuckets, allBucketsSlot);
      allBuckets.add("count", table.numAdds);
      allBucketsAcc.setValues(allBuckets, -1);
      // allBuckets currently doesn't execute sub-facets (because it doesn't change the domain?)
      res.add("allBuckets", allBuckets);
    }

    if (freq.missing) {
      // TODO: it would be more efficient to buid up a missing DocSet if we need it here anyway.

      SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
      fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field));
      res.add("missing", missingBucket);
    }

    // if we are deep paging, we don't have to order the highest "offset" counts.
    int collectCount = Math.max(0, queue.size() - off);
    assert collectCount <= lim;
    int[] sortedSlots = new int[collectCount];
    for (int i = collectCount - 1; i >= 0; i--) {
      sortedSlots[i] = queue.pop().slot;
    }

    ArrayList bucketList = new ArrayList(collectCount);
    res.add("buckets", bucketList);

    boolean needFilter = deferredAggs != null || freq.getSubFacets().size() > 0;

    for (int slotNum : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
      Comparable val = calc.bitsToValue(table.vals[slotNum]);
      bucket.add("val", val);

      Query filter = needFilter ? sf.getType().getFieldQuery(null, sf, calc.formatValue(val)) : null;

      fillBucket(bucket, table.counts[slotNum], slotNum, null, filter);

      bucketList.add(bucket);
    }



    return res;
  }
}
