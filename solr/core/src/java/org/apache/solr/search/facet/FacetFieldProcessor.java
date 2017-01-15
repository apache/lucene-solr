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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;

/**
 * Facet processing based on field values. (not range nor by query)
 * @see FacetField
 */
abstract class FacetFieldProcessor extends FacetProcessor<FacetField> {
  SchemaField sf;
  SlotAcc indexOrderAcc;
  int effectiveMincount;

  Map<String,AggValueSource> deferredAggs;  // null if none

  // TODO: push any of this down to base class?

  //
  // For sort="x desc", collectAcc would point to "x", and sortAcc would also point to "x".
  // collectAcc would be used to accumulate all buckets, and sortAcc would be used to sort those buckets.
  //
  SlotAcc collectAcc;  // Accumulator to collect across entire domain (in addition to the countAcc).  May be null.
  SlotAcc sortAcc;     // Accumulator to use for sorting *only* (i.e. not used for collection). May be an alias of countAcc, collectAcc, or indexOrderAcc
  SlotAcc[] otherAccs; // Accumulators that do not need to be calculated across all buckets.

  SpecialSlotAcc allBucketsAcc;  // this can internally refer to otherAccs and/or collectAcc. setNextReader should be called on otherAccs directly if they exist.

  FacetFieldProcessor(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq);
    this.sf = sf;
    this.effectiveMincount = (int)(fcontext.isShard() ? Math.min(1 , freq.mincount) : freq.mincount);
  }

  /** This is used to create accs for second phase (or to create accs for all aggs) */
  @Override
  protected void createAccs(int docCount, int slotCount) throws IOException {
    if (accMap == null) {
      accMap = new LinkedHashMap<>();
    }

    // allow a custom count acc to be used
    if (countAcc == null) {
      countAcc = new CountSlotArrAcc(fcontext, slotCount);
      countAcc.key = "count";
    }

    if (accs != null) {
      // reuse these accs, but reset them first
      for (SlotAcc acc : accs) {
        acc.reset();
      }
      return;
    } else {
      accs = new SlotAcc[ freq.getFacetStats().size() ];
    }

    int accIdx = 0;
    for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
      SlotAcc acc = null;
      if (slotCount == 1) {
        acc = accMap.get(entry.getKey());
        if (acc != null) {
          acc.reset();
        }
      }
      if (acc == null) {
        acc = entry.getValue().createSlotAcc(fcontext, docCount, slotCount);
        acc.key = entry.getKey();
        accMap.put(acc.key, acc);
      }
      accs[accIdx++] = acc;
    }
  }

  void createCollectAcc(int numDocs, int numSlots) throws IOException {
    accMap = new LinkedHashMap<>();

    // we always count...
    // allow a subclass to set a custom counter.
    if (countAcc == null) {
      countAcc = new CountSlotArrAcc(fcontext, numSlots);
    }

    if ("count".equals(freq.sortVariable)) {
      sortAcc = countAcc;
      deferredAggs = freq.getFacetStats();
    } else if ("index".equals(freq.sortVariable)) {
      // allow subclass to set indexOrderAcc first
      if (indexOrderAcc == null) {
        // This sorting accumulator just goes by the slot number, so does not need to be collected
        // and hence does not need to find it's way into the accMap or accs array.
        indexOrderAcc = new SortSlotAcc(fcontext);
      }
      sortAcc = indexOrderAcc;
      deferredAggs = freq.getFacetStats();
    } else {
      AggValueSource sortAgg = freq.getFacetStats().get(freq.sortVariable);
      if (sortAgg != null) {
        collectAcc = sortAgg.createSlotAcc(fcontext, numDocs, numSlots);
        collectAcc.key = freq.sortVariable; // TODO: improve this
      }
      sortAcc = collectAcc;
      deferredAggs = new HashMap<>(freq.getFacetStats());
      deferredAggs.remove(freq.sortVariable);
    }

    if (deferredAggs.size() == 0) {
      deferredAggs = null;
    }

    boolean needOtherAccs = freq.allBuckets;  // TODO: use for missing too...

    if (!needOtherAccs) {
      // we may need them later, but we don't want to create them now
      // otherwise we won't know if we need to call setNextReader on them.
      return;
    }

    // create the deferred aggs up front for use by allBuckets
    createOtherAccs(numDocs, 1);
  }

  private void createOtherAccs(int numDocs, int numSlots) throws IOException {
    if (otherAccs != null) {
      // reuse existing accumulators
      for (SlotAcc acc : otherAccs) {
        acc.reset();  // todo - make reset take numDocs and numSlots?
      }
      return;
    }

    int numDeferred = deferredAggs == null ? 0 : deferredAggs.size();
    if (numDeferred <= 0) return;

    otherAccs = new SlotAcc[ numDeferred ];

    int otherAccIdx = 0;
    for (Map.Entry<String,AggValueSource> entry : deferredAggs.entrySet()) {
      AggValueSource agg = entry.getValue();
      SlotAcc acc = agg.createSlotAcc(fcontext, numDocs, numSlots);
      acc.key = entry.getKey();
      accMap.put(acc.key, acc);
      otherAccs[otherAccIdx++] = acc;
    }

    if (numDeferred == freq.getFacetStats().size()) {
      // accs and otherAccs are the same...
      accs = otherAccs;
    }
  }

  int collectFirstPhase(DocSet docs, int slot) throws IOException {
    int num = -1;
    if (collectAcc != null) {
      num = collectAcc.collect(docs, slot);
    }
    if (allBucketsAcc != null) {
      num = allBucketsAcc.collect(docs, slot);
    }
    return num >= 0 ? num : docs.size();
  }

  void collectFirstPhase(int segDoc, int slot) throws IOException {
    if (collectAcc != null) {
      collectAcc.collect(segDoc, slot);
    }
    if (allBucketsAcc != null) {
      allBucketsAcc.collect(segDoc, slot);
    }
  }

  /** Processes the collected data to finds the top slots, and composes it in the response NamedList. */
  SimpleOrderedMap<Object> findTopSlots(final int numSlots, final int slotCardinality,
                                        IntFunction<Comparable> bucketValFromSlotNumFunc,
                                        Function<Comparable, String> fieldQueryValFunc) throws IOException {
    int numBuckets = 0;
    List<Object> bucketVals = null;
    if (freq.numBuckets && fcontext.isShard()) {
      bucketVals = new ArrayList<>(100);
    }

    final int off = fcontext.isShard() ? 0 : (int) freq.offset;

    long effectiveLimit = Integer.MAX_VALUE; // use max-int instead of max-long to avoid overflow
    if (freq.limit >= 0) {
      effectiveLimit = freq.limit;
      if (fcontext.isShard()) {
        // add over-request if this is a shard request
        if (freq.overrequest == -1) {
          effectiveLimit = (long) (effectiveLimit*1.1+4); // default: add 10% plus 4 (to overrequest for very small limits)
        } else {
          effectiveLimit += freq.overrequest;
        }
      }
    }


    final int sortMul = freq.sortDirection.getMultiplier();

    int maxTopVals = (int) (effectiveLimit >= 0 ? Math.min(off + effectiveLimit, Integer.MAX_VALUE - 1) : Integer.MAX_VALUE - 1);
    maxTopVals = Math.min(maxTopVals, slotCardinality);
    final SlotAcc sortAcc = this.sortAcc, indexOrderAcc = this.indexOrderAcc;
    final BiPredicate<Slot,Slot> orderPredicate;
    if (indexOrderAcc != null && indexOrderAcc != sortAcc) {
      orderPredicate = (a, b) -> {
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? (indexOrderAcc.compare(a.slot, b.slot) > 0) : cmp < 0;
      };
    } else {
      orderPredicate = (a, b) -> {
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? b.slot < a.slot : cmp < 0;
      };
    }
    final PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxTopVals) {
      @Override
      protected boolean lessThan(Slot a, Slot b) { return orderPredicate.test(a, b); }
    };

    // note: We avoid object allocation by having a Slot and re-using the 'bottom'.
    Slot bottom = null;
    Slot scratchSlot = new Slot();
    for (int slotNum = 0; slotNum < numSlots; slotNum++) {
      // screen out buckets not matching mincount immediately (i.e. don't even increment numBuckets)
      if (effectiveMincount > 0 && countAcc.getCount(slotNum) < effectiveMincount) {
        continue;
      }

      numBuckets++;
      if (bucketVals != null && bucketVals.size()<100) {
        Object val = bucketValFromSlotNumFunc.apply(slotNum);
        bucketVals.add(val);
      }

      if (bottom != null) {
        scratchSlot.slot = slotNum; // scratchSlot is only used to hold this slotNum for the following line
        if (orderPredicate.test(bottom, scratchSlot)) {
          bottom.slot = slotNum;
          bottom = queue.updateTop();
        }
      } else if (effectiveLimit > 0) {
        // queue not full
        Slot s = new Slot();
        s.slot = slotNum;
        queue.add(s);
        if (queue.size() >= maxTopVals) {
          bottom = queue.top();
        }
      }
    }

    assert queue.size() <= numBuckets;

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();
    if (freq.numBuckets) {
      if (!fcontext.isShard()) {
        res.add("numBuckets", numBuckets);
      } else {
        SimpleOrderedMap<Object> map = new SimpleOrderedMap<>(2);
        map.add("numBuckets", numBuckets);
        map.add("vals", bucketVals);
        res.add("numBuckets", map);
      }
    }

    FacetDebugInfo fdebug = fcontext.getDebugInfo();
    if (fdebug != null) fdebug.putInfoItem("numBuckets", (long) numBuckets);

    if (freq.allBuckets) {
      SimpleOrderedMap<Object> allBuckets = new SimpleOrderedMap<>();
      // countAcc.setValues(allBuckets, allBucketsSlot);
      allBuckets.add("count", allBucketsAcc.getSpecialCount());
      allBucketsAcc.setValues(allBuckets, -1); // -1 slotNum is unused for SpecialSlotAcc
      // allBuckets currently doesn't execute sub-facets (because it doesn't change the domain?)
      res.add("allBuckets", allBuckets);
    }

    if (freq.missing) {
      // TODO: it would be more efficient to build up a missing DocSet if we need it here anyway.
      SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
      fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null);
      res.add("missing", missingBucket);
    }

    // if we are deep paging, we don't have to order the highest "offset" counts.
    int collectCount = Math.max(0, queue.size() - off);
    assert collectCount <= effectiveLimit;
    int[] sortedSlots = new int[collectCount];
    for (int i = collectCount - 1; i >= 0; i--) {
      sortedSlots[i] = queue.pop().slot;
    }

    ArrayList<SimpleOrderedMap> bucketList = new ArrayList<>(collectCount);
    res.add("buckets", bucketList);

    boolean needFilter = deferredAggs != null || freq.getSubFacets().size() > 0;

    for (int slotNum : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
      Comparable val = bucketValFromSlotNumFunc.apply(slotNum);
      bucket.add("val", val);

      Query filter = needFilter ? sf.getType().getFieldQuery(null, sf, fieldQueryValFunc.apply(val)) : null;

      fillBucket(bucket, countAcc.getCount(slotNum), slotNum, null, filter);

      bucketList.add(bucket);
    }

    return res;
  }

  private static class Slot {
    int slot;
  }

  private void fillBucket(SimpleOrderedMap<Object> target, int count, int slotNum, DocSet subDomain, Query filter) throws IOException {
    target.add("count", count);
    if (count <= 0 && !freq.processEmpty) return;

    if (collectAcc != null && slotNum >= 0) {
      collectAcc.setValues(target, slotNum);
    }

    createOtherAccs(-1, 1);

    if (otherAccs == null && freq.subFacets.isEmpty()) return;

    if (subDomain == null) {
      subDomain = fcontext.searcher.getDocSet(filter, fcontext.base);
    }

    // if no subFacets, we only need a DocSet
    // otherwise we need more?
    // TODO: save something generic like "slotNum" in the context and use that to implement things like filter exclusion if necessary?
    // Hmmm, but we need to look up some stuff anyway (for the label?)
    // have a method like "DocSet applyConstraint(facet context, DocSet parent)"
    // that's needed for domain changing things like joins anyway???

    if (otherAccs != null) {
      // do acc at a time (traversing domain each time) or do all accs for each doc?
      for (SlotAcc acc : otherAccs) {
        acc.reset(); // TODO: only needed if we previously used for allBuckets or missing
        acc.collect(subDomain, 0);
        acc.setValues(target, 0);
      }
    }

    processSubs(target, filter, subDomain);
  }

  @Override
  protected void processStats(SimpleOrderedMap<Object> bucket, DocSet docs, int docCount) throws IOException {
    if (docCount == 0 && !freq.processEmpty || freq.getFacetStats().size() == 0) {
      bucket.add("count", docCount);
      return;
    }
    createAccs(docCount, 1);
    int collected = collect(docs, 0);

    // countAcc.incrementCount(0, collected);  // should we set the counton the acc instead of just passing it?

    assert collected == docCount;
    addStats(bucket, collected, 0);
  }

  // overrides but with different signature!
  private void addStats(SimpleOrderedMap<Object> target, int count, int slotNum) throws IOException {
    target.add("count", count);
    if (count > 0 || freq.processEmpty) {
      for (SlotAcc acc : accs) {
        acc.setValues(target, slotNum);
      }
    }
  }

  @Override
  void setNextReader(LeafReaderContext ctx) throws IOException {
    // base class calls this (for missing bucket...) ...  go over accs[] in that case
    super.setNextReader(ctx);
  }

  void setNextReaderFirstPhase(LeafReaderContext ctx) throws IOException {
    if (collectAcc != null) {
      collectAcc.setNextReader(ctx);
    }
    if (otherAccs != null) {
      for (SlotAcc acc : otherAccs) {
        acc.setNextReader(ctx);
      }
    }
  }

  static class SpecialSlotAcc extends SlotAcc {
    SlotAcc collectAcc;
    SlotAcc[] otherAccs;
    int collectAccSlot;
    int otherAccsSlot;
    long count;

    SpecialSlotAcc(FacetContext fcontext, SlotAcc collectAcc, int collectAccSlot, SlotAcc[] otherAccs, int otherAccsSlot) {
      super(fcontext);
      this.collectAcc = collectAcc;
      this.collectAccSlot = collectAccSlot;
      this.otherAccs = otherAccs;
      this.otherAccsSlot = otherAccsSlot;
    }

    public int getCollectAccSlot() { return collectAccSlot; }
    public int getOtherAccSlot() { return otherAccsSlot; }

    long getSpecialCount() {
      return count;
    }

    @Override
    public void collect(int doc, int slot) throws IOException {
      assert slot != collectAccSlot || slot < 0;
      count++;
      if (collectAcc != null) {
        collectAcc.collect(doc, collectAccSlot);
      }
      if (otherAccs != null) {
        for (SlotAcc otherAcc : otherAccs) {
          otherAcc.collect(doc, otherAccsSlot);
        }
      }
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      // collectAcc and otherAccs will normally have setNextReader called directly on them.
      // This, however, will be used when collect(DocSet,slot) variant is used on this Acc.
      if (collectAcc != null) {
        collectAcc.setNextReader(readerContext);
      }
      if (otherAccs != null) {
        for (SlotAcc otherAcc : otherAccs) {
          otherAcc.setNextReader(readerContext);
        }
      }
    }

    @Override
    public int compare(int slotA, int slotB) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      if (collectAcc != null) {
        collectAcc.setValues(bucket, collectAccSlot);
      }
      if (otherAccs != null) {
        for (SlotAcc otherAcc : otherAccs) {
          otherAcc.setValues(bucket, otherAccsSlot);
        }
      }
    }

    @Override
    public void reset() {
      // reset should be called on underlying accs
      // TODO: but in case something does need to be done here, should we require this method to be called but do nothing for now?
      throw new UnsupportedOperationException();
    }

    @Override
    public void resize(Resizer resizer) {
      // someone else will call resize on collectAcc directly
      if (collectAccSlot >= 0) {
        collectAccSlot = resizer.getNewSlot(collectAccSlot);
      }
    }
  }
}
