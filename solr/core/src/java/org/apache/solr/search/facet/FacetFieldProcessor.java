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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.facet.SlotAcc.SlotContext;
import org.apache.solr.search.facet.SlotAcc.SweepableSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SweepingCountSlotAcc;

import static org.apache.solr.search.facet.FacetContext.SKIP_FACET;

/**
 * Facet processing based on field values. (not range nor by query)
 * @see FacetField
 */
abstract class FacetFieldProcessor extends FacetProcessor<FacetField> {
  SchemaField sf;
  SlotAcc indexOrderAcc;
  int effectiveMincount;
  final boolean singlePassSlotAccCollection;
  final FacetRequest.FacetSort sort; // never null (may be the user's requested sort, or the prelim_sort)
  final FacetRequest.FacetSort resort; // typically null (unless the user specified a prelim_sort)
  
  final Map<String,AggValueSource> deferredAggs = new HashMap<String,AggValueSource>();

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
    this.singlePassSlotAccCollection = (freq.limit == -1 && freq.subFacets.size() == 0);

    if ( null == freq.prelim_sort ) {
      // If the user has not specified any preliminary sort, then things are very simple.
      // Just use the "sort" as is w/o needing any re-sorting
      this.sort = freq.sort;
      this.resort = null;
    } else {
      assert null != freq.prelim_sort;
      
      if ( fcontext.isShard() ) {
        // for a shard request, we can ignore the users requested "sort" and focus solely on the prelim_sort
        // the merger will worry about the final sorting -- we don't need to resort anything...
        this.sort = freq.prelim_sort;
        this.resort = null;
        
      } else { // non shard...
        if ( singlePassSlotAccCollection ) { // special case situation...
          // when we can do a single pass SlotAcc collection on non-shard request, there is
          // no point re-sorting. Ignore the freq.prelim_sort and use the freq.sort option as is...
          this.sort = freq.sort;
          this.resort = null;
        } else {
          // for a non-shard request, we will use the prelim_sort as our initial sort option if it exists
          // then later we will re-sort on the final desired sort...
          this.sort = freq.prelim_sort;
          this.resort = freq.sort;
        }
      }
    }
    assert null != this.sort;
  }

  /** This is used to create accs for second phase (or to create accs for all aggs) */
  @Override
  protected void createAccs(int docCount, int slotCount) throws IOException {
    if (accMap == null) {
      accMap = new LinkedHashMap<>();
    }

    // allow a custom count acc to be used
    if (countAcc == null) {
      countAcc = new SlotAcc.CountSlotArrAcc(fcontext, slotCount);
    }

    if (accs != null) {
      // reuse these accs, but reset them first and resize since size could be different
      for (SlotAcc acc : accs) {
        acc.reset();
        acc.resize(new FlatteningResizer(slotCount));
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

  /** 
   * Simple helper for checking if a {@link FacetRequest.FacetSort} is on "count" or "index" and picking
   * the existing SlotAcc 
   * @return an existing SlotAcc for sorting, else null if it should be built from the Aggs
   */
  private SlotAcc getTrivialSortingSlotAcc(FacetRequest.FacetSort fsort) {
    if ("count".equals(fsort.sortVariable)) {
      assert null != countAcc;
      return countAcc;
    } else if ("index".equals(fsort.sortVariable)) {
      // allow subclass to set indexOrderAcc first
      if (indexOrderAcc == null) {
        // This sorting accumulator just goes by the slot number, so does not need to be collected
        // and hence does not need to find it's way into the accMap or accs array.
        indexOrderAcc = new SlotAcc.SortSlotAcc(fcontext);
      }
      return indexOrderAcc;
    }
    return null;
  }
  
  void createCollectAcc(int numDocs, int numSlots) throws IOException {
    accMap = new LinkedHashMap<>();
    
    // start with the assumption that we're going to defer the computation of all stats
    deferredAggs.putAll(freq.getFacetStats());
 
    // we always count...
    // allow a subclass to set a custom counter.
    if (countAcc == null) {
      countAcc = new SlotAcc.CountSlotArrAcc(fcontext, numSlots);
    }

    sortAcc = getTrivialSortingSlotAcc(this.sort);

    if (this.singlePassSlotAccCollection) {
      // If we are going to return all buckets, and if there are no subfacets (that would need a domain),
      // then don't defer any aggregation calculations to a second phase.
      // This way we can avoid calculating domains for each bucket, which can be expensive.

      // TODO: BEGIN: why can't we just call createAccs here ?
      accs = new SlotAcc[ freq.getFacetStats().size() ];
      int otherAccIdx = 0;
      for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
        AggValueSource agg = entry.getValue();
        SlotAcc acc = agg.createSlotAcc(fcontext, numDocs, numSlots);
        acc.key = entry.getKey();
        accMap.put(acc.key, acc);
        accs[otherAccIdx++] = acc;
      }
      // TODO: END: why can't we just call createAccs here ?
      if (accs.length == 1) {
        collectAcc = accs[0];
      } else {
        collectAcc = new MultiAcc(fcontext, accs);
      }

      if (sortAcc == null) {
        sortAcc = accMap.get(sort.sortVariable);
        assert sortAcc != null;
      }

      deferredAggs.clear();
    }

    if (sortAcc == null) {
      AggValueSource sortAgg = freq.getFacetStats().get(sort.sortVariable);
      if (sortAgg != null) {
        collectAcc = sortAgg.createSlotAcc(fcontext, numDocs, numSlots);
        collectAcc.key = sort.sortVariable; // TODO: improve this
      }
      sortAcc = collectAcc;
      deferredAggs.remove(sort.sortVariable);
    }

    boolean needOtherAccs = freq.allBuckets;  // TODO: use for missing too...

    if (sortAcc == null) {
      // as sort is already validated, in what case sortAcc would be null?
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Invalid sort '" + sort + "' for field '" + sf.getName() + "'");
    }

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

    final int numDeferred = deferredAggs.size();
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

  int collectFirstPhase(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    int num = -1;
    if (collectAcc != null) {
      num = collectAcc.collect(docs, slot, slotContext);
    }
    if (allBucketsAcc != null) {
      num = allBucketsAcc.collect(docs, slot, slotContext);
    }
    return num >= 0 ? num : docs.size();
  }

  void collectFirstPhase(int segDoc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    if (collectAcc != null) {
      collectAcc.collect(segDoc, slot, slotContext);
    }
    if (allBucketsAcc != null) {
      allBucketsAcc.collect(segDoc, slot, slotContext);
    }
  }

  /** Processes the collected data to finds the top slots, and composes it in the response NamedList. */
  SimpleOrderedMap<Object> findTopSlots(final int numSlots, final int slotCardinality,
                                        @SuppressWarnings("rawtypes") IntFunction<Comparable> bucketValFromSlotNumFunc,
                                        @SuppressWarnings("rawtypes") Function<Comparable, String> fieldQueryValFunc) throws IOException {
    assert this.sortAcc != null;
    int numBuckets = 0;

    final int off = fcontext.isShard() ? 0 : (int) freq.offset;

    long effectiveLimit = Integer.MAX_VALUE; // use max-int instead of max-long to avoid overflow
    if (freq.limit >= 0) {
      effectiveLimit = freq.limit;
      if (fcontext.isShard()) {
        if (freq.overrequest == -1) {
          // add over-request if this is a shard request and if we have a small offset (large offsets will already be gathering many more buckets than needed)
          if (freq.offset < 10) {
            effectiveLimit = (long) (effectiveLimit * 1.1 + 4); // default: add 10% plus 4 (to overrequest for very small limits)
          }
        } else {
          effectiveLimit += freq.overrequest;
        }
      } else if (null != resort && 0 < freq.overrequest) {
        // in non-shard situations, if we have a 'resort' we check for explicit overrequest > 0
        effectiveLimit += freq.overrequest;
      }
    }

    final int sortMul = sort.sortDirection.getMultiplier();

    int maxTopVals = (int) (effectiveLimit >= 0 ? Math.min(freq.offset + effectiveLimit, Integer.MAX_VALUE - 1) : Integer.MAX_VALUE - 1);
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
    boolean shardHasMoreBuckets = false;  // This shard has more buckets than were returned
    for (int slotNum = 0; slotNum < numSlots; slotNum++) {

      // screen out buckets not matching mincount
      if (effectiveMincount > 0) {
        int count = countAcc.getCount(slotNum);
        if (count  < effectiveMincount) {
          if (count > 0)
            numBuckets++;  // Still increment numBuckets as long as we have some count.  This is for consistency between distrib and non-distrib mode.
          continue;
        }
      }

      numBuckets++;

      if (bottom != null) {
        shardHasMoreBuckets = true;
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
        calculateNumBuckets(res);
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

    SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
    if (freq.missing) {
      res.add("missing", missingBucket);
      // moved missing fillBucket after we fill facet since it will reset all the accumulators.
    }

    final boolean needFilter = (!deferredAggs.isEmpty()) || freq.getSubFacets().size() > 0;
    if (needFilter) {
      createOtherAccs(-1, 1);
    }

    // if we are deep paging, we don't have to order the highest "offset" counts...
    // ...unless we need to resort.
    int collectCount = Math.max(0, queue.size() - (null == this.resort ? off : 0));
    //
    assert collectCount <= maxTopVals;
    Slot[] sortedSlots = new Slot[collectCount];
    for (int i = collectCount - 1; i >= 0; i--) {
      Slot slot = sortedSlots[i] = queue.pop();
      // At this point we know we're either returning this Slot as a Bucket, or resorting it,
      // so definitely fill in the bucket value -- we'll need it either way
      slot.bucketVal = bucketValFromSlotNumFunc.apply(slot.slot);
      
      if (needFilter || null != this.resort) {
        slot.bucketFilter = makeBucketQuery(fieldQueryValFunc.apply(slot.bucketVal));
      }
    }
    
    final SlotAcc resortAccForFill = resortSlots(sortedSlots); // No-Op if not needed
    
    if (null != this.resort) {
      // now that we've completely resorted, throw away extra docs from possible offset/overrequest...
      final int endOffset = (int)Math.min((long) sortedSlots.length,
                                          // NOTE: freq.limit is long, so no risk of overflow here
                                          off + (freq.limit < 0 ? Integer.MAX_VALUE : freq.limit));
      if (0 < off || endOffset < sortedSlots.length) {
        sortedSlots = Arrays.copyOfRange(sortedSlots, off, endOffset);
      }
    }
    @SuppressWarnings({"rawtypes"})
    List<SimpleOrderedMap> bucketList = new ArrayList<>(sortedSlots.length);

    for (Slot slot : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
      bucket.add("val", slot.bucketVal);

      fillBucketFromSlot(bucket, slot, resortAccForFill);

      bucketList.add(bucket);
    }

    res.add("buckets", bucketList);
      
    
    if (fcontext.isShard() && shardHasMoreBuckets) {
      // Currently, "more" is an internal implementation detail and only returned for distributed sub-requests
      res.add("more", true);
    }

    if (freq.missing) {
      // TODO: it would be more efficient to build up a missing DocSet if we need it here anyway.
      fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null, false, null);
    }

    return res;
  }

  /**
   * Trivial helper method for building up a bucket query given the (Stringified) bucket value
   */
  protected Query makeBucketQuery(final String bucketValue) {
    return sf.getType().getFieldTermQuery(null, sf, bucketValue);
  }

  private void calculateNumBuckets(SimpleOrderedMap<Object> target) throws IOException {
    DocSet domain = fcontext.base;
    if (freq.prefix != null) {
      Query prefixFilter = sf.getType().getPrefixQuery(null, sf, freq.prefix);
      domain = fcontext.searcher.getDocSet(prefixFilter, domain);
    }

    HLLAgg agg = new HLLAgg(freq.field);
    SlotAcc acc = agg.createSlotAcc(fcontext, domain.size(), 1);
    acc.collect(domain, 0, null); // we know HLL doesn't care about the bucket query
    acc.key = "numBuckets";
    acc.setValues(target, 0);
  }

  private static class Slot {
    /** The Slot number used during collection */
    int slot;

    /** filled in only once we know the bucket will either be involved in resorting, or returned */
    @SuppressWarnings({"rawtypes"})
    Comparable bucketVal;

    /** Filled in if and only if needed for resorting, deferred stats, or subfacets */
    Query bucketFilter;
    // TODO: we could potentially store the bucket's (DocSet)subDomain as well,
    // but that's much bigger object to hang onto for every slot at the sametime
    // Probably best to just trust the filterCache to do it's job
    
    /** The Slot number used during resorting */
    int resortSlotNum;
  }

  /** Helper method used solely when looping over buckets to be returned in findTopSlots */
  private void fillBucketFromSlot(SimpleOrderedMap<Object> target, Slot slot,
                                  SlotAcc resortAcc) throws IOException {
    final int slotOrd = slot.slot;
    countAcc.setValues(target, slotOrd);
    if (countAcc.getCount(slotOrd) <= 0 && !freq.processEmpty) return;

    if (slotOrd >= 0 && collectAcc != null) {
      collectAcc.setValues(target, slotOrd);
    }

    if (otherAccs == null && freq.subFacets.isEmpty()) return;

    assert null != slot.bucketFilter;
    final Query filter = slot.bucketFilter;
    final DocSet subDomain = fcontext.searcher.getDocSet(filter, fcontext.base);

    // if no subFacets, we only need a DocSet
    // otherwise we need more?
    // TODO: save something generic like "slotNum" in the context and use that to implement things like filter exclusion if necessary?
    // Hmmm, but we need to look up some stuff anyway (for the label?)
    // have a method like "DocSet applyConstraint(facet context, DocSet parent)"
    // that's needed for domain changing things like joins anyway???

    if (otherAccs != null) {
      // do acc at a time (traversing domain each time) or do all accs for each doc?
      for (SlotAcc acc : otherAccs) {
        if (acc == resortAcc) {
          // already collected, just need to get the value from the correct slot
          acc.setValues(target, slot.resortSlotNum);
        } else {
          acc.reset(); // TODO: only needed if we previously used for allBuckets or missing
          acc.collect(subDomain, 0, s -> { return new SlotContext(filter); });
          acc.setValues(target, 0);
        }
      }
    }

    processSubs(target, filter, subDomain, false, null);
  }

  /** 
   * Helper method that resorts the slots (if needed).
   * 
   * @return a SlotAcc that should be used {@link SlotAcc#setValues} on the final buckets via 
   *    {@link Slot#resortSlotNum} or null if no special SlotAcc was needed (ie: no resorting, or resorting 
   *    on something already known/collected)
   */
  private SlotAcc resortSlots(Slot[] slots) throws IOException {
    if (null == this.resort) {
      return null; // Nothing to do.
    }
    assert ! fcontext.isShard();

    // NOTE: getMultiplier() is confusing and weird and ment for use in PriorityQueue.lessThan,
    // so it's backwards from what you'd expect in a Comparator...
    final int resortMul = -1 * resort.sortDirection.getMultiplier();
    
    SlotAcc resortAcc = getTrivialSortingSlotAcc(this.resort);
    if (null != resortAcc) {
      // resorting on count or index is rare (and not particularly useful) but if someone chooses to do
      // either of these we don't need to re-collect ... instead just re-sort the slots based on
      // the previously collected values using the originally collected slot numbers...
      if (resortAcc.equals(countAcc)) {
        final Comparator<Slot> comparator = null != indexOrderAcc ?
          (new Comparator<Slot>() {
            public int compare(Slot x, Slot y) {
              final int cmp = resortMul * countAcc.compare(x.slot, y.slot);
              return  cmp != 0 ? cmp : indexOrderAcc.compare(x.slot, y.slot);
            }
          })
          : (new Comparator<Slot>() {
            public int compare(Slot x, Slot y) {
              final int cmp = resortMul * countAcc.compare(x.slot, y.slot);
              return  cmp != 0 ? cmp : Integer.compare(x.slot, y.slot);
            }
          });
        Arrays.sort(slots, comparator);
        return null;
      }
      if (resortAcc.equals(indexOrderAcc)) {
        // obviously indexOrderAcc is not null, and no need for a fancy tie breaker...
        Arrays.sort(slots, new Comparator<Slot>() {
          public int compare(Slot x, Slot y) {
            return resortMul * indexOrderAcc.compare(x.slot, y.slot);
          }
        });
        return null;
      }
      // nothing else should be possible
      assert false : "trivial resort isn't count or index: " + this.resort;
    }

    assert null == resortAcc;
    for (SlotAcc acc : otherAccs) {
      if (acc.key.equals(this.resort.sortVariable)) {
        resortAcc = acc;
        break;
      }
    }
    // TODO: what if resortAcc is still null, ie: bad input? ... throw an error?  (see SOLR-13022)
    // looks like equivilent sort code path silently ignores sorting if sortVariable isn't in accMap...
    // ...and we get a deffered NPE when trying to collect.
    assert null != resortAcc;
    
    final SlotAcc acc = resortAcc;
    
    // reset resortAcc to be (just) big enough for all the slots we care about...
    acc.reset();
    acc.resize(new FlatteningResizer(slots.length));
    
    // give each existing Slot a new resortSlotNum and let the resortAcc collect it...
    for (int slotNum = 0; slotNum < slots.length; slotNum++) {
      Slot slot = slots[slotNum];
      slot.resortSlotNum = slotNum;
      
      assert null != slot.bucketFilter : "null filter for slot=" +slot.bucketVal;
      
      final DocSet subDomain = fcontext.searcher.getDocSet(slot.bucketFilter, fcontext.base);
      acc.collect(subDomain, slotNum, s -> { return new SlotContext(slot.bucketFilter); } );
    }
    
    // now resort all the Slots according to the new collected values...
    final Comparator<Slot> comparator = null != indexOrderAcc ?
      (new Comparator<Slot>() {
        public int compare(Slot x, Slot y) {
          final int cmp = resortMul * acc.compare(x.resortSlotNum, y.resortSlotNum);
          return  cmp != 0 ? cmp : indexOrderAcc.compare(x.slot, y.slot);
        }
      })
      : (new Comparator<Slot>() {
        public int compare(Slot x, Slot y) {
          final int cmp = resortMul * acc.compare(x.resortSlotNum, y.resortSlotNum);
          return  cmp != 0 ? cmp : Integer.compare(x.slot, y.slot);
        }
      });
    Arrays.sort(slots, comparator);
    return acc;
  }
  
  @Override
  protected void processStats(SimpleOrderedMap<Object> bucket, Query bucketQ, DocSet docs, int docCount) throws IOException {
    if (docCount == 0 && !freq.processEmpty || freq.getFacetStats().size() == 0) {
      bucket.add("count", docCount);
      return;
    }
    createAccs(docCount, 1);
    assert null != bucketQ;
    int collected = collect(docs, 0, slotNum -> { return new SlotContext(bucketQ); });

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

  static class MultiAcc extends SlotAcc implements SweepableSlotAcc<SlotAcc> {
    final SlotAcc[] subAccs;

    MultiAcc(FacetContext fcontext, SlotAcc[] subAccs) {
      super(fcontext);
      this.subAccs = subAccs;
    }

    @Override
    public void setNextReader(LeafReaderContext ctx) throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.setNextReader(ctx);
      }
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.collect(doc, slot, slotContext);
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
    public void reset() throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.reset();
      }
    }

    @Override
    public void resize(Resizer resizer) {
      for (SlotAcc acc : subAccs) {
        acc.resize(resizer);
      }
    }

    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      for (SlotAcc acc : subAccs) {
        acc.setValues(bucket, slotNum);
      }
    }

    @Override
    public SlotAcc registerSweepingAccs(SweepingCountSlotAcc baseSweepingAcc) {
      final FacetFieldProcessor p = (FacetFieldProcessor) fcontext.processor;
      int j = 0;
      for (int i = 0; i < subAccs.length; i++) {
        final SlotAcc acc = subAccs[i];
        if (acc instanceof SweepableSlotAcc) {
          SlotAcc replacement = ((SweepableSlotAcc<?>)acc).registerSweepingAccs(baseSweepingAcc);
          if (replacement == null) {
            // drop acc, do not increment j
            continue;
          } else if (replacement != acc || j < i) {
            subAccs[j] = replacement;
          }
        } else if (j < i) {
          subAccs[j] = acc;
        }
        j++;
      }
      switch (j) {
        case 0:
          return null;
        case 1:
          return subAccs[0];
        default:
          if (j == subAccs.length) {
            return this;
          } else {
            // must resize final field subAccs
            return new MultiAcc(fcontext, ArrayUtil.copyOfSubArray(subAccs, 0, j));
          }
      }
    }
  }

  /**
   * Helper method that subclasses can use to indicate they with to use sweeping.
   * If {@link #countAcc} and {@link #collectAcc} support sweeping, then this method will: 
   * <ul>
   * <li>replace {@link #collectAcc} with it's sweeping equivalent</li>
   * <li>update {@link #allBucketsAcc}'s reference to {@link #collectAcc} (if it exists)</li>
   * </ul>
   *
   * @return true if the above actions were taken
   * @see SweepableSlotAcc
   * @see SweepingCountSlotAcc
   */
  protected boolean registerSweepingAccIfSupportedByCollectAcc() {
    if (countAcc instanceof SweepingCountSlotAcc && collectAcc instanceof SweepableSlotAcc) {
      final SweepingCountSlotAcc sweepingCountAcc = (SweepingCountSlotAcc)countAcc;
      collectAcc = ((SweepableSlotAcc<?>)collectAcc).registerSweepingAccs(sweepingCountAcc);
      if (allBucketsAcc != null) {
        allBucketsAcc.collectAcc = collectAcc;
        allBucketsAcc.sweepingCountAcc = sweepingCountAcc;
      }
      return true;
    }
    return false;
  }

  private static final SlotContext ALL_BUCKETS_SLOT_CONTEXT = new SlotContext(null) {
    @Override
    public Query getSlotQuery() {
      throw new IllegalStateException("getSlotQuery() is mutually exclusive with isAllBuckets==true");
    }
    @Override
    public boolean isAllBuckets() {
      return true;
    }
  };
  private static final IntFunction<SlotContext> ALL_BUCKETS_SLOT_FUNCTION = new IntFunction<SlotContext>() {
    @Override
    public SlotContext apply(int value) {
      return ALL_BUCKETS_SLOT_CONTEXT;
    }
  };

  static class SpecialSlotAcc extends SlotAcc {
    SlotAcc collectAcc;
    SlotAcc[] otherAccs;
    int collectAccSlot;
    int otherAccsSlot;
    long count;
    SweepingCountSlotAcc sweepingCountAcc; // null unless/until sweeping is initialized

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
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      assert slot != collectAccSlot || slot < 0;
      count++;
      if (collectAcc != null) {
        collectAcc.collect(doc, collectAccSlot, ALL_BUCKETS_SLOT_FUNCTION);
      }
      if (otherAccs != null) {
        for (SlotAcc otherAcc : otherAccs) {
          otherAcc.collect(doc, otherAccsSlot, ALL_BUCKETS_SLOT_FUNCTION);
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
      if (sweepingCountAcc != null) {
        sweepingCountAcc.setSweepValues(bucket, collectAccSlot);
      }
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


  /*
   "qfacet":{"cat2":{"_l":["A"]}},
   "all":{"_s":[[
     "all",
     {"cat3":{"_l":["A"]}}]]},
   "cat1":{"_l":["A"]}}}
   */

  @SuppressWarnings({"unchecked"})
  static <T> List<T> asList(Object list) {
    return list != null ? (List<T>)list : Collections.emptyList();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected SimpleOrderedMap<Object> refineFacets() throws IOException {
    boolean skipThisFacet = (fcontext.flags & SKIP_FACET) != 0;


    List leaves = asList(fcontext.facetInfo.get("_l"));        // We have not seen this bucket: do full faceting for this bucket, including all sub-facets
    List<List> skip = asList(fcontext.facetInfo.get("_s"));    // We have seen this bucket, so skip stats on it, and skip sub-facets except for the specified sub-facets that should calculate specified buckets.
    List<List> partial = asList(fcontext.facetInfo.get("_p")); // We have not seen this bucket, do full faceting for this bucket, and most sub-facets... but some sub-facets are partial and should only visit specified buckets.

    // For leaf refinements, we do full faceting for each leaf bucket.  Any sub-facets of these buckets will be fully evaluated.  Because of this, we should never
    // encounter leaf refinements that have sub-facets that return partial results.

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();
    List<SimpleOrderedMap> bucketList = new ArrayList<>( leaves.size() + skip.size() + partial.size() );
    res.add("buckets", bucketList);

    // TODO: an alternate implementations can fill all accs at once
    createAccs(-1, 1);

    for (Object bucketVal : leaves) {
      bucketList.add( refineBucket(bucketVal, false, null) );
    }

    for (List bucketAndFacetInfo : skip) {
      assert bucketAndFacetInfo.size() == 2;
      Object bucketVal = bucketAndFacetInfo.get(0);
      Map<String,Object> facetInfo = (Map<String, Object>) bucketAndFacetInfo.get(1);

      bucketList.add( refineBucket(bucketVal, true, facetInfo ) );
    }

    // The only difference between skip and missing is the value of "skip" passed to refineBucket

    for (List bucketAndFacetInfo : partial) {
      assert bucketAndFacetInfo.size() == 2;
      Object bucketVal = bucketAndFacetInfo.get(0);
      Map<String,Object> facetInfo = (Map<String, Object>) bucketAndFacetInfo.get(1);

      bucketList.add( refineBucket(bucketVal, false, facetInfo ) );
    }

    if (freq.missing) {
      Map<String,Object> bucketFacetInfo = (Map<String,Object>)fcontext.facetInfo.get("missing");

      if (bucketFacetInfo != null || !skipThisFacet) {
        SimpleOrderedMap<Object> missingBucket = new SimpleOrderedMap<>();
        fillBucket(missingBucket, getFieldMissingQuery(fcontext.searcher, freq.field), null, skipThisFacet, bucketFacetInfo);
        res.add("missing", missingBucket);
      }
    }

    if (freq.numBuckets && !skipThisFacet) {
      calculateNumBuckets(res);
    }

    // If there are just a couple of leaves, and if the domain is large, then
    // going by term is likely the most efficient?
    // If the domain is small, or if the number of leaves is large, then doing
    // the normal collection method may be best.

    return res;
  }

  private SimpleOrderedMap<Object> refineBucket(Object bucketVal, boolean skip, Map<String,Object> facetInfo) throws IOException {
    SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
    FieldType ft = sf.getType();
    bucketVal = ft.toNativeType(bucketVal);  // refinement info passed in as JSON will cause int->long and float->double
    bucket.add("val", bucketVal);

    // fieldQuery currently relies on a string input of the value...
    String bucketStr = bucketVal instanceof Date ? ((Date)bucketVal).toInstant().toString() : bucketVal.toString();
    Query domainQ = ft.getFieldTermQuery(null, sf, bucketStr);

    fillBucket(bucket, domainQ, null, skip, facetInfo);

    return bucket;
  }

  /** Resizes to the specified size, remapping all existing slots to slot 0 */
  private static final class FlatteningResizer extends SlotAcc.Resizer {
    private final int slotCount;
    public FlatteningResizer(int slotCount) {
      this.slotCount = slotCount;
    }
    @Override
    public int getNewSize() {
      return slotCount;
    }
    
    @Override
    public int getNewSlot(int oldSlot) {
      return 0;
    }
  }
}
