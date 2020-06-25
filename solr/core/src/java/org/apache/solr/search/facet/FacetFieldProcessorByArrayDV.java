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
import java.util.List;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SweepCountAccStruct;
import org.apache.solr.search.facet.SlotAcc.SweepingCountSlotAcc;
import org.apache.solr.search.facet.SweepCountAware.SegCountGlobal;
import org.apache.solr.search.facet.SweepCountAware.SegCountPerSeg;
import org.apache.solr.uninverting.FieldCacheImpl;

/**
 * Grabs values from {@link DocValues}.
 */
class FacetFieldProcessorByArrayDV extends FacetFieldProcessorByArray {
  static boolean unwrap_singleValued_multiDv = true;  // only set to false for test coverage

  boolean multiValuedField;
  SortedSetDocValues si;  // only used for term lookups (for both single and multi-valued)
  OrdinalMap ordinalMap = null; // maps per-segment ords to global ords

  FacetFieldProcessorByArrayDV(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
    multiValuedField = sf.multiValued() || sf.getType().multiValuedFieldCache();
  }

  @Override
  protected void findStartAndEndOrds() throws IOException {
    if (multiValuedField) {
      si = FieldUtil.getSortedSetDocValues(fcontext.qcontext, sf, null);
      if (si instanceof MultiDocValues.MultiSortedSetDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedSetDocValues)si).mapping;
      }
    } else {
      // multi-valued view
      SortedDocValues single = FieldUtil.getSortedDocValues(fcontext.qcontext, sf, null);
      si = DocValues.singleton(single);
      if (single instanceof MultiDocValues.MultiSortedDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedDocValues)single).mapping;
      }
    }

    if (si.getValueCount() >= Integer.MAX_VALUE) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field has too many unique values. field=" + sf + " nterms= " + si.getValueCount());
    }

    if (prefixRef != null) {
      startTermIndex = (int)si.lookupTerm(prefixRef.get());
      if (startTermIndex < 0) startTermIndex = -startTermIndex - 1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = (int)si.lookupTerm(prefixRef.get());
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex - 1;
    } else {
      startTermIndex = 0;
      endTermIndex = (int)si.getValueCount();
    }

    nTerms = endTermIndex - startTermIndex;
  }

  @Override
  protected void collectDocs() throws IOException {
    int domainSize = fcontext.base.size();

    if (nTerms <= 0 || domainSize < effectiveMincount) { // TODO: what about allBuckets? missing bucket?
      return;
    }

    final SweepCountAccStruct base = SweepingCountSlotAcc.baseStructOf(this);
    final List<SweepCountAccStruct> others = SweepingCountSlotAcc.otherStructsOf(this);
    assert null != base;
    
    // TODO: refactor some of this logic into a base class
    boolean countOnly = collectAcc==null && allBucketsAcc==null;
    boolean fullRange = startTermIndex == 0 && endTermIndex == si.getValueCount();

    // Are we expecting many hits per bucket?
    // FUTURE: pro-rate for nTerms?
    // FUTURE: better take into account number of values in multi-valued fields.  This info is available for indexed fields.
    // FUTURE: take into account that bigger ord maps are more expensive than smaller ones
    // One test: 5M doc index, faceting on a single-valued field with almost 1M unique values, crossover point where global counting was slower
    // than per-segment counting was a domain of 658k docs.  At that point, top 10 buckets had 6-7 matches each.
    // this was for heap docvalues produced by UninvertingReader
    // Since these values were randomly distributed, lets round our domain multiplier up to account for less random real world data.
    long domainMultiplier = multiValuedField ? 4L : 2L;
    boolean manyHitsPerBucket = domainSize * domainMultiplier > (si.getValueCount() + 3);  // +3 to increase test coverage with small tests

    // If we're only calculating counts, we're not prefixing, and we expect to collect many documents per unique value,
    // then collect per-segment before mapping to global ords at the end.  This will save redundant seg->global ord mappings.
    // FUTURE: there are probably some other non "countOnly" cases where we can use this as well (i.e. those where
    // the docid is not used)
    boolean canDoPerSeg = countOnly && fullRange;
    boolean accumSeg = manyHitsPerBucket && canDoPerSeg;

    if (freq.perSeg != null) accumSeg = canDoPerSeg && freq.perSeg;  // internal - override perSeg heuristic

    final int maxSize = others.size() + 1; // others + base
    final List<LeafReaderContext> leaves = fcontext.searcher.getIndexReader().leaves();
    final DocIdSetIterator[] subIterators = new DocIdSetIterator[maxSize];
    final CountSlotAcc[] activeCountAccs = new CountSlotAcc[maxSize];

    for (int subIdx = 0; subIdx < leaves.size(); subIdx++) {
      LeafReaderContext subCtx = leaves.get(subIdx);

      setNextReaderFirstPhase(subCtx);

      final SweepDISI disi = SweepDISI.newInstance(base, others, subIterators, activeCountAccs, subCtx);
      if (disi == null) {
        continue;
      }
      LongValues toGlobal = ordinalMap == null ? null : ordinalMap.getGlobalOrds(subIdx);

      SortedDocValues singleDv = null;
      SortedSetDocValues multiDv = null;
      if (multiValuedField) {
        // TODO: get sub from multi?
        multiDv = subCtx.reader().getSortedSetDocValues(sf.getName());
        if (multiDv == null) {
          if (countOnly) {
            continue;
          } else {
            multiDv = DocValues.emptySortedSet();
          }
        } else if (countOnly && multiDv.getValueCount() < 1){
          continue;
        }
        // some codecs may optimize SortedSet storage for single-valued fields
        // this will be null if this is not a wrapped single valued docvalues.
        if (unwrap_singleValued_multiDv) {
          singleDv = DocValues.unwrapSingleton(multiDv);
        }
      } else {
        singleDv = subCtx.reader().getSortedDocValues(sf.getName());
        if (singleDv == null) {
          if (countOnly) {
            continue;
          } else {
            singleDv = DocValues.emptySorted();
          }
        } else if (countOnly && singleDv.getValueCount() < 1) {
          continue;
        }
      }

      if (singleDv != null) {
        if (accumSeg) {
          collectPerSeg(singleDv, disi, toGlobal);
        } else {
          if (canDoPerSeg && toGlobal != null) {
            collectCounts(singleDv, disi, toGlobal);
          } else {
            collectDocs(singleDv, disi, toGlobal);
          }
        }
      } else {
        if (accumSeg) {
          collectPerSeg(multiDv, disi, toGlobal);
        } else {
          if (canDoPerSeg && toGlobal != null) {
            collectCounts(multiDv, disi, toGlobal);
          } else {
            collectDocs(multiDv, disi, toGlobal);
          }
        }
      }
    }

    Arrays.fill(reuse, null);  // better GC
  }

  @Override
  protected BytesRef lookupOrd(int ord) throws IOException {
    return si.lookupOrd(ord);
  }

  private void collectPerSeg(SortedDocValues singleDv, SweepDISI disi, LongValues toGlobal) throws IOException {
    int segMax = singleDv.getValueCount();
    final SegCountPerSeg segCounter = getSegCountPerSeg(disi, segMax);

    /** alternate trial implementations
     // ord
     // FieldUtil.visitOrds(singleDv, disi,  (doc,ord)->{counts[ord+1]++;} );

    FieldUtil.OrdValues ordValues = FieldUtil.getOrdValues(singleDv, disi);
    while (ordValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      counts[ ordValues.getOrd() + 1]++;
    }
     **/


    // calculate segment-local counts
    int doc;
    if (singleDv instanceof FieldCacheImpl.SortedDocValuesImpl.Iter) {
      FieldCacheImpl.SortedDocValuesImpl.Iter fc = (FieldCacheImpl.SortedDocValuesImpl.Iter) singleDv;
      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        final int segOrd = fc.getOrd(doc);
        if (segOrd >= 0) {
          final int maxIdx = disi.registerCounts(segCounter);
          segCounter.incrementCount(segOrd, 1, maxIdx);
        }
      }
    } else {
      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (singleDv.advanceExact(doc)) {
          final int segOrd = singleDv.ordValue();
          if (segOrd >= 0) {
            final int maxIdx = disi.registerCounts(segCounter);
            segCounter.incrementCount(segOrd, 1, maxIdx);
          }
        }
      }
    }

    // convert segment-local counts to global counts
    segCounter.register(disi.countAccs, toGlobal, segMax - 1);
  }

  private SegCountPerSeg getSegCountPerSeg(SweepDISI disi, int segMax) {
    final int size = disi.size;
    return new SegCountPerSeg(getSegmentCountArrays(segMax, size), getBoolArr(segMax), segMax, size);
  }

  private SegCountGlobal getSegCountGlobal(SweepDISI disi, SortedDocValues dv) {
    return new SegCountGlobal(disi.countAccs);
  }

  private SegCountGlobal getSegCountGlobal(SweepDISI disi, SortedSetDocValues dv) {
    return new SegCountGlobal(disi.countAccs);
  }

  private void collectPerSeg(SortedSetDocValues multiDv, SweepDISI disi, LongValues toGlobal) throws IOException {
    int segMax = (int)multiDv.getValueCount();
    final SegCountPerSeg segCounter = getSegCountPerSeg(disi, segMax);

    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (multiDv.advanceExact(doc)) {
        final int maxIdx = disi.registerCounts(segCounter);
        for (;;) {
          int segOrd = (int)multiDv.nextOrd();
          if (segOrd < 0) break;
          segCounter.incrementCount(segOrd, 1, maxIdx);
        }
      }
    }

    segCounter.register(disi.countAccs, toGlobal, segMax - 1);
  }

  private boolean[] reuseBool;
  private boolean[] getBoolArr(int maxNeeded) {
    if (reuseBool == null) {
      // make the count array large enough for any segment
      // FUTURE: (optionally) directly use the array of the CountAcc for an optimized index..
      reuseBool = new boolean[(int) si.getValueCount() + 1];
    } else {
      Arrays.fill(reuseBool, 0, maxNeeded, false);
    }
    return reuseBool;
  }

  private int[][] reuse = new int[12][];
  private int[] getCountArr(int maxNeeded, int idx) {
    if (idx >= reuse.length) {
      reuse = Arrays.copyOf(reuse, idx + 1);
    }
    if (reuse[idx] == null) {
      // make the count array large enough for any segment
      // FUTURE: (optionally) directly use the array of the CountAcc for an optimized index..
      reuse[idx] = new int[(int) si.getValueCount() + 1];
    } else {
      Arrays.fill(reuse[idx], 0, maxNeeded, 0);
    }
    return reuse[idx];
  }

  private int[][] getSegmentCountArrays(int segMax, int size) {
    int[][] ret = new int[size][];
    int i = size - 1;
    do {
      ret[i] = getCountArr(segMax, i);
    } while (i-- > 0);
    return ret;
  }

  private void collectDocs(SortedDocValues singleDv, SweepDISI disi, LongValues toGlobal) throws IOException {
    int doc;
    final SegCountGlobal segCounter = getSegCountGlobal(disi, singleDv);
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (singleDv.advanceExact(doc)) {
        final int maxIdx = disi.registerCounts(segCounter);
        int segOrd = singleDv.ordValue();
        collect(doc, segOrd, toGlobal, segCounter, maxIdx, disi.collectBase());
      }
    }
  }

  private void collectCounts(SortedDocValues singleDv, SweepDISI disi, LongValues toGlobal) throws IOException {
    final SegCountGlobal segCounter = getSegCountGlobal(disi, singleDv);
    int doc;
    if (singleDv instanceof FieldCacheImpl.SortedDocValuesImpl.Iter) {

      FieldCacheImpl.SortedDocValuesImpl.Iter fc = (FieldCacheImpl.SortedDocValuesImpl.Iter)singleDv;
      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        int segOrd = fc.getOrd(doc);
        if (segOrd < 0) continue;
        int ord = (int)toGlobal.get(segOrd);
        int maxIdx = disi.registerCounts(segCounter);
        segCounter.incrementCount(ord, 1, maxIdx);
      }

    } else {

      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (singleDv.advanceExact(doc)) {
          int segOrd = singleDv.ordValue();
          int ord = (int) toGlobal.get(segOrd);
          int maxIdx = disi.registerCounts(segCounter);
          segCounter.incrementCount(ord, 1, maxIdx);
        }
      }
    }
  }

  private void collectDocs(SortedSetDocValues multiDv, SweepDISI disi, LongValues toGlobal) throws IOException {
    final SegCountGlobal segCounter = getSegCountGlobal(disi, multiDv);
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (multiDv.advanceExact(doc)) {
        final int maxIdx = disi.registerCounts(segCounter);
        final boolean collectBase = disi.collectBase();
        for(;;) {
          int segOrd = (int)multiDv.nextOrd();
          if (segOrd < 0) break;
          collect(doc, segOrd, toGlobal, segCounter, maxIdx, collectBase);
        }
      }
    }
  }

  private void collectCounts(SortedSetDocValues multiDv, SweepDISI disi, LongValues toGlobal) throws IOException {
    final SegCountGlobal segCounter = getSegCountGlobal(disi, multiDv);
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (multiDv.advanceExact(doc)) {
        final int maxIdx = disi.registerCounts(segCounter);
        for(;;) {
          int segOrd = (int)multiDv.nextOrd();
          if (segOrd < 0) break;
          int ord = (int)toGlobal.get(segOrd);
          segCounter.incrementCount(ord, 1, maxIdx);
        }
      }
    }
  }

  private void collect(int doc, int segOrd, LongValues toGlobal, SegCountGlobal segCounter, int maxIdx, boolean collectBase) throws IOException {
    int ord = (toGlobal != null && segOrd >= 0) ? (int)toGlobal.get(segOrd) : segOrd;

    int arrIdx = ord - startTermIndex;
    // This code handles faceting prefixes, which narrows the range of ords we want to collect.
    // It’s not an error for an ord to fall outside this range… we simply want to skip it.
    if (arrIdx >= 0 && arrIdx < nTerms) {
      segCounter.incrementCount(arrIdx, 1, maxIdx);
      if (collectBase) {
        if (collectAcc != null) {
          collectAcc.collect(doc, arrIdx, slotContext);
        }
        if (allBucketsAcc != null) {
          allBucketsAcc.collect(doc, arrIdx, slotContext);
        }
      }
    }
  }

}
