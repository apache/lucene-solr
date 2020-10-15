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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LongValues;
import org.apache.solr.request.TermFacetCache.CacheUpdater;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;

/**
 * Implemented by extensions of doc iterators (i.e., {@link DocIdSetIterator}, {@link DocIterator} over one or
 * more domain, to support facet count accumulation corresponding to each domain (and via {@link #collectBase()}
 * to inform the necessity of "collection" for a single optional backing "base" set).
 */
interface SweepCountAware {

  /**
   * Returns true if one of the domains underlying this iterator is the "base" domain, and if that base domain
   * contains the doc on which this iterator is currently positioned. If "true", then "collection" may be necessary
   * for the current doc.
   * 
   * For each iterator position (each doc), {@link #registerCounts(SegCounter)} must be called before this method.
   */
  boolean collectBase();

  /**
   * Called on a positioned doc iterator to register array index mappings for domains that contain the current
   * doc. Implementations register these mappings by calling {@link SegCounter#map(int, int)} on the specified
   * segCounts param.
   * 
   * For each iterator position, this method must be called before {@link #collectBase()}
   *
   * @param segCounts - to register mappings of array indices for domains that contain this doc
   * @return - the max index of an array representing the domains that contain the current doc. If "n" domains
   * contain the current doc, the return value would be "n - 1"
   * @throws IOException - if thrown by advancing an underlying doc iterator
   */
  int registerCounts(SegCounter segCounts) throws IOException;

  /**
   * Used to coordinate multiple count accumulations over multiple domains. Implementers will have "n" backing term-ord-indexed
   * counts -- one for each domain over which count accumulation is to be performed. For each doc, count accumulation
   * takes place in two phases, invoked by a "driver" (e.g., {@link FacetFieldProcessor}) that manages iteration over the
   * union of doc domains:
   * 
   * First, the driver passes this object as the param to {@link SweepCountAware#registerCounts(SegCounter)}, which
   * calls {@link #map(int, int)} on "this" to map the static "allIdx" (allIdx &lt; n) for each active backing domain to
   * a transient "activeIdx" for counts corresponding to active domains (activeIdx &lt; count(allIdx) &lt;= n). (The return value
   * of {@link SweepCountAware#registerCounts(SegCounter)} indicates to the "driver" the max "active counts" index (for
   * domains that contain the current doc).
   * 
   * The driver then calls {@link SegCountPerSeg#incrementCount(int, int, int)} or {@link SegCountGlobal#incrementCount(int, int, int, int)},
   * passing the seg term ord (or seg term ord and global term ord, respectively), increment amount (usually "1"), and the max
   * "active counts" index returned from {@link SweepCountAware#registerCounts(SegCounter)} in the first phase. The "max active counts
   * index" param is used as the limit (inclusive) to iterate count accumulation over each of the "active" domains for the current doc.
   * 
   * @see SweepCountAware#registerCounts(SegCounter)
   */
  static interface SegCounter {
    /**
     * Mark/map a given domain/CountSlotAcc as active (eligible for count accumulation) for the current doc.
     *
     * @param allIdx - the static index of the domain/CountSlotAcc to be "activated" for the current doc
     * @param activeIdx - the transient "active index" (for the purpose of actual count accumulation) to which to map
     * the domain/CountSlotAcc indicated by "allIdx".
     */
    void map(int allIdx, int activeIdx);
  }

  /**
   * This class is designed to count over global term ords ({@link SegCountPerSeg} provides equivalent functionality for
   * per-segment term ords).
   * 
   * @see SegCountPerSeg
   */
  static class SegCountGlobal implements SegCounter {
    private final CountSlotAcc[] allCounts;
    private final CountSlotAcc[] activeCounts;

    public SegCountGlobal(CountSlotAcc[] allCounts) {
      this.allCounts = allCounts;
      this.activeCounts = Arrays.copyOf(allCounts, allCounts.length);
    }

    @Override
    public void map(int allIdx, int activeIdx) {
      activeCounts[activeIdx] = allCounts[allIdx];
    }

    /**
     * Increments counts for active domains/CountSlotAccs.
     * 
     * @param segOrd - the seg term ord for which to increment counts
     * @param globalOrd - the global term ord for which to increment counts
     * @param inc - the amount by which to increment the count for the specified term ord
     * @param maxIdx - the max index (inclusive) of active domains/CountSlotAccs to be incremented for the current doc
     */
    public final void incrementCount(int segOrd, int globalOrd, int inc, int maxIdx) {
      int i = maxIdx;
      do {
        incrementIdxCount(i, segOrd, globalOrd, inc);
      } while (i-- > 0);
    }

    protected void incrementIdxCount(int idx, int segOrd, int globalOrd, int inc) {
      activeCounts[idx].incrementCount(globalOrd, inc);
    }

    public void register() {
      //NoOp
    }

    public int getSegMissingIdx() {
      return -1;
    }
  }

  static final class SegCountGlobalCache extends SegCountGlobal {
    private final int[][] allSegCounts;
    private final int[][] activeSegCounts;
    private final CacheUpdater[] cacheUpdaters;
    private final int segMissingIdx;

    public SegCountGlobalCache(int[][] allSegCounts, int segMax, CountSlotAcc[] allCounts, CacheUpdater[] cacheUpdaters) {
      super(allCounts);
      this.allSegCounts = allSegCounts;
      this.activeSegCounts = Arrays.copyOf(this.allSegCounts, this.allSegCounts.length);
      this.cacheUpdaters = cacheUpdaters;
      this.segMissingIdx = segMax - 1;
    }

    @Override
    public void map(int allIdx, int activeIdx) {
      super.map(allIdx, activeIdx);
      activeSegCounts[activeIdx] = allSegCounts[allIdx];
    }

    @Override
    protected void incrementIdxCount(int idx, int segOrd, int globalOrd, int inc) {
      super.incrementIdxCount(idx, segOrd, globalOrd, inc);
      if (activeSegCounts[idx] != null) {
        activeSegCounts[idx][segOrd] += inc;
      }
    }

    @Override
    public void register() {
      int i = allSegCounts.length - 1;
      do {
        if (cacheUpdaters[i] != null) {
          cacheUpdaters[i].updateLeaf(allSegCounts[i]);
        }
      } while (i-- > 0);
    }

    @Override
    public int getSegMissingIdx() {
      return segMissingIdx;
    }
  }

  /**
   * This class is designed to count over per-segment term ords ({@link SegCountGlobal} provides equivalent functionality for
   * global term ords).
   * 
   * @see SegCountGlobal
   */
  static class SegCountPerSeg implements SegCounter {
    protected final int[][] allSegCounts;
    private final int[][] activeSegCounts;
    private final boolean[] seen;

    public SegCountPerSeg(int[][] allSegCounts, boolean[] seen, int segMax, int size) {
      this.allSegCounts = allSegCounts;
      this.activeSegCounts = Arrays.copyOf(this.allSegCounts, size);
      this.seen = seen;
    }

    @Override
    public final void map(int allIdx, int activeIdx) {
      activeSegCounts[activeIdx] = allSegCounts[allIdx];
    }

    public final void incrementCount(int segOrd, int inc, int maxIdx) {
      seen[segOrd] = true;
      int i = maxIdx;
      do {
        activeSegCounts[i][segOrd] += inc;
      } while (i-- > 0);
    }

    /**
     * Maps accumulated per-segment term ords to global term ords and increments global slots on the specified countAccs
     * accordingly. The index of each CountSlotAcc in the specified countAccs array must correspond to the
     * the static index of its associated count accumulation doc domain and per-seg count array.
     * 
     * @param countAccs - global-scope CountSlotAccs (one for each domain) to be incremented for the most recently accumulated
     * segment
     * @param toGlobal - mapping of per-segment term ords to global term ords for the most recently accumulated segment
     * @param segMissingIdx - index for the "missing" bucket on most recently accumulated segment
     * @param globalMissingIdx - index for the global "missing" bucket
     */
    public void register(CountSlotAcc[] countAccs, LongValues toGlobal, int segMissingIdx, int globalMissingIdx) {
      // NOTE: this method is optimized, with demonstrable benefits for the common "single count" use case.
      // Be careful of performance if modifying.
      int segOrd;
      final int maxIdx = activeSegCounts.length - 1; // based on actual size -- countAccs may be oversized nocommit: until better idea of why this?
      if (segMissingIdx < 0) {
        // not tracking "missing"; segMissingIdx represents (-maxSegOrd - 1)
        segOrd = ~segMissingIdx;
      } else {
        // only one of these, so potential JIT hit of checking seen[segMissingIdx] is not worth it.
        int i = maxIdx;
        do {
          countAccs[i].incrementCount(globalMissingIdx, allSegCounts[i][segMissingIdx]);
        } while (i-- > 0);
        if (segMissingIdx > 0) {
          segOrd = segMissingIdx - 1;
        } else {
          // only "missing"
          return;
        }
      }
      do {
        if (seen[segOrd]) {
          int i = maxIdx;
          do {
            final int inc = allSegCounts[i][segOrd];
            if (inc > 0) {
              countAccs[i].incrementCount(toGlobal == null ? (segOrd) : (int)toGlobal.get(segOrd), inc);
            }
          } while (i-- > 0);
        }
      } while (segOrd-- > 0);
    }
  }

  static final class SegCountPerSegCache extends SegCountPerSeg {

    private final CacheUpdater[] cacheUpdaters;

    public SegCountPerSegCache(int[][] allSegCounts, boolean[] seen, int segMax, int size, CacheUpdater[] cacheUpdaters) {
      super(allSegCounts, seen, segMax, size);
      this.cacheUpdaters = cacheUpdaters;
    }

    @Override
    public void register(CountSlotAcc[] countAccs, LongValues toGlobal, int segMax, int globalMissingIdx) {
      super.register(countAccs, toGlobal, segMax, globalMissingIdx);
      int i = cacheUpdaters.length - 1;
      do {
        if (cacheUpdaters[i] != null) {
          cacheUpdaters[i].updateLeaf(allSegCounts[i]);
        }
      } while (i-- > 0);
    }
  }

}
