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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Maps per-segment ordinals to/from global ordinal space, using a compact packed-ints representation.
 *
 *  <p><b>NOTE</b>: this is a costly operation, as it must merge sort all terms, and may require non-trivial RAM once done.  It's better to operate in
 *  segment-private ordinal space instead when possible.
 *
 * @lucene.internal */
public class OrdinalMap implements Accountable {
  // TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we need it
  // TODO: use more efficient packed ints structures?

  private static class TermsEnumIndex {
    public final static TermsEnumIndex[] EMPTY_ARRAY = new TermsEnumIndex[0];
    final int subIndex;
    final TermsEnum termsEnum;
    BytesRef currentTerm;

    public TermsEnumIndex(TermsEnum termsEnum, int subIndex) {
      this.termsEnum = termsEnum;
      this.subIndex = subIndex;
    }

    public BytesRef next() throws IOException {
      currentTerm = termsEnum.next();
      return currentTerm;
    }
  }

  private static class SegmentMap implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SegmentMap.class);

    /** Build a map from an index into a sorted view of `weights` to an index into `weights`. */
    private static int[] map(final long[] weights) {
      final int[] newToOld = new int[weights.length];
      for (int i = 0; i < weights.length; ++i) {
        newToOld[i] = i;
      }
      new InPlaceMergeSorter() {
        @Override
        protected void swap(int i, int j) {
          final int tmp = newToOld[i];
          newToOld[i] = newToOld[j];
          newToOld[j] = tmp;
        }
        @Override
        protected int compare(int i, int j) {
          // j first since we actually want higher weights first
          return Long.compare(weights[newToOld[j]], weights[newToOld[i]]);
        }
      }.sort(0, weights.length);
      return newToOld;
    }

    /** Inverse the map. */
    private static int[] inverse(int[] map) {
      final int[] inverse = new int[map.length];
      for (int i = 0; i < map.length; ++i) {
        inverse[map[i]] = i;
      }
      return inverse;
    }

    private final int[] newToOld, oldToNew;

    SegmentMap(long[] weights) {
      newToOld = map(weights);
      oldToNew = inverse(newToOld);
      assert Arrays.equals(newToOld, inverse(oldToNew));
    }

    int newToOld(int segment) {
      return newToOld[segment];
    }

    int oldToNew(int segment) {
      return oldToNew[segment];
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(newToOld) + RamUsageEstimator.sizeOf(oldToNew);
    }
  }

  /**
   * Create an ordinal map that uses the number of unique values of each
   * {@link SortedDocValues} instance as a weight.
   * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
   */
  public static OrdinalMap build(IndexReader.CacheKey owner, SortedDocValues[] values, float acceptableOverheadRatio) throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];
    final long[] weights = new long[values.length];
    for (int i = 0; i < values.length; ++i) {
      subs[i] = values[i].termsEnum();
      weights[i] = values[i].getValueCount();
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }

  /**
   * Create an ordinal map that uses the number of unique values of each
   * {@link SortedSetDocValues} instance as a weight.
   * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
   */
  public static OrdinalMap build(IndexReader.CacheKey owner, SortedSetDocValues[] values, float acceptableOverheadRatio) throws IOException {
    final TermsEnum[] subs = new TermsEnum[values.length];
    final long[] weights = new long[values.length];
    for (int i = 0; i < values.length; ++i) {
      subs[i] = values[i].termsEnum();
      weights[i] = values[i].getValueCount();
    }
    return build(owner, subs, weights, acceptableOverheadRatio);
  }

  /** 
   * Creates an ordinal map that allows mapping ords to/from a merged
   * space from <code>subs</code>.
   * @param owner a cache key
   * @param subs TermsEnums that support {@link TermsEnum#ord()}. They need
   *             not be dense (e.g. can be FilteredTermsEnums}.
   * @param weights a weight for each sub. This is ideally correlated with
   *             the number of unique terms that each sub introduces compared
   *             to the other subs
   * @throws IOException if an I/O error occurred.
   */
  public static OrdinalMap build(IndexReader.CacheKey owner, TermsEnum subs[], long[] weights, float acceptableOverheadRatio) throws IOException {
    if (subs.length != weights.length) {
      throw new IllegalArgumentException("subs and weights must have the same length");
    }

    // enums are not sorted, so let's sort to save memory
    final SegmentMap segmentMap = new SegmentMap(weights);
    return new OrdinalMap(owner, subs, segmentMap, acceptableOverheadRatio);
  }

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OrdinalMap.class);

  /** Cache key of whoever asked for this awful thing */
  public final IndexReader.CacheKey owner;
  // number of global ordinals
  final long valueCount;
  // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the the ordinal in the first segment that contains this term
  final LongValues globalOrdDeltas;
  // globalOrd -> first segment container
  final LongValues firstSegments;
  // for every segment, segmentOrd -> globalOrd
  final LongValues segmentToGlobalOrds[];
  // the map from/to segment ids
  final SegmentMap segmentMap;
  // ram usage
  final long ramBytesUsed;
    
  OrdinalMap(IndexReader.CacheKey owner, TermsEnum subs[], SegmentMap segmentMap, float acceptableOverheadRatio) throws IOException {
    // create the ordinal mappings by pulling a termsenum over each sub's 
    // unique terms, and walking a multitermsenum over those
    this.owner = owner;
    this.segmentMap = segmentMap;
    // even though we accept an overhead ratio, we keep these ones with COMPACT
    // since they are only used to resolve values given a global ord, which is
    // slow anyway
    PackedLongValues.Builder globalOrdDeltas = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    PackedLongValues.Builder firstSegments = PackedLongValues.packedBuilder(PackedInts.COMPACT);
    long firstSegmentBits = 0L;
    final PackedLongValues.Builder[] ordDeltas = new PackedLongValues.Builder[subs.length];
    for (int i = 0; i < ordDeltas.length; i++) {
      ordDeltas[i] = PackedLongValues.monotonicBuilder(acceptableOverheadRatio);
    }
    long[] ordDeltaBits = new long[subs.length];
    long[] segmentOrds = new long[subs.length];

    // Just merge-sorts by term:
    PriorityQueue<TermsEnumIndex> queue = new PriorityQueue<TermsEnumIndex>(subs.length) {
        @Override
        protected boolean lessThan(TermsEnumIndex a, TermsEnumIndex b) {
          return a.currentTerm.compareTo(b.currentTerm) < 0;
        }
      };
    
    for (int i = 0; i < subs.length; i++) {
      TermsEnumIndex sub = new TermsEnumIndex(subs[segmentMap.newToOld(i)], i);
      if (sub.next() != null) {
        queue.add(sub);
      }
    }

    BytesRefBuilder scratch = new BytesRefBuilder();
      
    long globalOrd = 0;
    while (queue.size() != 0) {
      TermsEnumIndex top = queue.top();
      scratch.copyBytes(top.currentTerm);

      int firstSegmentIndex = Integer.MAX_VALUE;
      long globalOrdDelta = Long.MAX_VALUE;

      // Advance past this term, recording the per-segment ord deltas:
      while (true) {
        top = queue.top();
        long segmentOrd = top.termsEnum.ord();
        long delta = globalOrd - segmentOrd;
        int segmentIndex = top.subIndex;
        // We compute the least segment where the term occurs. In case the
        // first segment contains most (or better all) values, this will
        // help save significant memory
        if (segmentIndex < firstSegmentIndex) {
          firstSegmentIndex = segmentIndex;
          globalOrdDelta = delta;
        }
        ordDeltaBits[segmentIndex] |= delta;

        // for each per-segment ord, map it back to the global term; the while loop is needed
        // in case the incoming TermsEnums don't have compact ordinals (some ordinal values
        // are skipped), which can happen e.g. with a FilteredTermsEnum:
        assert segmentOrds[segmentIndex] <= segmentOrd;

        // TODO: we could specialize this case (the while loop is not needed when the ords
        // are compact)
        do {
          ordDeltas[segmentIndex].add(delta);
          segmentOrds[segmentIndex]++;
        } while (segmentOrds[segmentIndex] <= segmentOrd);
        
        if (top.next() == null) {
          queue.pop();
          if (queue.size() == 0) {
            break;
          }
        } else {
          queue.updateTop();
        }
        if (queue.top().currentTerm.equals(scratch.get()) == false) {
          break;
        }
      }

      // for each unique term, just mark the first segment index/delta where it occurs
      firstSegments.add(firstSegmentIndex);
      firstSegmentBits |= firstSegmentIndex;
      globalOrdDeltas.add(globalOrdDelta);
      globalOrd++;
    }

    long ramBytesUsed = BASE_RAM_BYTES_USED + segmentMap.ramBytesUsed();
    this.valueCount = globalOrd;

    // If the first segment contains all of the global ords, then we can apply a small optimization
    // and hardcode the first segment indices and global ord deltas as all zeroes.
    if (ordDeltaBits.length > 0 && ordDeltaBits[0] == 0L && firstSegmentBits == 0L) {
      this.firstSegments = LongValues.ZEROES;
      this.globalOrdDeltas = LongValues.ZEROES;
    } else {
      PackedLongValues packedFirstSegments = firstSegments.build();
      PackedLongValues packedGlobalOrdDeltas = globalOrdDeltas.build();
      this.firstSegments = packedFirstSegments;
      this.globalOrdDeltas = packedGlobalOrdDeltas;
      ramBytesUsed += packedFirstSegments.ramBytesUsed() + packedGlobalOrdDeltas.ramBytesUsed();
    }

    // ordDeltas is typically the bottleneck, so let's see what we can do to make it faster
    segmentToGlobalOrds = new LongValues[subs.length];
    ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds);
    for (int i = 0; i < ordDeltas.length; ++i) {
      final PackedLongValues deltas = ordDeltas[i].build();
      if (ordDeltaBits[i] == 0L) {
        // segment ords perfectly match global ordinals
        // likely in case of low cardinalities and large segments
        segmentToGlobalOrds[i] = LongValues.IDENTITY;
      } else {
        final int bitsRequired = ordDeltaBits[i] < 0 ? 64 : PackedInts.bitsRequired(ordDeltaBits[i]);
        final long monotonicBits = deltas.ramBytesUsed() * 8;
        final long packedBits = bitsRequired * deltas.size();
        if (deltas.size() <= Integer.MAX_VALUE
            && packedBits <= monotonicBits * (1 + acceptableOverheadRatio)) {
          // monotonic compression mostly adds overhead, let's keep the mapping in plain packed ints
          final int size = (int) deltas.size();
          final PackedInts.Mutable newDeltas = PackedInts.getMutable(size, bitsRequired, acceptableOverheadRatio);
          final PackedLongValues.Iterator it = deltas.iterator();
          for (int ord = 0; ord < size; ++ord) {
            newDeltas.set(ord, it.next());
          }
          assert it.hasNext() == false;
          segmentToGlobalOrds[i] = new LongValues() {
              @Override
              public long get(long ord) {
                return ord + newDeltas.get((int) ord);
              }
            };
          ramBytesUsed += newDeltas.ramBytesUsed();
        } else {
          segmentToGlobalOrds[i] = new LongValues() {
              @Override
              public long get(long ord) {
                return ord + deltas.get(ord);
              }
            };
          ramBytesUsed += deltas.ramBytesUsed();
        }
        ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds[i]);
      }
    }

    this.ramBytesUsed = ramBytesUsed;
  }

  /** 
   * Given a segment number, return a {@link LongValues} instance that maps
   * segment ordinals to global ordinals.
   */
  public LongValues getGlobalOrds(int segmentIndex) {
    return segmentToGlobalOrds[segmentMap.oldToNew(segmentIndex)];
  }

  /**
   * Given global ordinal, returns the ordinal of the first segment which contains
   * this ordinal (the corresponding to the segment return {@link #getFirstSegmentNumber}).
   */
  public long getFirstSegmentOrd(long globalOrd) {
    return globalOrd - globalOrdDeltas.get(globalOrd);
  }
    
  /** 
   * Given a global ordinal, returns the index of the first
   * segment that contains this term.
   */
  public int getFirstSegmentNumber(long globalOrd) {
    return segmentMap.newToOld((int) firstSegments.get(globalOrd));
  }
    
  /**
   * Returns the total number of unique terms in global ord space.
   */
  public long getValueCount() {
    return valueCount;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    resources.add(Accountables.namedAccountable("segment map", segmentMap));
    // TODO: would be nice to return the ordinal and segment maps too, but it's not straightforward
    //  because of optimizations.
    return resources;
  }
}
