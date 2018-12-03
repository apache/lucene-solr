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
package org.apache.lucene.codecs.lucene70;

import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.RankBitSet;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Utility class for generating compressed read-only in-memory representations of longs.
 * The representation is optimized towards random access primarily and space secondarily.
 *
 * The representation always applies delta-to-minvalue and greatest-common-divisor compression.
 *
 * Depending on the number of 0-entries and the length of the array, a sparse representation is
 * used, using rank to improve access speed. Sparseness introduces an O(1) access time overhead.
 * Sparseness can be turned off.
 */
public class LongCompressor {

  /**
   * The minimum amount of total values in the data set for sparse to be active.
   */
  private static final int DEFAULT_MIN_TOTAL_VALUES_FOR_SPARSE = 10_000;

  /**
   * The minimum total amount of zero values in the data set for sparse to be active.
   */
  private static final int DEFAULT_MIN_ZERO_VALUES_FOR_SPARSE = 500;

  /**
   * The minimum fraction of the data set that must be zero for sparse to be active.
   */
  private static final double DEFAULT_MIN_ZERO_VALUES_FRACTION_FOR_SPARSE = 0.2; // 20% (just guessing of a value here)

  /**
   * Create a compact version of the given values.
   * @param values PackedInts with no special constraints.
   * @return a compact version of the given values or the given values if compression did not improve on heap overhead.
   */
  public static PackedInts.Reader compress(PackedInts.Reader values) {
    return compress(values, values.size());
  }

  /**
   * Create a compact version of the given values from index 0 to length-1.
   * @param values PackedInts with no special constraints.
   * @param length the number of values to compress.
   * @return a compact version of the given values or the given values if compression did not improve on heap overhead.
   */
  public static PackedInts.Reader compress(PackedInts.Reader values, int length) {
    return compress(values, values.size(), true);
  }

  /**
   * Create a compact version of the given values from index 0 to length-1.
   * @param values PackedInts with no special constraints.
   * @param length the number of values to compress.
   * @param allowSparse if true and is the default limits matches the input, a sparse representation will be created.
   * @return a compact version of the given values or the given values if compression did not improve on heap overhead.
   */
  public static PackedInts.Reader compress(PackedInts.Reader values, int length, boolean allowSparse) {
    return compress(values, length, allowSparse,
        DEFAULT_MIN_TOTAL_VALUES_FOR_SPARSE,
        DEFAULT_MIN_ZERO_VALUES_FOR_SPARSE,
        DEFAULT_MIN_ZERO_VALUES_FRACTION_FOR_SPARSE);
  }

  /**
   * Create a compact version of the given values from index 0 to length-1.
   * @param values PackedInts with no special constraints.
   * @param length the number of values to compress.
   * @param allowSparse if true and is the default limits matches the input, a sparse representation will be created.
   * @param minTotalSparse the minimum absolute number of 0-entries needed for a sparse representation.
   *                       0-entries are counted after minValue compression: {@code 3, 5, 3, 7, 16} has two 0-entries.
   * @return a compact version of the given values or the given values if compression did not improve on heap overhead.
   */
  public static PackedInts.Reader compress(
      PackedInts.Reader values, int length, boolean allowSparse,
      int minTotalSparse, int minZeroSparse, double minZeroFractionSparse) {
    if (length == 0) {
      return PackedInts.getMutable(0, 1, PackedInts.DEFAULT);
    }

    final long min = getMin(values, length);
    final long gcd = getGCD(values, length, min);
    final long maxCompressed = getMax(values, length, min, gcd);

    int zeroCount;
    if (!isPossiblySparseCandidate(length, allowSparse, minTotalSparse) ||
        !isSparseCandidate(values, length, true, minTotalSparse,
            (zeroCount = countZeroes(values, length, min, gcd)), minZeroSparse, minZeroFractionSparse)) {
      // TODO: Add abort-early if it becomes apparent that no space saving is possible
      PackedInts.Mutable inner =
          PackedInts.getMutable(length, PackedInts.bitsRequired(maxCompressed), PackedInts.DEFAULT);
      for (int i = 0 ; i < length ; i++) {
        inner.set(i, (values.get(i)-min)/gcd);
      }
      PackedInts.Reader comp = new CompressedReader(inner, min, gcd);
      // Sanity check that compression worked and if not, return the original input
      return comp.ramBytesUsed() < values.ramBytesUsed() ? comp : values;
    }

    // Sparsify
    RankBitSet rank = new RankBitSet(length);
    PackedInts.Mutable inner =
        PackedInts.getMutable(values.size()-zeroCount, PackedInts.bitsRequired(maxCompressed), PackedInts.DEFAULT);
    int valueIndex = 0;
    for (int i = 0 ; i < length ; i++) {
      long value = (values.get(i)-min)/gcd;
      if (value != 0) {
        rank.set(i);
        inner.set(valueIndex++, value);
      }
    }
    rank.buildRankCache();
    PackedInts.Reader comp = new CompressedReader(inner, min, gcd, rank);
    // Sanity check that compression worked and if not, return the original input
    return comp.ramBytesUsed() < values.ramBytesUsed() ? comp : values;
  }

  // Fast check
  private static boolean isPossiblySparseCandidate(int length, boolean allowSparse, int minTotalSparse) {
    return allowSparse && minTotalSparse <= length;
  }

  // Also fast, but requires zeroCount which is slow to calculate
  private static boolean isSparseCandidate(
      PackedInts.Reader values, int length, boolean allowSparse, int minTotalSparse,
      int zeroCount, int minZeroSparse, double minZeroFractionSparse) {
    return allowSparse && minTotalSparse <= length &&
        minZeroSparse < zeroCount && minZeroFractionSparse < 1.0 * zeroCount / length;
  }

  // Not very fast as is requires #length divisions.
  private static int countZeroes(PackedInts.Reader values, int length, long min, final long gcd) {
    int zeroCount = 0;
    for (int i = 0 ; i < length ; i++) {
      if ((values.get(i)-min)/gcd == 0) { // Hope the case where gcd==1 gets JITted. We could add a switch to be sure?
        zeroCount++;
      }
    }
    return zeroCount;
  }

  static class CompressedReader extends PackedInts.Reader {
    private final PackedInts.Reader inner;
    final long min;
    final long gcd;
    final RankBitSet rank;

    CompressedReader(PackedInts.Reader inner, long min, long gcd) {
      this(inner, min, gcd, null);
    }

    CompressedReader(PackedInts.Reader inner, long min, long gcd, RankBitSet rank) {
      this.inner = inner;
      this.min = min;
      this.gcd = gcd;
      this.rank = rank;
    }

    @Override
    public int size() {
      return rank == null ? inner.size() : rank.length();
    }

    @Override
    public long get(int docID) {
      // No rank: The value at the index
      // Rank but no set bit: min*gcd
      // Rank and set bit: (The value at the rank + min) * gcd
      return (rank == null ? inner.get(docID) : rank.get(docID) ? inner.get(rank.rank(docID)) : 0) * gcd + min;
    }

    @Override
    public long ramBytesUsed() {
      return inner.ramBytesUsed() + (rank == null ? 0 : rank.ramBytesUsed());
    }
  }

  private static long getMin(PackedInts.Reader values, int length) {
    long min = Long.MAX_VALUE;
    for (int i = 0 ; i < length ; i++) {
      if (min > values.get(i)) {
        min = values.get(i);
      }
    }
    return min;
  }

  // GCD-code takes & adjusted from Lucene70DocValuesConsumer
  private static long getGCD(final PackedInts.Reader values, final int length, final long min) {
    long gcd = -1;

    for (int i = 0 ; i < length ; i++) {
      long value = values.get(i)-min;
      if (value == 0) {
        continue;
      }
      if (gcd == -1) {
        gcd = value;
        continue;
      }

      if (value < Long.MIN_VALUE / 2 || value > Long.MAX_VALUE / 2) {
        // in that case v - minValue might overflow and make the GCD computation return
        // wrong results. Since these extreme values are unlikely, we just discard
        // GCD computation for them
        gcd = 1;
      } else { // minValue needs to be set first
        gcd = MathUtil.gcd(gcd, value);
      }

      if (gcd == 1) {
        break;
      }
    }
    return gcd == -1 ? 1 : gcd;
  }

  private static long getMax(final PackedInts.Reader values, final int length, final long min, final long gcd) {
    long rawMax = Long.MIN_VALUE;
    for (int i = 0 ; i < length ; i++) {
      long value = values.get(i);
      if (value > rawMax) {
        rawMax = value;
      }
    }
    return (rawMax-min)/gcd;
  }
}
