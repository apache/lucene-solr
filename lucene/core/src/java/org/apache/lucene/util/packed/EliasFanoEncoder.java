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

package org.apache.lucene.util.packed;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.FixedBitSet; // for javadocs
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ToStringUtils;


/** Encode a non decreasing sequence of non negative whole numbers in the Elias-Fano encoding
 * that was introduced in the 1970's by Peter Elias and Robert Fano.
 * <p>
 * The Elias-Fano encoding is a high bits / low bits representation of
 * a monotonically increasing sequence of {@code numValues > 0} natural numbers <code>x[i]</code>
 * <p>
 * {@code 0 <= x[0] <= x[1] <= ... <= x[numValues-2] <= x[numValues-1] <= upperBound}
 * <p>
 * where {@code upperBound > 0} is an upper bound on the last value.
 * <br>
 * The Elias-Fano encoding uses less than half a bit per encoded number more
 * than the smallest representation
 * that can encode any monotone sequence with the same bounds.
 * <p>
 * The lower <code>L</code> bits of each <code>x[i]</code> are stored explicitly and contiguously
 * in the lower-bits array, with <code>L</code> chosen as (<code>log()</code> base 2):
 * <p>
 * <code>L = max(0, floor(log(upperBound/numValues)))</code>
 * <p>
 * The upper bits are stored in the upper-bits array as a sequence of unary-coded gaps (<code>x[-1] = 0</code>):
 * <p>
 * <code>(x[i]/2**L) - (x[i-1]/2**L)</code>
 * <p>
 * The unary code encodes a natural number <code>n</code> by <code>n</code> 0 bits followed by a 1 bit:
 * <code>0...01</code>. <br>
 * In the upper bits the total the number of 1 bits is <code>numValues</code>
 * and the total number of 0 bits is:<p>
 * {@code floor(x[numValues-1]/2**L) <= upperBound/(2**max(0, floor(log(upperBound/numValues)))) <= 2*numValues}
 * <p>
 * The Elias-Fano encoding uses at most
 * <p>
 * <code>2 + ceil(log(upperBound/numValues))</code>
 * <p>
 * bits per encoded number. With <code>upperBound</code> in these bounds (<code>p</code> is an integer):
 * <p>
 * {@code 2**p < x[numValues-1] <= upperBound <= 2**(p+1)}
 * <p>
 * the number of bits per encoded number is minimized.
 * <p>
 * In this implementation the values in the sequence can be given as <code>long</code>,
 * <code>numValues = 0</code> and <code>upperBound = 0</code> are allowed,
 * and each of the upper and lower bit arrays should fit in a <code>long[]</code>.
 * <br>
 * An index of positions of zero's in the upper bits is also built.
 * <p>
 * This implementation is based on this article:
 * <br>
 * Sebastiano Vigna, "Quasi Succinct Indices", June 19, 2012, sections 3, 4 and 9.
 * Retrieved from http://arxiv.org/pdf/1206.4300 .
 *
 * <p>The articles originally describing the Elias-Fano representation are:
 * <br>Peter Elias, "Efficient storage and retrieval by content and address of static files",
 * J. Assoc. Comput. Mach., 21(2):246–260, 1974.
 * <br>Robert M. Fano, "On the number of bits required to implement an associative memory",
 *  Memorandum 61, Computer Structures Group, Project MAC, MIT, Cambridge, Mass., 1971.
 *
 * @lucene.internal
 */

public class EliasFanoEncoder implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(EliasFanoEncoder.class);

  final long numValues;
  private final long upperBound;
  final int numLowBits;
  final long lowerBitsMask;
  final long[] upperLongs;
  final long[] lowerLongs;
  private static final int LOG2_LONG_SIZE = Long.numberOfTrailingZeros(Long.SIZE);

  long numEncoded = 0L;
  long lastEncoded = 0L;

  /** The default index interval for zero upper bits. */
  public static final long DEFAULT_INDEX_INTERVAL = 256;
  final long numIndexEntries;
  final long indexInterval;
  final int nIndexEntryBits;
  /** upperZeroBitPositionIndex[i] (filled using packValue) will contain the bit position
   *  just after the zero bit ((i+1) * indexInterval) in the upper bits.
   */
  final long[] upperZeroBitPositionIndex;
  long currentEntryIndex; // also indicates how many entries in the index are valid.



  /**
   * Construct an Elias-Fano encoder.
   * After construction, call {@link #encodeNext} <code>numValues</code> times to encode
   * a non decreasing sequence of non negative numbers.
   * @param numValues The number of values that is to be encoded.
   * @param upperBound  At least the highest value that will be encoded.
   *                For space efficiency this should not exceed the power of two that equals
   *                or is the first higher than the actual maximum.
   *                <br>When {@code numValues >= (upperBound/3)}
   *                a {@link FixedBitSet} will take less space.
   * @param indexInterval The number of high zero bits for which a single index entry is built.
   *                The index will have at most <code>2 * numValues / indexInterval</code> entries
   *                and each index entry will use at most <code>ceil(log2(3 * numValues))</code> bits,
   *                see {@link EliasFanoEncoder}.
   * @throws IllegalArgumentException when:
   *         <ul>
   *         <li><code>numValues</code> is negative, or
   *         <li><code>numValues</code> is non negative and <code>upperBound</code> is negative, or
   *         <li>the low bits do not fit in a <code>long[]</code>:
   *             {@code (L * numValues / 64) > Integer.MAX_VALUE}, or
   *         <li>the high bits do not fit in a <code>long[]</code>:
   *             {@code (2 * numValues / 64) > Integer.MAX_VALUE}, or
   *         <li>{@code indexInterval < 2},
   *         <li>the index bits do not fit in a <code>long[]</code>:
   *             {@code (numValues / indexInterval * ceil(2log(3 * numValues)) / 64) > Integer.MAX_VALUE}.
   *         </ul>
   */
  public EliasFanoEncoder(long numValues, long upperBound, long indexInterval) {
    if (numValues < 0L) {
      throw new IllegalArgumentException("numValues should not be negative: " + numValues);
    }
    this.numValues = numValues;
    if ((numValues > 0L) && (upperBound < 0L)) {
      throw new IllegalArgumentException("upperBound should not be negative: " + upperBound + " when numValues > 0");
    }
    this.upperBound = numValues > 0 ? upperBound : -1L; // if there is no value, -1 is the best upper bound
    int nLowBits = 0;
    if (this.numValues > 0) { // nLowBits = max(0; floor(2log(upperBound/numValues)))
      long lowBitsFac = this.upperBound / this.numValues;
      if (lowBitsFac > 0) {
        nLowBits = 63 - Long.numberOfLeadingZeros(lowBitsFac); // see Long.numberOfLeadingZeros javadocs
      }
    }
    this.numLowBits = nLowBits;
    this.lowerBitsMask = Long.MAX_VALUE >>> (Long.SIZE - 1 - this.numLowBits);

    long numLongsForLowBits = numLongsForBits(numValues * numLowBits);
    if (numLongsForLowBits > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("numLongsForLowBits too large to index a long array: " + numLongsForLowBits);
    }
    this.lowerLongs = new long[(int) numLongsForLowBits];

    long numHighBitsClear = ((this.upperBound > 0) ? this.upperBound : 0) >>> this.numLowBits;
    assert numHighBitsClear <= (2 * this.numValues);
    long numHighBitsSet = this.numValues;

    long numLongsForHighBits = numLongsForBits(numHighBitsClear + numHighBitsSet);
    if (numLongsForHighBits > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("numLongsForHighBits too large to index a long array: " + numLongsForHighBits);
    }
    this.upperLongs = new long[(int) numLongsForHighBits];
    if (indexInterval < 2) {
      throw new IllegalArgumentException("indexInterval should at least 2: " + indexInterval);
    }
    // For the index:
    long maxHighValue = upperBound >>> this.numLowBits;
    long nIndexEntries = maxHighValue / indexInterval; // no zero value index entry
    this.numIndexEntries = (nIndexEntries >= 0) ? nIndexEntries : 0;
    long maxIndexEntry = maxHighValue + numValues - 1; // clear upper bits, set upper bits, start at zero
    this.nIndexEntryBits = (maxIndexEntry <= 0) ? 0
                          : (64 - Long.numberOfLeadingZeros(maxIndexEntry));
    long numLongsForIndexBits = numLongsForBits(numIndexEntries * nIndexEntryBits);
    if (numLongsForIndexBits > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("numLongsForIndexBits too large to index a long array: " + numLongsForIndexBits);
    }
    this.upperZeroBitPositionIndex = new long[(int) numLongsForIndexBits];
    this.currentEntryIndex = 0;
    this.indexInterval = indexInterval;
  }

  /**
  * Construct an Elias-Fano encoder using {@link #DEFAULT_INDEX_INTERVAL}.
  */
  public EliasFanoEncoder(long numValues, long upperBound) {
    this(numValues, upperBound, DEFAULT_INDEX_INTERVAL);
  }

  private static long numLongsForBits(long numBits) { // Note: int version in FixedBitSet.bits2words()
    assert numBits >= 0 : numBits;
    return (numBits + (Long.SIZE-1)) >>> LOG2_LONG_SIZE;
  }

  /** Call at most <code>numValues</code> times to encode a non decreasing sequence of non negative numbers.
   * @param x The next number to be encoded.
   * @throws IllegalStateException when called more than <code>numValues</code> times.
   * @throws IllegalArgumentException when:
   *         <ul>
   *         <li><code>x</code> is smaller than an earlier encoded value, or
   *         <li><code>x</code> is larger than <code>upperBound</code>.
   *         </ul>
   */
  public void encodeNext(long x) {
    if (numEncoded >= numValues) {
      throw new IllegalStateException("encodeNext called more than " + numValues + " times.");
    }
    if (lastEncoded > x) {
      throw new IllegalArgumentException(x + " smaller than previous " + lastEncoded);
    }
    if (x > upperBound) {
      throw new IllegalArgumentException(x + " larger than upperBound " + upperBound);
    }
    long highValue = x >>> numLowBits;
    encodeUpperBits(highValue);
    encodeLowerBits(x & lowerBitsMask);
    lastEncoded = x;
    // Add index entries:
    long indexValue = (currentEntryIndex + 1) * indexInterval;
    while (indexValue <= highValue) { 
      long afterZeroBitPosition = indexValue + numEncoded;
      packValue(afterZeroBitPosition, upperZeroBitPositionIndex, nIndexEntryBits, currentEntryIndex);
      currentEntryIndex += 1;
      indexValue += indexInterval;
    }
    numEncoded++;
  }

  private void encodeUpperBits(long highValue) {
    long nextHighBitNum = numEncoded + highValue; // sequence of unary gaps
    upperLongs[(int)(nextHighBitNum >>> LOG2_LONG_SIZE)] |= (1L << (nextHighBitNum & (Long.SIZE-1)));
  }

  private void encodeLowerBits(long lowValue) {
    packValue(lowValue, lowerLongs, numLowBits, numEncoded);
  }

  private static void packValue(long value, long[] longArray, int numBits, long packIndex) {
    if (numBits != 0) {
      long bitPos = numBits * packIndex;
      int index = (int) (bitPos >>> LOG2_LONG_SIZE);
      int bitPosAtIndex = (int) (bitPos & (Long.SIZE-1));
      longArray[index] |= (value << bitPosAtIndex);
      if ((bitPosAtIndex + numBits) > Long.SIZE) {
        longArray[index+1] = (value >>> (Long.SIZE - bitPosAtIndex));
      }
    }
  }

  /** Provide an indication that it is better to use an {@link EliasFanoEncoder} than a {@link FixedBitSet}
   *  to encode document identifiers.
   *  This indication is not precise and may change in the future.
   *  <br>An EliasFanoEncoder is favoured when the size of the encoding by the EliasFanoEncoder
   *  (including some space for its index) is at most about 5/6 of the size of the FixedBitSet,
   *  this is the same as comparing estimates of the number of bits accessed by a pair of FixedBitSets and
   *  by a pair of non indexed EliasFanoDocIdSets when determining the intersections of the pairs.
   *  <br>A bit set is preferred when {@code upperbound <= 256}.
   *  <br>It is assumed that {@link #DEFAULT_INDEX_INTERVAL} is used.
   *  @param numValues The number of document identifiers that is to be encoded. Should be non negative.
   *  @param upperBound The maximum possible value for a document identifier. Should be at least <code>numValues</code>.
   */
  public static boolean sufficientlySmallerThanBitSet(long numValues, long upperBound) {
    /* When (upperBound / 6) == numValues,
     * the number of bits per entry for the EliasFanoEncoder is 2 + ceil(2log(upperBound/numValues)) == 5.
     *
     * For intersecting two bit sets upperBound bits are accessed, roughly half of one, half of the other.
     * For intersecting two EliasFano sequences without index on the upper bits,
     * all (2 * 3 * numValues) upper bits are accessed.
     */
    return (upperBound > (4 * Long.SIZE)) // prefer a bit set when it takes no more than 4 longs.
            && (upperBound / 7) > numValues; // 6 + 1 to allow some room for the index.
  }

  /**
   * Returns an {@link EliasFanoDecoder} to access the encoded values.
   * Perform all calls to {@link #encodeNext} before calling {@link #getDecoder}.
   */
  public EliasFanoDecoder getDecoder() {
    // decode as far as currently encoded as determined by numEncoded.
    return new EliasFanoDecoder(this);
  }

  /** Expert. The low bits. */
  public long[] getLowerBits() {
    return lowerLongs;
  }

  /** Expert. The high bits. */
  public long[] getUpperBits() {
    return upperLongs;
  }
  
  /** Expert. The index bits. */
  public long[] getIndexBits() {
    return upperZeroBitPositionIndex;
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder("EliasFanoSequence");
    s.append(" numValues " + numValues);
    s.append(" numEncoded " + numEncoded);
    s.append(" upperBound " + upperBound);
    s.append(" lastEncoded " + lastEncoded);
    s.append(" numLowBits " + numLowBits);
    s.append("\nupperLongs[" + upperLongs.length + "]");
    for (int i = 0; i < upperLongs.length; i++) {
      s.append(" " + ToStringUtils.longHex(upperLongs[i]));
    }
    s.append("\nlowerLongs[" + lowerLongs.length + "]");
    for (int i = 0; i < lowerLongs.length; i++) {
      s.append(" " + ToStringUtils.longHex(lowerLongs[i]));
    }
    s.append("\nindexInterval: " + indexInterval + ", nIndexEntryBits: " + nIndexEntryBits);
    s.append("\nupperZeroBitPositionIndex[" + upperZeroBitPositionIndex.length + "]");
    for (int i = 0; i < upperZeroBitPositionIndex.length; i++) { 
      s.append(" " + ToStringUtils.longHex(upperZeroBitPositionIndex[i]));
    }
    return s.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (! (other instanceof EliasFanoEncoder)) {
      return false;
    }
    EliasFanoEncoder oefs = (EliasFanoEncoder) other;
    // no equality needed for upperBound
    return (this.numValues == oefs.numValues)
        && (this.numEncoded == oefs.numEncoded)
        && (this.numLowBits == oefs.numLowBits)
        && (this.numIndexEntries == oefs.numIndexEntries)
        && (this.indexInterval == oefs.indexInterval) // no need to check index content
        && Arrays.equals(this.upperLongs, oefs.upperLongs)
        && Arrays.equals(this.lowerLongs, oefs.lowerLongs);
  }

  @Override
  public int hashCode() {
    int h = ((int) (31*(numValues + 7*(numEncoded + 5*(numLowBits + 3*(numIndexEntries + 11*indexInterval))))))
            ^ Arrays.hashCode(upperLongs)
            ^ Arrays.hashCode(lowerLongs);
    return h;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED
        + RamUsageEstimator.sizeOf(lowerLongs)
        + RamUsageEstimator.sizeOf(upperLongs)
        + RamUsageEstimator.sizeOf(upperZeroBitPositionIndex);
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}

