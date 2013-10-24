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

import org.apache.lucene.util.BroadWord; // bit selection in long


/** A decoder for an {@link EliasFanoEncoder}.
 * @lucene.internal
 */
public class EliasFanoDecoder {
  private static final int LOG2_LONG_SIZE = Long.numberOfTrailingZeros(Long.SIZE);

  private final EliasFanoEncoder efEncoder;
  private final long numEncoded;
  private long efIndex = -1; // the decoding index.
  private long setBitForIndex = -1; // the index of the high bit at the decoding index.

  public final static long NO_MORE_VALUES = -1L;

  private final long numIndexEntries;
  private final long indexMask;

  /** Construct a decoder for a given {@link EliasFanoEncoder}.
   * The decoding index is set to just before the first encoded value.
   */
  public EliasFanoDecoder(EliasFanoEncoder efEncoder) {
    this.efEncoder = efEncoder;
    this.numEncoded = efEncoder.numEncoded; // not final in EliasFanoEncoder
    this.numIndexEntries = efEncoder.currentEntryIndex;  // not final in EliasFanoEncoder
    this.indexMask = (1L << efEncoder.nIndexEntryBits) - 1;
  }

  /** @return The Elias-Fano encoder that is decoded. */
  public EliasFanoEncoder getEliasFanoEncoder() {
    return efEncoder;
  }
  
  /** The number of values encoded by the encoder.
   * @return The number of values encoded by the encoder.
   */
  public long numEncoded() { 
    return numEncoded;
  }


  /** The current decoding index.
   * The first value encoded by {@link EliasFanoEncoder#encodeNext} has index 0.
   * Only valid directly after
   * {@link #nextValue}, {@link #advanceToValue},
   * {@link #previousValue}, or {@link #backToValue}
   * returned another value than {@link #NO_MORE_VALUES},
   * or {@link #advanceToIndex} returned true.
   * @return The decoding index of the last decoded value, or as last set by {@link #advanceToIndex}.
   */
  public long currentIndex() {
    if (efIndex < 0) {
      throw new IllegalStateException("index before sequence");
    }
    if (efIndex >= numEncoded) {
      throw new IllegalStateException("index after sequence");
    }
    return efIndex;
  }

  /** The value at the current decoding index.
   * Only valid when {@link #currentIndex} would return a valid result.
   * <br>This is only intended for use after {@link #advanceToIndex} returned true.
   * @return The value encoded at {@link #currentIndex}.
   */
  public long currentValue() {
    return combineHighLowValues(currentHighValue(), currentLowValue());
  }

  /**  @return The high value for the current decoding index. */
  private long currentHighValue() {
    return setBitForIndex - efIndex; // sequence of unary gaps
  }

  /** See also {@link EliasFanoEncoder#packValue} */
  private static long unPackValue(long[] longArray, int numBits, long packIndex, long bitsMask) {
    if (numBits == 0) {
      return 0;
    }
    long bitPos = packIndex * numBits;
    int index = (int) (bitPos >>> LOG2_LONG_SIZE);
    int bitPosAtIndex = (int) (bitPos & (Long.SIZE-1));
    long value = longArray[index] >>> bitPosAtIndex;
    if ((bitPosAtIndex + numBits) > Long.SIZE) {
      value |= (longArray[index + 1] << (Long.SIZE - bitPosAtIndex));
    }
    value &= bitsMask;
    return value;
  }

  /**  @return The low value for the current decoding index. */
  private long currentLowValue() {
    assert ((efIndex >= 0) && (efIndex < numEncoded)) : "efIndex " + efIndex;
    return unPackValue(efEncoder.lowerLongs, efEncoder.numLowBits, efIndex, efEncoder.lowerBitsMask);
  }

  /**  @return The given highValue shifted left by the number of low bits from by the EliasFanoSequence,
   *           logically OR-ed with the given lowValue.
   */
  private long combineHighLowValues(long highValue, long lowValue) {
    return (highValue << efEncoder.numLowBits) | lowValue;
  }

  private long curHighLong;


  /* The implementation of forward decoding and backward decoding is done by the following method pairs.
   *
   * toBeforeSequence - toAfterSequence
   * getCurrentRightShift - getCurrentLeftShift
   * toAfterCurrentHighBit - toBeforeCurrentHighBit
   * toNextHighLong - toPreviousHighLong
   * nextHighValue - previousHighValue
   * nextValue - previousValue
   * advanceToValue - backToValue
   *
   */

  /* Forward decoding section */


  /** Set the decoding index to just before the first encoded value.
   */
  public void toBeforeSequence() {
    efIndex = -1;
    setBitForIndex = -1;
  }

  /** @return the number of bits in a long after (setBitForIndex modulo Long.SIZE) */
  private int getCurrentRightShift() {
    int s = (int) (setBitForIndex & (Long.SIZE-1));
    return s;
  }

  /** Increment efIndex and setBitForIndex and
   * shift curHighLong so that it does not contain the high bits before setBitForIndex.
   * @return true iff efIndex still smaller than numEncoded.
   */
  private boolean toAfterCurrentHighBit() {
    efIndex += 1;
    if (efIndex >= numEncoded) {
      return false;
    }
    setBitForIndex += 1;
    int highIndex = (int)(setBitForIndex >>> LOG2_LONG_SIZE);
    curHighLong = efEncoder.upperLongs[highIndex] >>> getCurrentRightShift();
    return true;
  }

  /** The current high long has been determined to not contain the set bit that is needed.
   *  Increment setBitForIndex to the next high long and set curHighLong accordingly.
   */
  private void toNextHighLong() {
    setBitForIndex += Long.SIZE - (setBitForIndex & (Long.SIZE-1));
    //assert getCurrentRightShift() == 0;
    int highIndex = (int)(setBitForIndex >>> LOG2_LONG_SIZE);
    curHighLong = efEncoder.upperLongs[highIndex];
  }

  /** setBitForIndex and efIndex have just been incremented, scan to the next high set bit
   *  by incrementing setBitForIndex, and by setting curHighLong accordingly.
   */
  private void toNextHighValue() {
    while (curHighLong == 0L) {
      toNextHighLong(); // inlining and unrolling would simplify somewhat
    }
    setBitForIndex += Long.numberOfTrailingZeros(curHighLong);
  }

  /** setBitForIndex and efIndex have just been incremented, scan to the next high set bit
   *  by incrementing setBitForIndex, and by setting curHighLong accordingly.
   *  @return the next encoded high value.
   */
  private long nextHighValue() {
    toNextHighValue();
    return currentHighValue();
  }

  /** If another value is available after the current decoding index, return this value and
   * and increase the decoding index by 1. Otherwise return {@link #NO_MORE_VALUES}.
   */
  public long nextValue() {
    if (! toAfterCurrentHighBit()) {
      return NO_MORE_VALUES;
    }
    long highValue = nextHighValue();
    return combineHighLowValues(highValue, currentLowValue());
  }

  /** Advance the decoding index to a given index.
   * and return <code>true</code> iff it is available.
   * <br>See also {@link #currentValue}.
   * <br>The current implementation does not use the index on the upper bit zero bit positions.
   * <br>Note: there is currently no implementation of <code>backToIndex</code>.
   */
  public boolean advanceToIndex(long index) {
    assert index > efIndex;
    if (index >= numEncoded) {
      efIndex = numEncoded;
      return false;
    }
    if (! toAfterCurrentHighBit()) {
      assert false;
    }
    /* CHECKME: Add a (binary) search in the upperZeroBitPositions here. */
    int curSetBits = Long.bitCount(curHighLong);
    while ((efIndex + curSetBits) < index) { // curHighLong has not enough set bits to reach index
      efIndex += curSetBits;
      toNextHighLong();
      curSetBits = Long.bitCount(curHighLong);
    }
    // curHighLong has enough set bits to reach index
    while (efIndex < index) {
      /* CHECKME: Instead of the linear search here, use (forward) broadword selection from
       * "Broadword Implementation of Rank/Select Queries", Sebastiano Vigna, January 30, 2012.
       */
      if (! toAfterCurrentHighBit()) {
        assert false;
      }
      toNextHighValue();
    }
    return true;
  }



  /** Given a target value, advance the decoding index to the first bigger or equal value
   * and return it if it is available. Otherwise return {@link #NO_MORE_VALUES}.
   * <br>The current implementation uses the index on the upper zero bit positions.
   */
  public long advanceToValue(long target) {
    efIndex += 1;
    if (efIndex >= numEncoded) {
      return NO_MORE_VALUES;
    }
    setBitForIndex += 1; // the high bit at setBitForIndex belongs to the unary code for efIndex

    int highIndex = (int)(setBitForIndex >>> LOG2_LONG_SIZE);
    long upperLong = efEncoder.upperLongs[highIndex];
    curHighLong = upperLong >>> ((int) (setBitForIndex & (Long.SIZE-1))); // may contain the unary 1 bit for efIndex

    // determine index entry to advance to
    long highTarget = target >>> efEncoder.numLowBits;

    long indexEntryIndex = (highTarget / efEncoder.indexInterval) - 1;
    if (indexEntryIndex >= 0) { // not before first index entry
      if (indexEntryIndex >= numIndexEntries) {
        indexEntryIndex = numIndexEntries - 1; // no further than last index entry
      }
      long indexHighValue = (indexEntryIndex + 1) * efEncoder.indexInterval;
      assert indexHighValue <= highTarget;
      if (indexHighValue > (setBitForIndex - efIndex)) { // advance to just after zero bit position of index entry.
        setBitForIndex = unPackValue(efEncoder.upperZeroBitPositionIndex, efEncoder.nIndexEntryBits, indexEntryIndex, indexMask);
        efIndex = setBitForIndex - indexHighValue; // the high bit at setBitForIndex belongs to the unary code for efIndex
        highIndex = (int)(setBitForIndex >>> LOG2_LONG_SIZE);
        upperLong = efEncoder.upperLongs[highIndex];
        curHighLong = upperLong >>> ((int) (setBitForIndex & (Long.SIZE-1))); // may contain the unary 1 bit for efIndex
      }
      assert efIndex < numEncoded; // there is a high value to be found.
    }

    int curSetBits = Long.bitCount(curHighLong); // shifted right.
    int curClearBits = Long.SIZE - curSetBits - ((int) (setBitForIndex & (Long.SIZE-1))); // subtract right shift, may be more than encoded

    while (((setBitForIndex - efIndex) + curClearBits) < highTarget) {
      // curHighLong has not enough clear bits to reach highTarget
      efIndex += curSetBits;
      if (efIndex >= numEncoded) {
        return NO_MORE_VALUES;
      }
      setBitForIndex += Long.SIZE - (setBitForIndex & (Long.SIZE-1));
      // highIndex = (int)(setBitForIndex >>> LOG2_LONG_SIZE);
      assert (highIndex + 1) == (int)(setBitForIndex >>> LOG2_LONG_SIZE);
      highIndex += 1;
      upperLong = efEncoder.upperLongs[highIndex];
      curHighLong = upperLong;
      curSetBits = Long.bitCount(curHighLong);
      curClearBits = Long.SIZE - curSetBits;
    }
    // curHighLong has enough clear bits to reach highTarget, and may not have enough set bits.
    while (curHighLong == 0L) {
      setBitForIndex += Long.SIZE - (setBitForIndex & (Long.SIZE-1));
      assert (highIndex + 1) == (int)(setBitForIndex >>> LOG2_LONG_SIZE);
      highIndex += 1;
      upperLong = efEncoder.upperLongs[highIndex];
      curHighLong = upperLong;
    }

    // curHighLong has enough clear bits to reach highTarget, has at least 1 set bit, and may not have enough set bits.
    int rank = (int) (highTarget - (setBitForIndex - efIndex)); // the rank of the zero bit for highValue.
    assert (rank <= Long.SIZE) : ("rank " + rank);
    if (rank >= 1) {
      long invCurHighLong = ~curHighLong;
      int clearBitForValue = (rank <= 8)
                              ? BroadWord.selectNaive(invCurHighLong, rank)
                              : BroadWord.select(invCurHighLong, rank);
      assert clearBitForValue <= (Long.SIZE-1);
      setBitForIndex += clearBitForValue + 1; // the high bit just before setBitForIndex is zero
      int oneBitsBeforeClearBit = clearBitForValue - rank + 1;
      efIndex += oneBitsBeforeClearBit; // the high bit at setBitForIndex and belongs to the unary code for efIndex
      if (efIndex >= numEncoded) {
        return NO_MORE_VALUES;
      }

      if ((setBitForIndex & (Long.SIZE - 1)) == 0L) { // exhausted curHighLong
        assert (highIndex + 1) == (int)(setBitForIndex >>> LOG2_LONG_SIZE);
        highIndex += 1;
        upperLong = efEncoder.upperLongs[highIndex];
        curHighLong = upperLong;
      }
      else {
        assert highIndex == (int)(setBitForIndex >>> LOG2_LONG_SIZE);
        curHighLong = upperLong >>> ((int) (setBitForIndex & (Long.SIZE-1)));
      }
      // curHighLong has enough clear bits to reach highTarget, and may not have enough set bits.
 
      while (curHighLong == 0L) {
        setBitForIndex += Long.SIZE - (setBitForIndex & (Long.SIZE-1));
        assert (highIndex + 1) == (int)(setBitForIndex >>> LOG2_LONG_SIZE);
        highIndex += 1;
        upperLong = efEncoder.upperLongs[highIndex];
        curHighLong = upperLong;
      }
    }
    setBitForIndex += Long.numberOfTrailingZeros(curHighLong);
    assert (setBitForIndex - efIndex) >= highTarget; // highTarget reached

    // Linear search also with low values
    long currentValue = combineHighLowValues((setBitForIndex - efIndex), currentLowValue());
    while (currentValue < target) {
      currentValue = nextValue();
      if (currentValue == NO_MORE_VALUES) {
        return NO_MORE_VALUES;
      }
    }
    return currentValue;
  }


  /* Backward decoding section */

  /** Set the decoding index to just after the last encoded value.
   */
  public void toAfterSequence() {
    efIndex = numEncoded; // just after last index
    setBitForIndex = (efEncoder.lastEncoded >>> efEncoder.numLowBits) + numEncoded;
  }

  /** @return the number of bits in a long before (setBitForIndex modulo Long.SIZE) */
  private int getCurrentLeftShift() {
    int s = Long.SIZE - 1 - (int) (setBitForIndex & (Long.SIZE-1));
    return s;
  }

  /** Decrement efindex and setBitForIndex and
   * shift curHighLong so that it does not contain the high bits after setBitForIndex.
   * @return true iff efindex still >= 0
   */
  private boolean toBeforeCurrentHighBit() {
    efIndex -= 1;
    if (efIndex < 0) {
      return false;
    }
    setBitForIndex -= 1;
    int highIndex = (int)(setBitForIndex >>> LOG2_LONG_SIZE);
    curHighLong = efEncoder.upperLongs[highIndex] << getCurrentLeftShift();
    return true;
  }

  /** The current high long has been determined to not contain the set bit that is needed.
   *  Decrement setBitForIndex to the previous high long and set curHighLong accordingly.
   */
  private void toPreviousHighLong() {
    setBitForIndex -= (setBitForIndex & (Long.SIZE-1)) + 1;
    //assert getCurrentLeftShift() == 0;
    int highIndex = (int)(setBitForIndex >>> LOG2_LONG_SIZE);
    curHighLong = efEncoder.upperLongs[highIndex];
  }

  /** setBitForIndex and efIndex have just been decremented, scan to the previous high set bit
   *  by decrementing setBitForIndex and by setting curHighLong accordingly.
   *  @return the previous encoded high value.
   */
  private long previousHighValue() {
    while (curHighLong == 0L) {
      toPreviousHighLong(); // inlining and unrolling would simplify somewhat
    }
    setBitForIndex -= Long.numberOfLeadingZeros(curHighLong);
    return currentHighValue();
  }

  /** If another value is available before the current decoding index, return this value
   * and decrease the decoding index by 1. Otherwise return {@link #NO_MORE_VALUES}.
   */
  public long previousValue() {
    if (! toBeforeCurrentHighBit()) {
      return NO_MORE_VALUES;
    }
    long highValue = previousHighValue();
    return combineHighLowValues(highValue, currentLowValue());
  }


  /** setBitForIndex and efIndex have just been decremented, scan backward to the high set bit
   *  of at most a given high value
   *  by decrementing setBitForIndex and by setting curHighLong accordingly.
   * <br>The current implementation does not use the index on the upper zero bit positions.
   *  @return the largest encoded high value that is at most the given one.
   */
  private long backToHighValue(long highTarget) {
    /* CHECKME: Add using the index as in advanceToHighValue */
    int curSetBits = Long.bitCount(curHighLong); // is shifted by getCurrentLeftShift()
    int curClearBits = Long.SIZE - curSetBits - getCurrentLeftShift();
    while ((currentHighValue() - curClearBits) > highTarget) {
      // curHighLong has not enough clear bits to reach highTarget
      efIndex -= curSetBits;
      if (efIndex < 0) {
        return NO_MORE_VALUES;
      }
      toPreviousHighLong();
      //assert getCurrentLeftShift() == 0;
      curSetBits = Long.bitCount(curHighLong);
      curClearBits = Long.SIZE - curSetBits;
    }
    // curHighLong has enough clear bits to reach highTarget, but may not have enough set bits.
    long highValue = previousHighValue();
    while (highValue > highTarget) {
      /* CHECKME: See at advanceToHighValue on using broadword bit selection. */
      if (! toBeforeCurrentHighBit()) {
        return NO_MORE_VALUES;
      }
      highValue = previousHighValue();
    }
    return highValue;
  }

  /** Given a target value, go back to the first smaller or equal value
   * and return it if it is available. Otherwise return {@link #NO_MORE_VALUES}.
   * <br>The current implementation does not use the index on the upper zero bit positions.
   */
  public long backToValue(long target) {
    if (! toBeforeCurrentHighBit()) {
      return NO_MORE_VALUES;
    }
    long highTarget = target >>> efEncoder.numLowBits;
    long highValue = backToHighValue(highTarget);
    if (highValue == NO_MORE_VALUES) {
      return NO_MORE_VALUES;
    }
    // Linear search with low values:
    long currentValue = combineHighLowValues(highValue, currentLowValue());
    while (currentValue > target) {
      currentValue = previousValue();
      if (currentValue == NO_MORE_VALUES) {
        return NO_MORE_VALUES;
      }
    }
    return currentValue;
  }
}

