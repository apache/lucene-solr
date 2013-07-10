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

/** A decoder for an {@link EliasFanoEncoder}.
 * @lucene.internal
 */
public class EliasFanoDecoder {
  private static final int LOG2_LONG_SIZE = Long.numberOfTrailingZeros(Long.SIZE);

  private final EliasFanoEncoder efEncoder;
  final long numEncoded;
  private long efIndex = -1; // the decoding index.
  private long setBitForIndex = -1; // the index of the high bit at the decoding index.

  public final static long NO_MORE_VALUES = -1L;

  /** Construct a decoder for a given {@link EliasFanoEncoder}.
   * The decoding index is set to just before the first encoded value.
   */
  public EliasFanoDecoder(EliasFanoEncoder efEncoder) {
    this.efEncoder = efEncoder;
    this.numEncoded = efEncoder.numEncoded; // numEncoded is not final in EliasFanoEncoder
  }

  /** Return the Elias-Fano encoder that is decoded. */
  public EliasFanoEncoder getEliasFanoEncoder() {
    return efEncoder;
  }


  /** Return the index of the last decoded value.
   * The first value encoded by {@link EliasFanoEncoder#encodeNext} has index 0.
   * Only valid directly after
   * {@link #nextValue}, {@link #advanceToValue},
   * {@link #previousValue}, or {@link #backToValue}
   * returned another value than {@link #NO_MORE_VALUES}.
   */
  public long index() {
    if (efIndex < 0) {
      throw new IllegalStateException("index before sequence");
    }
    if (efIndex >= numEncoded) {
      throw new IllegalStateException("index after sequence");
    }
    return efIndex;
  }

  /** Return the high value for the current decoding index. */
  private long currentHighValue() {
    return setBitForIndex - efIndex; // sequence of unary gaps
  }

  /**  Return the low value for the current decoding index. */
  private long currentLowValue() {
    assert efIndex >= 0;
    assert efIndex < numEncoded;
    if (efEncoder.numLowBits == 0) {
      return 0;
    }
    long bitPos = efIndex * efEncoder.numLowBits;
    int lowIndex = (int) (bitPos >>> LOG2_LONG_SIZE);
    int bitPosAtIndex = (int) (bitPos & (Long.SIZE-1));
    long lowValue = efEncoder.lowerLongs[lowIndex] >>> bitPosAtIndex;
    if ((bitPosAtIndex + efEncoder.numLowBits) > Long.SIZE) {
      lowValue |= (efEncoder.lowerLongs[lowIndex + 1] << (Long.SIZE - bitPosAtIndex));
    }
    lowValue &= efEncoder.lowerBitsMask;
    return lowValue;
  }

  /**  Return the given highValue shifted left by the number of low bits from by the EliasFanoSequence,
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

  /** Return the number of bits in a long after (setBitForIndex modulo Long.SIZE) */
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


  /** setBitForIndex and efIndex have just been incremented, scan forward to the high set bit
   *  of at least a given high value
   *  by incrementing setBitForIndex, and by setting curHighLong accordingly.
   *  @return the smallest encoded high value that is at least the given one.
   */
  private long advanceToHighValue(long highTarget) {
    int curSetBits = Long.bitCount(curHighLong); // is shifted by getCurrentRightShift()
    int curClearBits = Long.SIZE - curSetBits - getCurrentRightShift();
    while ((currentHighValue() + curClearBits) < highTarget) {
      // curHighLong has not enough clear bits to reach highTarget
      efIndex += curSetBits;
      if (efIndex >= numEncoded) {
        return NO_MORE_VALUES;
      }
      toNextHighLong();
      // assert getCurrentRightShift() == 0;
      curSetBits = Long.bitCount(curHighLong);
      curClearBits = Long.SIZE - curSetBits;
    }
    // curHighLong has enough clear bits to reach highTarget, but may not have enough set bits.
    long highValue = nextHighValue();
    while (highValue < highTarget) {
      /* CHECKME: Instead of the linear search here, use (forward) broadword selection from
       * "Broadword Implementation of Rank/Select Queries", Sebastiano Vigna, January 30, 2012.
       */
      if (! toAfterCurrentHighBit()) {
        return NO_MORE_VALUES;
      }
      highValue = nextHighValue();
    }
    return highValue;
  }

  /** Given a target value, advance the decoding index to the first bigger or equal value
   * and return it if it is available. Otherwise return {@link #NO_MORE_VALUES}.
   */
  public long advanceToValue(long target) {
    if (! toAfterCurrentHighBit()) {
      return NO_MORE_VALUES;
    }
    long highTarget = target >>> efEncoder.numLowBits;
    long highValue = advanceToHighValue(highTarget);
    if (highValue == NO_MORE_VALUES) {
      return NO_MORE_VALUES;
    }
    // Linear search with low values:
    long currentValue = combineHighLowValues(highValue, currentLowValue());
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

  /** Return the number of bits in a long before (setBitForIndex modulo Long.SIZE) */
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

  /** If another value is available before the current decoding index, return this value and
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
   *  @return the largest encoded high value that is at most the given one.
   */
  private long backToHighValue(long highTarget) {
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
      /* CHECKME: See at advanceToHighValue. */
      if (! toBeforeCurrentHighBit()) {
        return NO_MORE_VALUES;
      }
      highValue = previousHighValue();
    }
    return highValue;
  }

  /** Given a target value, go back to the first smaller or equal value
   * and return it if it is available. Otherwise return {@link #NO_MORE_VALUES}.
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

