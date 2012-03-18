package org.apache.lucene.util.packed;

/**
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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Space optimized random access capable array of values with a fixed number of
 * bits. For 32 bits/value and less, performance on 32 bit machines is not
 * optimal. Consider using {@link Packed32} for such a setup.
 * </p><p>
 * The implementation strives to avoid conditionals and expensive operations,
 * sacrificing code clarity to achieve better performance.
 */

class Packed64 extends PackedInts.ReaderImpl implements PackedInts.Mutable {
  static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
  static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
  static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

  private static final int ENTRY_SIZE = BLOCK_SIZE + 1;
  static final int FAC_BITPOS = 3;

  /*
   * In order to make an efficient value-getter, conditionals should be
   * avoided. A value can be positioned inside of a block, requiring shifting
   * left or right or it can span two blocks, requiring a left-shift on the
   * first block and a right-shift on the right block.
   * </p><p>
   * By always shifting the first block both left and right, we get exactly
   * the right bits. By always shifting the second block right and applying
   * a mask, we get the right bits there. After that, we | the two bitsets.
  */
  static final int[][] SHIFTS =
    new int[ENTRY_SIZE][ENTRY_SIZE * FAC_BITPOS];
  static final long[][] MASKS = new long[ENTRY_SIZE][ENTRY_SIZE];

  static { // Generate shifts
      for (int elementBits = 1 ; elementBits <= BLOCK_SIZE ; elementBits++) {
          for (int bitPos = 0 ; bitPos < BLOCK_SIZE ; bitPos++) {
              int[] currentShifts = SHIFTS[elementBits];
              int base = bitPos * FAC_BITPOS;
              currentShifts[base    ] = bitPos;
              currentShifts[base + 1] = BLOCK_SIZE - elementBits;
              if (bitPos <= BLOCK_SIZE - elementBits) { // Single block
                  currentShifts[base + 2] = 0;
                  MASKS[elementBits][bitPos] = 0;
              } else { // Two blocks
                  int rBits = elementBits - (BLOCK_SIZE - bitPos);
                  currentShifts[base + 2] = BLOCK_SIZE - rBits;
                  MASKS[elementBits][bitPos] = ~(~0L << rBits);
              }
          }
      }
  }

  /*
   * The setter requires more masking than the getter.
  */
  private static final long[][] WRITE_MASKS =
          new long[ENTRY_SIZE][ENTRY_SIZE * FAC_BITPOS];
  static {
      for (int elementBits = 1 ; elementBits <= BLOCK_SIZE ; elementBits++) {
          long elementPosMask = ~(~0L << elementBits);
          int[] currentShifts = SHIFTS[elementBits];
          long[] currentMasks = WRITE_MASKS[elementBits];
          for (int bitPos = 0 ; bitPos < BLOCK_SIZE ; bitPos++) {
              int base = bitPos * FAC_BITPOS;
              currentMasks[base  ] =~((elementPosMask
                                 << currentShifts[base + 1])
                                >>> currentShifts[base]);
              if (bitPos <= BLOCK_SIZE - elementBits) { // Second block not used
                currentMasks[base+1] = ~0; // Keep all bits
                currentMasks[base+2] = 0;  // Or with 0
              } else {
                currentMasks[base+1] = ~(elementPosMask
                                         << currentShifts[base + 2]);
                currentMasks[base+2] = currentShifts[base + 2] == 0 ? 0 : ~0;
              }
          }
      }
  }

  /* The bits */
  private long[] blocks;

  // Cached calculations
  private int maxPos;      // blocks.length * BLOCK_SIZE / elementBits - 1
  private int[] shifts;    // The shifts for the current elementBits
  private long[] readMasks;
  private long[] writeMasks;

  /**
   * Creates an array with the internal structures adjusted for the given
   * limits and initialized to 0.
   * @param valueCount   the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   */
  public Packed64(int valueCount, int bitsPerValue) {
    // TODO: Test for edge-cases (2^31 values, 63 bitsPerValue)
    // +2 due to the avoid-conditionals-trick. The last entry is always 0
    this(new long[(int)((long)valueCount * bitsPerValue / BLOCK_SIZE + 2)],
            valueCount, bitsPerValue);
  }


  /**
   * Creates an array backed by the given blocks.
   * </p><p>
   * Note: The blocks are used directly, so changes to the given block will
   * affect the Packed32-structure.
   * @param blocks   used as the internal backing array. Not that the last
   *                 element cannot be addressed directly.
   * @param valueCount the number of values.
   * @param bitsPerValue the number of bits available for any given value.
   */
  public Packed64(long[] blocks, int valueCount, int bitsPerValue) {
    super(valueCount, bitsPerValue);
    this.blocks = blocks;
    updateCached();
  }

  /**
   * Creates an array with content retrieved from the given DataInput.
   * @param in       a DataInput, positioned at the start of Packed64-content.
   * @param valueCount  the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   * @throws java.io.IOException if the values for the backing array could not
   *                             be retrieved.
   */
  public Packed64(DataInput in, int valueCount, int bitsPerValue)
                                                            throws IOException {
    super(valueCount, bitsPerValue);
    int size = size(valueCount, bitsPerValue);
    blocks = new long[size+1]; // +1 due to non-conditional tricks
    // TODO: find a faster way to bulk-read longs...
    for(int i=0;i<size;i++) {
      blocks[i] = in.readLong();
    }
    updateCached();
  }

  private static int size(int valueCount, int bitsPerValue) {
    final long totBitCount = (long) valueCount * bitsPerValue;
    return (int)(totBitCount/64 + ((totBitCount % 64 == 0 ) ? 0:1));
  }

  private void updateCached() {
    readMasks = MASKS[bitsPerValue];
    shifts = SHIFTS[bitsPerValue];
    writeMasks = WRITE_MASKS[bitsPerValue];
    maxPos = (int)((((long)blocks.length) * BLOCK_SIZE / bitsPerValue) - 2);
  }

  /**
   * @param index the position of the value.
   * @return the value at the given index.
   */
  public long get(final int index) {
    assert index >= 0 && index < size();
    final long majorBitPos = (long)index * bitsPerValue;
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    final int bitPos =     (int)(majorBitPos & MOD_MASK); // % BLOCK_SIZE);

    final int base = bitPos * FAC_BITPOS;
    assert elementPos < blocks.length : "elementPos: " + elementPos + "; blocks.len: " + blocks.length;
    return ((blocks[elementPos] << shifts[base]) >>> shifts[base+1]) |
            ((blocks[elementPos+1] >>> shifts[base+2]) & readMasks[bitPos]);
  }

  public void set(final int index, final long value) {
    final long majorBitPos = (long)index * bitsPerValue;
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    final int bitPos =     (int)(majorBitPos & MOD_MASK); // % BLOCK_SIZE);
    final int base = bitPos * FAC_BITPOS;

    blocks[elementPos  ] = (blocks[elementPos  ] & writeMasks[base])
                           | (value << shifts[base + 1] >>> shifts[base]);
    blocks[elementPos+1] = (blocks[elementPos+1] & writeMasks[base+1])
                           | ((value << shifts[base + 2]) & writeMasks[base+2]);
  }

  @Override
  public String toString() {
    return "Packed64(bitsPerValue=" + bitsPerValue + ", size="
            + size() + ", maxPos=" + maxPos
            + ", elements.length=" + blocks.length + ")";
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.sizeOf(blocks);
  }

  public void clear() {
    Arrays.fill(blocks, 0L);
  }
}
