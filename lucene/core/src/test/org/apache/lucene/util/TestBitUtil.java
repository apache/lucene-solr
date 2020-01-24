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

package org.apache.lucene.util;

public class TestBitUtil extends LuceneTestCase {

  public void testNextBitSet() {
    for (int i = 0; i < 10000; i++) {
      long[] bits = buildRandomBits();
      int numLong = bits.length - 1;

      // Verify nextBitSet with countBitsUpTo for all bit indexes.
      for (int bitIndex = -1; bitIndex < 64 * numLong; bitIndex++) {
        int nextIndex = BitUtil.nextBitSet(bits, numLong, bitIndex);
        if (nextIndex == -1) {
          assertEquals("No next bit set, so expected no bit count diff"
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitUtil.countBitsUpTo(bits, numLong, bitIndex + 1), BitUtil.countBits(bits, numLong));
        } else {
          assertTrue("Expected next bit set at nextIndex=" + nextIndex
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitUtil.isBitSet(bits, numLong, nextIndex));
          assertEquals("Next bit set at nextIndex=" + nextIndex
                  + " so expected bit count diff of 1"
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitUtil.countBitsUpTo(bits, numLong, bitIndex + 1) + 1,
              BitUtil.countBitsUpTo(bits, numLong, nextIndex + 1));
        }
      }
    }
  }

  public void testPreviousBitSet() {
    for (int i = 0; i < 10000; i++) {
      long[] bits = buildRandomBits();
      int numLong = bits.length - 1;

      // Verify previousBitSet with countBitsUpTo for all bit indexes.
      for (int bitIndex = 0; bitIndex <= 64 * numLong; bitIndex++) {
        int previousIndex = BitUtil.previousBitSet(bits, numLong, bitIndex);
        if (previousIndex == -1) {
          assertEquals("No previous bit set, so expected bit count 0"
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              0, BitUtil.countBitsUpTo(bits, numLong, bitIndex));
        } else {
          assertTrue("Expected previous bit set at previousIndex=" + previousIndex
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitUtil.isBitSet(bits, numLong, previousIndex));
          int bitCount = BitUtil.countBitsUpTo(bits, numLong, Math.min(bitIndex + 1, numLong * Long.SIZE));
          int expectedPreviousBitCount = bitIndex < numLong * Long.SIZE && BitUtil.isBitSet(bits, numLong, bitIndex) ?
              bitCount - 1 : bitCount;
          assertEquals("Previous bit set at previousIndex=" + previousIndex
                  + " with current bitCount=" + bitCount
                  + " so expected previousBitCount=" + expectedPreviousBitCount
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              expectedPreviousBitCount, BitUtil.countBitsUpTo(bits, numLong, previousIndex + 1));
        }
      }
    }
  }

  private long[] buildRandomBits() {
    long[] bits = new long[random().nextInt(3) + 2];
    for (int j = 0; j < bits.length; j++) {
      // Bias towards zeros which require special logic.
      bits[j] = random().nextInt(4) == 0 ? 0L : random().nextLong();
    }
    return bits;
  }
}
