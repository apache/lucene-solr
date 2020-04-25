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

package org.apache.lucene.util.fst;

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;

public class TestBitTableUtil extends LuceneTestCase {

  public void testNextBitSet() throws IOException {
    int numIterations = atLeast(1000);
    for (int i = 0; i < numIterations; i++) {
      byte[] bits = buildRandomBits();
      int numBytes = bits.length - 1;
      int numBits = numBytes * Byte.SIZE;

      // Verify nextBitSet with countBitsUpTo for all bit indexes.
      for (int bitIndex = -1; bitIndex < numBits; bitIndex++) {
        int nextIndex = BitTableUtil.nextBitSet(bitIndex, numBytes, reader(bits));
        if (nextIndex == -1) {
          assertEquals("No next bit set, so expected no bit count diff"
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitTableUtil.countBitsUpTo(bitIndex + 1, reader(bits)),
              BitTableUtil.countBits(numBytes, reader(bits)));
        } else {
          assertTrue("Expected next bit set at nextIndex=" + nextIndex
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitTableUtil.isBitSet(nextIndex, reader(bits)));
          assertEquals("Next bit set at nextIndex=" + nextIndex
                  + " so expected bit count diff of 1"
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitTableUtil.countBitsUpTo(bitIndex + 1, reader(bits)) + 1,
              BitTableUtil.countBitsUpTo(nextIndex + 1, reader(bits)));
        }
      }
    }
  }

  public void testPreviousBitSet() throws IOException {
    int numIterations = atLeast(1000);
    for (int i = 0; i < numIterations; i++) {
      byte[] bits = buildRandomBits();
      int numBytes = bits.length - 1;
      int numBits = numBytes * Byte.SIZE;

      // Verify previousBitSet with countBitsUpTo for all bit indexes.
      for (int bitIndex = 0; bitIndex <= numBits; bitIndex++) {
        int previousIndex = BitTableUtil.previousBitSet(bitIndex, reader(bits));
        if (previousIndex == -1) {
          assertEquals("No previous bit set, so expected bit count 0"
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              0, BitTableUtil.countBitsUpTo(bitIndex, reader(bits)));
        } else {
          assertTrue("Expected previous bit set at previousIndex=" + previousIndex
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              BitTableUtil.isBitSet(previousIndex, reader(bits)));
          int bitCount = BitTableUtil.countBitsUpTo(Math.min(bitIndex + 1, numBits), reader(bits));
          int expectedPreviousBitCount = bitIndex < numBits && BitTableUtil.isBitSet(bitIndex, reader(bits)) ?
              bitCount - 1 : bitCount;
          assertEquals("Previous bit set at previousIndex=" + previousIndex
                  + " with current bitCount=" + bitCount
                  + " so expected previousBitCount=" + expectedPreviousBitCount
                  + " (i=" + i + " bitIndex=" + bitIndex + ")",
              expectedPreviousBitCount, BitTableUtil.countBitsUpTo(previousIndex + 1, reader(bits)));
        }
      }
    }
  }

  private byte[] buildRandomBits() {
    byte[] bits = new byte[random().nextInt(24) + 2];
    for (int i = 0; i < bits.length; i++) {
      // Bias towards zeros which require special logic.
      bits[i] = random().nextInt(4) == 0 ? 0 : (byte) random().nextInt();
    }
    return bits;
  }

  private static FST.BytesReader reader(byte[] bits) {
    return new ByteArrayBytesReader(bits);
  }

  private static class ByteArrayBytesReader extends FST.BytesReader {

    private final byte[] bits;
    private int position;

    ByteArrayBytesReader(byte[] bits) {
      this.bits = bits;
    }

    @Override
    public long getPosition() {
      return position;
    }

    @Override
    public void setPosition(long pos) {
      position = (int) pos;
    }

    @Override
    public boolean reversed() {
      return false;
    }

    @Override
    public byte readByte() {
      return bits[position++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void skipBytes(long numBytes) {
      position += numBytes;
    }
  }
}