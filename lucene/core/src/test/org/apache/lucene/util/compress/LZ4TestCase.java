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
package org.apache.lucene.util.compress;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

public abstract class LZ4TestCase extends LuceneTestCase {

  protected abstract LZ4.HashTable newHashTable();

  protected static class AssertingHashTable extends LZ4.HashTable {

    private final LZ4.HashTable in;

    AssertingHashTable(LZ4.HashTable in) {
      this.in = in;
    }

    @Override
    void reset(byte[] b, int off, int len) {
      in.reset(b, off, len);
      assertTrue(in.assertReset());
    }

    @Override
    void initDictionary(int dictLen) {
      assertTrue(in.assertReset());
      in.initDictionary(dictLen);
    }

    @Override
    int get(int off) {
      return in.get(off);
    }

    @Override
    int previous(int off) {
      return in.previous(off);
    }

    @Override
    boolean assertReset() {
      throw new UnsupportedOperationException();
    }

  }

  private void doTest(byte[] data, LZ4.HashTable hashTable) throws IOException {
    int offset = data.length >= (1 << 16) || random().nextBoolean()
        ? random().nextInt(10)
        : (1<<16) - data.length / 2; // this triggers special reset logic for high compression
    byte[] copy = new byte[data.length + offset + random().nextInt(10)];
    System.arraycopy(data, 0, copy, offset, data.length);
    doTest(copy, offset, data.length, hashTable);
  }

  private void doTest(byte[] data, int offset, int length, LZ4.HashTable hashTable) throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    LZ4.compress(data, offset, length, out, hashTable);
    byte[] compressed = out.toArrayCopy();

    int off = 0;
    int decompressedOff = 0;
    for (;;) {
      final int token = compressed[off++] & 0xFF;
      int literalLen = token >>> 4;
      if (literalLen == 0x0F) {
        while (compressed[off] == (byte) 0xFF) {
          literalLen += 0xFF;
          ++off;
        }
        literalLen += compressed[off++] & 0xFF;
      }
      // skip literals
      off += literalLen;
      decompressedOff += literalLen;

      // check that the stream ends with literals and that there are at least
      // 5 of them
      if (off == compressed.length) {
        assertEquals(length, decompressedOff);
        assertTrue("lastLiterals=" + literalLen + ", bytes=" + length,
            literalLen >= LZ4.LAST_LITERALS || literalLen == length);
        break;
      }

      final int matchDec = (compressed[off++] & 0xFF) | ((compressed[off++] & 0xFF) << 8);
      // check that match dec is not 0
      assertTrue(matchDec + " " + decompressedOff, matchDec > 0 && matchDec <= decompressedOff);

      int matchLen = token & 0x0F;
      if (matchLen == 0x0F) {
        while (compressed[off] == (byte) 0xFF) {
          matchLen += 0xFF;
          ++off;
        }
        matchLen += compressed[off++] & 0xFF;
      }
      matchLen += LZ4.MIN_MATCH;

      // if the match ends prematurely, the next sequence should not have
      // literals or this means we are wasting space
      if (decompressedOff + matchLen < length - LZ4.LAST_LITERALS) {
        final boolean moreCommonBytes = data[offset + decompressedOff + matchLen] == data[offset + decompressedOff - matchDec + matchLen];
        final boolean nextSequenceHasLiterals = ((compressed[off] & 0xFF) >>> 4) != 0;
        assertTrue(moreCommonBytes == false || nextSequenceHasLiterals == false);
      }      

      decompressedOff += matchLen;
    }
    assertEquals(length, decompressedOff);

    // Compress once again with the same hash table to test reuse
    ByteBuffersDataOutput out2 = new ByteBuffersDataOutput();
    LZ4.compress(data, offset, length, out2, hashTable);
    assertArrayEquals(compressed, out2.toArrayCopy());

    // Now restore and compare bytes
    byte[] restored = new byte[length + random().nextInt(10)];
    LZ4.decompress(new ByteArrayDataInput(compressed), length, restored, 0);
    assertArrayEquals(ArrayUtil.copyOfSubArray(data, offset, offset+length), ArrayUtil.copyOfSubArray(restored, 0, length));

    // Now restore with an offset
    int restoreOffset = TestUtil.nextInt(random(), 1, 10);
    restored = new byte[restoreOffset + length + random().nextInt(10)];
    LZ4.decompress(new ByteArrayDataInput(compressed), length, restored, restoreOffset);
    assertArrayEquals(ArrayUtil.copyOfSubArray(data, offset, offset+length), ArrayUtil.copyOfSubArray(restored, restoreOffset, restoreOffset+length));
  }

  private void doTestWithDictionary(byte[] data, LZ4.HashTable hashTable) throws IOException {
    ByteBuffersDataOutput copy = new ByteBuffersDataOutput();
    int dictOff = TestUtil.nextInt(random(), 0, 10);
    copy.writeBytes(new byte[dictOff]);

    // Create a dictionary from substrings of the input to compress
    int dictLen = 0;
    for (int i = TestUtil.nextInt(random(), 0, data.length); i < data.length && dictLen < LZ4.MAX_DISTANCE; ) {
      int l = Math.min(data.length - i, TestUtil.nextInt(random(), 1, 32));
      l = Math.min(l, LZ4.MAX_DISTANCE - dictLen);
      copy.writeBytes(data, i, l);
      dictLen += l;
      i += l;
      i += TestUtil.nextInt(random(), 1, 32);
    }

    copy.writeBytes(data);
    copy.writeBytes(new byte[random().nextInt(10)]);

    byte[] copyBytes = copy.toArrayCopy();
    doTestWithDictionary(copyBytes, dictOff, dictLen, data.length, hashTable);
  }

  private void doTestWithDictionary(byte[] data, int dictOff, int dictLen, int length, LZ4.HashTable hashTable) throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    LZ4.compressWithDictionary(data, dictOff, dictLen, length, out, hashTable);
    byte[] compressed = out.toArrayCopy();

    // Compress once again with the same hash table to test reuse
    ByteBuffersDataOutput out2 = new ByteBuffersDataOutput();
    LZ4.compressWithDictionary(data, dictOff, dictLen, length, out2, hashTable);
    assertArrayEquals(compressed, out2.toArrayCopy());

    // Now restore and compare bytes
    int restoreOffset = TestUtil.nextInt(random(), 1, 10);
    byte[] restored = new byte[restoreOffset + dictLen + length + random().nextInt(10)];
    System.arraycopy(data, dictOff, restored, restoreOffset, dictLen);
    LZ4.decompress(new ByteArrayDataInput(compressed), length, restored, dictLen + restoreOffset);
    assertArrayEquals(
        ArrayUtil.copyOfSubArray(data, dictOff+dictLen, dictOff+dictLen+length),
        ArrayUtil.copyOfSubArray(restored, restoreOffset+dictLen, restoreOffset+dictLen+length));
  }

  public void testEmpty() throws IOException {
    // literals and matchs lengths <= 15
    final byte[] data = "".getBytes(StandardCharsets.UTF_8);
    doTest(data, newHashTable());
  }

  public void testShortLiteralsAndMatchs() throws IOException {
    // literals and matchs lengths <= 15
    final byte[] data = "1234562345673456745678910123".getBytes(StandardCharsets.UTF_8);
    doTest(data, newHashTable());
    doTestWithDictionary(data, newHashTable());
  }

  public void testLongMatchs() throws IOException {
    // match length >= 20
    final byte[] data = new byte[RandomNumbers.randomIntBetween(random(), 300, 1024)];
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte) i;
    }
    doTest(data, newHashTable());
  }

  public void testLongLiterals() throws IOException {
    // long literals (length >= 16) which are not the last literals
    final byte[] data = new byte[RandomNumbers.randomIntBetween(random(), 400, 1024)];
    random().nextBytes(data);
    final int matchRef = random().nextInt(30);
    final int matchOff = RandomNumbers.randomIntBetween(random(), data.length - 40, data.length - 20);
    final int matchLength = RandomNumbers.randomIntBetween(random(), 4, 10);
    System.arraycopy(data, matchRef, data, matchOff, matchLength);
    doTest(data, newHashTable());
  }

  public void testMatchRightBeforeLastLiterals() throws IOException {
    doTest(new byte[] {1,2,3,4, 1,2,3,4, 1,2,3,4,5}, newHashTable());
  }

  public void testIncompressibleRandom() throws IOException {
    byte[] b = new byte[TestUtil.nextInt(random(), 1, 1 << 32)];
    random().nextBytes(b);
    doTest(b, newHashTable());
    doTestWithDictionary(b, newHashTable());
  }

  public void testCompressibleRandom() throws IOException {
    byte[] b = new byte[TestUtil.nextInt(random(), 1, 1 << 18)];
    final int base = random().nextInt(256);
    final int maxDelta = 1 + random().nextInt(8);
    Random r = random();
    for (int i = 0; i < b.length; ++i) {
      b[i] = (byte) (base + r.nextInt(maxDelta));
    }
    doTest(b, newHashTable());
    doTestWithDictionary(b, newHashTable());
  }

  public void testLUCENE5201() throws IOException {
    byte[] data = new byte[]{
        14, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 72, 14, 72, 14, 85, 3, 72, 14, 72, 14, 72, 14, 72, 14, 72, 14, 72, 14, 85, 3, 72,
        14, 85, 3, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 85, 3, 72, 14, 50, 64, 0, 46, -1, 0, 0, 0, 29, 3, 85,
        8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 50, 64, 0, 47, -105, 0, 0, 0, 30, 3, -97, 6, 0, 68, -113,
        0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85,
        8, -113, 0, 68, -97, 3, 0, 2, -97, 6, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 120, 64, 0, 48, 4, 0, 0, 0, 31, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72,
        33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72,
        43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72,
        28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72,
        35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72,
        41, 72, 32, 72, 18, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 39, 24, 32, 34, 124, 0, 120, 64, 0, 48, 80, 0, 0, 0, 31, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72,
        35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72,
        41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72,
        40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72,
        31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72,
        26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72,
        37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72,
        36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72,
        20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72,
        22, 72, 31, 72, 43, 72, 19, 72, 34, 72, 29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72,
        38, 72, 26, 72, 28, 72, 42, 72, 24, 72, 27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 72, 34, 72,
        29, 72, 37, 72, 35, 72, 45, 72, 23, 72, 46, 72, 20, 72, 40, 72, 33, 72, 25, 72, 39, 72, 38, 72, 26, 72, 28, 72, 42, 72, 24, 72,
        27, 72, 36, 72, 41, 72, 32, 72, 18, 72, 30, 72, 22, 72, 31, 72, 43, 72, 19, 50, 64, 0, 49, 20, 0, 0, 0, 32, 3, -97, 6, 0,
        68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2,
        3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2,
        3, -97, 6, 0, 50, 64, 0, 50, 53, 0, 0, 0, 34, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3,
        -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97,
        3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3,
        85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0,
        2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3,
        -97, 6, 0, 50, 64, 0, 51, 85, 0, 0, 0, 36, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, -97, 5, 0, 2, 3, 85, 8, -113, 0, 68,
        -97, 3, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0,
        68, -113, 0, 2, 3, -97, 6, 0, 50, -64, 0, 51, -45, 0, 0, 0, 37, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6,
        0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, -97, 6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -113, 0, 2, 3, -97,
        6, 0, 68, -113, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 2, 3, 85, 8, -113, 0, 68, -97, 3, 0, 120, 64, 0, 52, -88, 0, 0,
        0, 39, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72,
        13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85,
        5, 72, 13, 85, 5, 72, 13, 72, 13, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85,
        5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85,
        5, 72, 13, 85, 5, 72, 13, 72, 13, 72, 13, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, 72,
        13, 85, 5, 72, 13, 72, 13, 85, 5, 72, 13, -19, -24, -101, -35
      };
    doTest(data, 9, data.length - 9, newHashTable());
  }

  public void testUseDictionary() throws IOException {
    byte[] b = new byte[] {
        1, 2, 3, 4, 5, 6, // dictionary
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    };
    int dictOff = 0;
    int dictLen = 6;
    int len = b.length - dictLen;

    doTestWithDictionary(b, dictOff, dictLen, len, newHashTable());
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    LZ4.compressWithDictionary(b, dictOff, dictLen, len, out, newHashTable());

    // The compressed output is smaller than the original input despite being incompressible on its own
    assertTrue(out.size() < len);
  }
}
