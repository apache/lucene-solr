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
package org.apache.lucene.codecs.compressing;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

public abstract class AbstractTestCompressionMode extends LuceneTestCase {

  CompressionMode mode;

  static byte[] randomArray() {
    final int max = random().nextBoolean()
        ? random().nextInt(4)
        : random().nextInt(256);
    final int length = random().nextBoolean()
        ? random().nextInt(20)
        : random().nextInt(192 * 1024);
    return randomArray(length, max);
  }

  static byte[] randomArray(int length, int max) {
    final byte[] arr = new byte[length];
    for (int i = 0; i < arr.length; ++i) {
      arr[i] = (byte) RandomInts.randomIntBetween(random(), 0, max);
    }
    return arr;
  }

  byte[] compress(byte[] decompressed, int off, int len) throws IOException {
    Compressor compressor = mode.newCompressor();
    return compress(compressor, decompressed, off, len);
  }

  static byte[] compress(Compressor compressor, byte[] decompressed, int off, int len) throws IOException {
    byte[] compressed = new byte[len * 2 + 16]; // should be enough
    ByteArrayDataOutput out = new ByteArrayDataOutput(compressed);
    compressor.compress(decompressed, off, len, out);
    final int compressedLen = out.getPosition();
    return Arrays.copyOf(compressed, compressedLen);
  }

  byte[] decompress(byte[] compressed, int originalLength) throws IOException {
    Decompressor decompressor = mode.newDecompressor();
    return decompress(decompressor, compressed, originalLength);
  }

  static byte[] decompress(Decompressor decompressor, byte[] compressed, int originalLength) throws IOException {
    final BytesRef bytes = new BytesRef();
    decompressor.decompress(new ByteArrayDataInput(compressed), originalLength, 0, originalLength, bytes);
    return Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
  }

  byte[] decompress(byte[] compressed, int originalLength, int offset, int length) throws IOException {
    Decompressor decompressor = mode.newDecompressor();
    final BytesRef bytes = new BytesRef();
    decompressor.decompress(new ByteArrayDataInput(compressed), originalLength, offset, length, bytes);
    return Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
  }

  public void testDecompress() throws IOException {
    final int iterations = atLeast(10);
    for (int i = 0; i < iterations; ++i) {
      final byte[] decompressed = randomArray();
      final int off = random().nextBoolean() ? 0 : TestUtil.nextInt(random(), 0, decompressed.length);
      final int len = random().nextBoolean() ? decompressed.length - off : TestUtil.nextInt(random(), 0, decompressed.length - off);
      final byte[] compressed = compress(decompressed, off, len);
      final byte[] restored = decompress(compressed, len);
      assertArrayEquals(Arrays.copyOfRange(decompressed, off, off+len), restored);
    }
  }

  public void testPartialDecompress() throws IOException {
    final int iterations = atLeast(10);
    for (int i = 0; i < iterations; ++i) {
      final byte[] decompressed = randomArray();
      final byte[] compressed = compress(decompressed, 0, decompressed.length);
      final int offset, length;
      if (decompressed.length == 0) {
        offset = length = 0;
      } else {
        offset = random().nextInt(decompressed.length);
        length = random().nextInt(decompressed.length - offset);
      }
      final byte[] restored = decompress(compressed, decompressed.length, offset, length);
      assertArrayEquals(Arrays.copyOfRange(decompressed, offset, offset + length), restored);
    }
  }

  public byte[] test(byte[] decompressed) throws IOException {
    return test(decompressed, 0, decompressed.length);
  }

  public byte[] test(byte[] decompressed, int off, int len) throws IOException {
    final byte[] compressed = compress(decompressed, off, len);
    final byte[] restored = decompress(compressed, len);
    assertEquals(len, restored.length);
    return compressed;
  }

  public void testEmptySequence() throws IOException {
    test(new byte[0]);
  }

  public void testShortSequence() throws IOException {
    test(new byte[] { (byte) random().nextInt(256) });
  }

  public void testIncompressible() throws IOException {
    final byte[] decompressed = new byte[RandomInts.randomIntBetween(random(), 20, 256)];
    for (int i = 0; i < decompressed.length; ++i) {
      decompressed[i] = (byte) i;
    }
    test(decompressed);
  }

  public void testConstant() throws IOException {
    final byte[] decompressed = new byte[TestUtil.nextInt(random(), 1, 10000)];
    Arrays.fill(decompressed, (byte) random().nextInt());
    test(decompressed);
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
    test(data, 9, data.length - 9);
  }

}