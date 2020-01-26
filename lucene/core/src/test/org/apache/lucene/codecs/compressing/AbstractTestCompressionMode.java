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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

public abstract class AbstractTestCompressionMode extends LuceneTestCase {

  CompressionMode mode;

  static byte[] randomArray() {
    final int max = random().nextBoolean()
        ? random().nextInt(4)
        : random().nextInt(255);
    final int length = random().nextBoolean()
        ? random().nextInt(20)
        : random().nextInt(192 * 1024);
    return randomArray(length, max);
  }

  static byte[] randomArray(int length, int max) {
    final byte[] arr = new byte[length];
    for (int i = 0; i < arr.length; ++i) {
      arr[i] = (byte) RandomNumbers.randomIntBetween(random(), 0, max);
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
    return ArrayUtil.copyOfSubArray(compressed, 0, compressedLen);
  }

  byte[] decompress(byte[] compressed, int originalLength) throws IOException {
    Decompressor decompressor = mode.newDecompressor();
    return decompress(decompressor, compressed, originalLength);
  }

  static byte[] decompress(Decompressor decompressor, byte[] compressed, int originalLength) throws IOException {
    final BytesRef bytes = new BytesRef();
    decompressor.decompress(new ByteArrayDataInput(compressed), originalLength, 0, originalLength, bytes);
    return BytesRef.deepCopyOf(bytes).bytes;
  }

  byte[] decompress(byte[] compressed, int originalLength, int offset, int length) throws IOException {
    Decompressor decompressor = mode.newDecompressor();
    final BytesRef bytes = new BytesRef();
    decompressor.decompress(new ByteArrayDataInput(compressed), originalLength, offset, length, bytes);
    return BytesRef.deepCopyOf(bytes).bytes;
  }

  public void testDecompress() throws IOException {
    final int iterations = atLeast(10);
    for (int i = 0; i < iterations; ++i) {
      final byte[] decompressed = randomArray();
      final int off = random().nextBoolean() ? 0 : TestUtil.nextInt(random(), 0, decompressed.length);
      final int len = random().nextBoolean() ? decompressed.length - off : TestUtil.nextInt(random(), 0, decompressed.length - off);
      final byte[] compressed = compress(decompressed, off, len);
      final byte[] restored = decompress(compressed, len);
      assertArrayEquals(ArrayUtil.copyOfSubArray(decompressed, off, off+len), restored);
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
      assertArrayEquals(ArrayUtil.copyOfSubArray(decompressed, offset, offset + length), restored);
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
    final byte[] decompressed = new byte[RandomNumbers.randomIntBetween(random(), 20, 256)];
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

}