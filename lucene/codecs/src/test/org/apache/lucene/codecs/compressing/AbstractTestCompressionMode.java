package org.apache.lucene.codecs.compressing;

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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

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

  byte[] compress(byte[] decompressed) throws IOException {
    Compressor compressor = mode.newCompressor();
    return compress(compressor, decompressed);
  }

  static byte[] compress(Compressor compressor, byte[] decompressed) throws IOException {
    byte[] compressed = new byte[decompressed.length * 2 + 16]; // should be enough
    ByteArrayDataOutput out = new ByteArrayDataOutput(compressed);
    compressor.compress(decompressed, 0, decompressed.length, out);
    final int compressedLen = out.getPosition();
    return Arrays.copyOf(compressed, compressedLen);
  }

  byte[] decompress(byte[] compressed) throws IOException {
    Decompressor decompressor = mode.newDecompressor();
    return decompress(decompressor, compressed);
  }

  static byte[] decompress(Decompressor decompressor, byte[] compressed) throws IOException {
    final BytesRef bytes = new BytesRef();
    decompressor.decompress(new ByteArrayDataInput(compressed), bytes);
    return Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
  }

  byte[] decompress(byte[] compressed, int offset, int length) throws IOException {
    Decompressor decompressor = mode.newDecompressor();
    final BytesRef bytes = new BytesRef();
    decompressor.decompress(new ByteArrayDataInput(compressed), offset, length, bytes);
    return Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
  }

  static byte[] copyCompressedData(Decompressor decompressor, byte[] compressed) throws IOException {
    GrowableByteArrayDataOutput out = new GrowableByteArrayDataOutput(compressed.length);
    decompressor.copyCompressedData(new ByteArrayDataInput(compressed), out);
    return Arrays.copyOf(out.bytes, out.length);
  }

  byte[] copyCompressedData(byte[] compressed) throws IOException {
    return copyCompressedData(mode.newDecompressor(), compressed);
  }

  public void testDecompress() throws IOException {
    final byte[] decompressed = randomArray();
    final byte[] compressed = compress(decompressed);
    final byte[] restored = decompress(compressed);
    assertArrayEquals(decompressed, restored);
  }

  public void testPartialDecompress() throws IOException {
    final int iterations = atLeast(10);
    for (int i = 0; i < iterations; ++i) {
      final byte[] decompressed = randomArray();
      final byte[] compressed = compress(decompressed);
      final int offset, length;
      if (decompressed.length == 0) {
        offset = length = 0;
      } else {
        offset = random().nextInt(decompressed.length);
        length = random().nextInt(decompressed.length - offset);
      }
      final byte[] restored = decompress(compressed, offset, length);
      assertArrayEquals(Arrays.copyOfRange(decompressed, offset, offset + length), restored);
    }
  }

  public void testCopyCompressedData() throws IOException {
    final byte[] decompressed = randomArray();
    final byte[] compressed = compress(decompressed);
    assertArrayEquals(compressed, copyCompressedData(compressed));
  }

  public void test(byte[] decompressed) throws IOException {
    final byte[] compressed = compress(decompressed);
    final byte[] restored = decompress(compressed);
    assertEquals(decompressed.length, restored.length);
    assertArrayEquals(compressed, copyCompressedData(compressed));
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

  // for LZ compression

  public void testShortLiteralsAndMatchs() throws IOException {
    // literals and matchs lengths <= 15
    final byte[] decompressed = "1234562345673456745678910123".getBytes("UTF-8");
    test(decompressed);
  }

  public void testLongMatchs() throws IOException {
    // match length > 16
    final byte[] decompressed = new byte[RandomInts.randomIntBetween(random(), 300, 1024)];
    for (int i = 0; i < decompressed.length; ++i) {
      decompressed[i] = (byte) i;
    }
    test(decompressed);
  }

  public void testLongLiterals() throws IOException {
    // long literals (length > 16) which are not the last literals
    final byte[] decompressed = randomArray(RandomInts.randomIntBetween(random(), 400, 1024), 256);
    final int matchRef = random().nextInt(30);
    final int matchOff = RandomInts.randomIntBetween(random(), decompressed.length - 40, decompressed.length - 20);
    final int matchLength = RandomInts.randomIntBetween(random(), 4, 10);
    System.arraycopy(decompressed, matchRef, decompressed, matchOff, matchLength);
    test(decompressed);
  }

}