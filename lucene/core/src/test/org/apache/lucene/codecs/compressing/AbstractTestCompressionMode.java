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
      final byte[] compressed = compress(decompressed);
      final byte[] restored = decompress(compressed, decompressed.length);
      assertArrayEquals(decompressed, restored);
    }
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
      final byte[] restored = decompress(compressed, decompressed.length, offset, length);
      assertArrayEquals(Arrays.copyOfRange(decompressed, offset, offset + length), restored);
    }
  }

  public byte[] test(byte[] decompressed) throws IOException {
    final byte[] compressed = compress(decompressed);
    final byte[] restored = decompress(compressed, decompressed.length);
    assertEquals(decompressed.length, restored.length);
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

}