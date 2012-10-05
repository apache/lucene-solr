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
    final byte[] arr = new byte[length];
    for (int i = 0; i < arr.length; ++i) {
      arr[i] = (byte) RandomInts.randomIntBetween(random(), 0, max);
    }
    return arr;
  }

  byte[] compress(byte[] uncompressed) throws IOException {
    Compressor compressor = mode.newCompressor();
    return compress(compressor, uncompressed);
  }

  static byte[] compress(Compressor compressor, byte[] uncompressed) throws IOException {
    byte[] compressed = new byte[uncompressed.length * 2 + 16]; // should be enough
    ByteArrayDataOutput out = new ByteArrayDataOutput(compressed);
    compressor.compress(uncompressed, 0, uncompressed.length, out);
    final int compressedLen = out.getPosition();
    return Arrays.copyOf(compressed, compressedLen);
  }

  byte[] uncompress(byte[] compressed) throws IOException {
    Uncompressor uncompressor = mode.newUncompressor();
    return uncompress(uncompressor, compressed);
  }

  static byte[] uncompress(Uncompressor uncompressor, byte[] compressed) throws IOException {
    final BytesRef bytes = new BytesRef();
    uncompressor.uncompress(new ByteArrayDataInput(compressed), bytes);
    return Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
  }

  byte[] uncompress(byte[] compressed, int offset, int length) throws IOException {
    Uncompressor uncompressor = mode.newUncompressor();
    final BytesRef bytes = new BytesRef();
    uncompressor.uncompress(new ByteArrayDataInput(compressed), offset, length, bytes);
    return Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
  }

  public void testUncompress() throws IOException {
    final byte[] uncompressed = randomArray();
    final byte[] compressed = compress(uncompressed);
    final byte[] restored = uncompress(compressed);
    assertArrayEquals(uncompressed, restored);
  }

  public void testPartialUncompress() throws IOException {
    final int iterations = atLeast(10);
    for (int i = 0; i < iterations; ++i) {
      final byte[] uncompressed = randomArray();
      final byte[] compressed = compress(uncompressed);
      final int offset, length;
      if (uncompressed.length == 0) {
        offset = length = 0;
      } else {
        offset = random().nextInt(uncompressed.length);
        length = random().nextInt(uncompressed.length - offset);
      }
      final byte[] restored = uncompress(compressed, offset, length);
      assertArrayEquals(Arrays.copyOfRange(uncompressed, offset, offset + length), restored);
    }
  }

  public void testCopyCompressedData() throws IOException {
    final byte[] uncompressed = randomArray();
    final byte[] compressed = compress(uncompressed);
    GrowableByteArrayDataOutput out = new GrowableByteArrayDataOutput(uncompressed.length);
    mode.newUncompressor().copyCompressedData(new ByteArrayDataInput(compressed), out);
    assertArrayEquals(compressed, Arrays.copyOf(out.bytes, out.length));
  }

}
