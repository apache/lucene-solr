package org.apache.lucene.codecs.blockpacked;
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

import static org.apache.lucene.codecs.blockpacked.BlockPackedPostingsFormat.BLOCK_SIZE;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Encode all values in normal area with fixed bit width, 
 * which is determined by the max value in this block.
 */
public class ForUtil {

  /**
   * Special number of bits per value used whenever all values to encode are equal.
   */
  private static final int ALL_VALUES_EQUAL = 0;

  static final int PACKED_INTS_VERSION = 0; // nocommit: encode in the stream?
  static final PackedInts.Encoder[] ENCODERS = new PackedInts.Encoder[33];
  static final PackedInts.Decoder[] DECODERS = new PackedInts.Decoder[33];
  static final int[] ITERATIONS = new int[33];
  static {
    for (int i = 1; i <= 32; ++i) {
      ENCODERS[i] = PackedInts.getEncoder(
          PackedInts.Format.PACKED, PACKED_INTS_VERSION, i);
      DECODERS[i] = PackedInts.getDecoder(
          PackedInts.Format.PACKED, PACKED_INTS_VERSION, i);
      ITERATIONS[i] = BLOCK_SIZE / DECODERS[i].valueCount();
    }
  }

  /**
   * Write a block of data (<code>For</code> format).
   *
   * @param data     the data to write
   * @param encoded  a buffer to use to encode data
   * @param out      the destination output
   * @throws IOException
   */
  static void writeBlock(long[] data, byte[] encoded, IndexOutput out) throws IOException {
    if (isAllEqual(data)) {
      out.writeVInt(ALL_VALUES_EQUAL);
      out.writeInt((int) data[0]);
      return;
    }

    final int numBits = bitsRequired(data);
    assert numBits > 0 && numBits <= 32 : numBits;
    final PackedInts.Encoder encoder = ENCODERS[numBits];
    final int iters = ITERATIONS[numBits];
    assert iters * encoder.valueCount() == BlockPackedPostingsFormat.BLOCK_SIZE;
    final int encodedSize = encodedSize(numBits);

    out.writeVInt(numBits);

    encoder.encode(data, 0, encoded, 0, iters);
    out.writeBytes(encoded, encodedSize);
  }

  /**
   * Read the next block of data (<code>For</code> format).
   *
   * @param in        the input to use to read data
   * @param encoded   a buffer that can be used to store encoded data
   * @param decoded   where to write decoded data
   * @throws IOException
   */
  static void readBlock(IndexInput in, byte[] encoded, long[] decoded) throws IOException {
    final int numBits = in.readVInt();
    assert numBits <= 32 : numBits;

    if (numBits == ALL_VALUES_EQUAL) {
      final int value = in.readInt();
      Arrays.fill(decoded, value);
      return;
    }

    final int encodedSize = encodedSize(numBits);
    in.readBytes(encoded, 0, encodedSize);

    final PackedInts.Decoder decoder = DECODERS[numBits];
    final int iters = ITERATIONS[numBits];
    assert iters * decoder.valueCount() == BLOCK_SIZE;
    assert 8 * iters * decoder.blockCount() == encodedSize;

    decoder.decode(encoded, 0, decoded, 0, iters);
  }

  /**
   * Skip the next block of data.
   *
   * @param in      the input where to read data
   * @throws IOException
   */
  static void skipBlock(IndexInput in) throws IOException {
    final int numBits = in.readVInt();
    assert numBits > 0 && numBits <= 32 : numBits;
    final int encodedSize = encodedSize(numBits);
    in.seek(in.getFilePointer() + encodedSize);
  }

  /**
   * Read values that have been written using variable-length encoding instead of bit-packing.
   */
  static void readVIntBlock(IndexInput docIn, long[] docBuffer,
      long[] freqBuffer, int num, boolean indexHasFreq) throws IOException {
    if (indexHasFreq) {
      for(int i=0;i<num;i++) {
        final int code = docIn.readVInt();
        docBuffer[i] = code >>> 1;
        if ((code & 1) != 0) {
          freqBuffer[i] = 1;
        } else {
          freqBuffer[i] = docIn.readVInt();
        }
      }
    } else {
      for(int i=0;i<num;i++) {
        docBuffer[i] = docIn.readVInt();
      }
    }
  }

  // nocommit: we must have a util function for this, hmm?
  private static boolean isAllEqual(final long[] data) {
    final long v = data[0];
    for (int i = 1; i < data.length; ++i) {
      if (data[i] != v) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compute the number of bits required to serialize any of the longs in
   * <code>data</code>.
   */
  private static int bitsRequired(final long[] data) {
    long or = 0;
    for (int i = 0; i < data.length; ++i) {
      or |= data[i];
    }
    return PackedInts.bitsRequired(or);
  }

  /**
   * Compute the number of bytes required to encode a block of values that require
   * <code>bitsPerValue</code> bits per value.
   */
  private static int encodedSize(int bitsPerValue) {
    return (BLOCK_SIZE * bitsPerValue) >>> 3;
  }

}
