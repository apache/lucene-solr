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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Encode all values in normal area with fixed bit width, 
 * which is determined by the max value in this block.
 */
public class ForUtil {
  
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
    final int numBits = bitsRequired(data);
    assert numBits > 0 && numBits <= 32 : numBits;
    final PackedInts.Encoder encoder = ENCODERS[numBits];
    final int iters = ITERATIONS[numBits];
    assert iters * encoder.valueCount() == BlockPackedPostingsFormat.BLOCK_SIZE;
    final int encodedSize = encoder.blockCount() * iters; // number of 64-bits blocks
    assert encodedSize > 0 && encodedSize <= BLOCK_SIZE / 2 : encodedSize;

    out.writeByte((byte) numBits);
    out.writeByte((byte) encodedSize);

    encoder.encode(data, 0, encoded, 0, iters);
    out.writeBytes(encoded, encodedSize << 3);
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
    final int numBits = in.readByte(); // no mask because should be <= 32
    final int encodedSize = in.readByte(); // no mask because should be <= 64
    assert numBits > 0 && numBits <= 32 : numBits;
    assert encodedSize > 0 && encodedSize <= BLOCK_SIZE / 2 : encodedSize; // because blocks are 64-bits and decoded values are 32-bits at most

    in.readBytes(encoded, 0, encodedSize << 3);

    final PackedInts.Decoder decoder = DECODERS[numBits];
    final int iters = ITERATIONS[numBits];
    assert iters * decoder.valueCount() == BLOCK_SIZE;
    assert iters * decoder.blockCount() == encodedSize;

    decoder.decode(encoded, 0, decoded, 0, iters);
  }

  /**
   * Skip the next block of data.
   *
   * @param in      the input where to read data
   * @throws IOException
   */
  static void skipBlock(IndexInput in) throws IOException {
    // see readBlock for comments
    final int numBits = in.readByte();
    final int encodedSize = in.readByte();
    assert numBits > 0 && numBits <= 32 : numBits;
    assert encodedSize > 0 && encodedSize <= BLOCK_SIZE / 2 : encodedSize;
    in.seek(in.getFilePointer() + (encodedSize << 3));
  }

  /**
   * Read values that have been written using variable-length encoding instead of bit-packing.
   */
  static void readVIntBlock(IndexInput docIn, long[] docBuffer, long[] freqBuffer, int num, boolean indexHasFreq) throws IOException {
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

  /**
   * Compute the number of bits required to serialize any of the longs in <code>data</code>.
   */
  private static int bitsRequired(final long[] data) {
    long or = 0;
    for (int i = 0; i < data.length; ++i) {
      or |= data[i];
    }
    return PackedInts.bitsRequired(or);
  }

}
