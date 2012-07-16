package org.apache.lucene.codecs.pfor;
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

import java.nio.IntBuffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Encode all values in normal area with fixed bit width, 
 * which is determined by the max value in this block.
 */
public class ForUtil {
  protected static final int[] MASK = {   0x00000000,
    0x00000001, 0x00000003, 0x00000007, 0x0000000f, 0x0000001f, 0x0000003f,
    0x0000007f, 0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff,
    0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff, 0x0001ffff, 0x0003ffff,
    0x0007ffff, 0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff,
    0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff,
    0x7fffffff, 0xffffffff};

  /** Compress given int[] into Integer buffer, with For format
   *
   * @param data        uncompressed data
   * @param size        num of ints to compress
   * @param intBuffer   integer buffer to hold compressed data
   * @return encoded block byte size
   */
  public static int compress(final int[] data, IntBuffer intBuffer) {
    int numBits=getNumBits(data);
    if (numBits == 0) {
      return compressDuplicateBlock(data,intBuffer);
    }
 
    int size=data.length;
    int encodedSize = (size*numBits+31)/32;

    for (int i=0; i<size; ++i) {
      encodeNormalValue(intBuffer,i,data[i], numBits);
    }

    return getHeader(encodedSize, numBits);
  }

  /**
   * Save only one int when the whole block equals to 1
   */
  static int compressDuplicateBlock(final int[] data, IntBuffer intBuffer) {
    intBuffer.put(0,data[0]);
    return getHeader(1, 0);
  }

  /** Decompress given Integer buffer into int array.
   *
   * @param intBuffer   integer buffer to hold compressed data
   * @param data        int array to hold uncompressed data
   */
  public static void decompress(IntBuffer intBuffer, int[] data, int header) {
    // since this buffer is reused at upper level, rewind first
    intBuffer.rewind();

    int numBits = ((header >> 8) & MASK[6]);

    decompressCore(intBuffer, data, numBits);
  }

  /**
   * IntBuffer will not be rewinded in this method, therefore
   * caller should ensure that the position is set to the first
   * encoded int before decoding.
   */
  static void decompressCore(IntBuffer intBuffer, int[] data, int numBits) {
    assert numBits<=32;
    assert numBits>=0;

    // TODO: PackedIntsDecompress is hardewired to size==128 only
    switch(numBits) {
      case 0: PackedIntsDecompress.decode0(intBuffer, data); break;
      case 1: PackedIntsDecompress.decode1(intBuffer, data); break;
      case 2: PackedIntsDecompress.decode2(intBuffer, data); break;
      case 3: PackedIntsDecompress.decode3(intBuffer, data); break;
      case 4: PackedIntsDecompress.decode4(intBuffer, data); break;
      case 5: PackedIntsDecompress.decode5(intBuffer, data); break;
      case 6: PackedIntsDecompress.decode6(intBuffer, data); break;
      case 7: PackedIntsDecompress.decode7(intBuffer, data); break;
      case 8: PackedIntsDecompress.decode8(intBuffer, data); break;
      case 9: PackedIntsDecompress.decode9(intBuffer, data); break;
      case 10: PackedIntsDecompress.decode10(intBuffer, data); break;
      case 11: PackedIntsDecompress.decode11(intBuffer, data); break;
      case 12: PackedIntsDecompress.decode12(intBuffer, data); break;
      case 13: PackedIntsDecompress.decode13(intBuffer, data); break;
      case 14: PackedIntsDecompress.decode14(intBuffer, data); break;
      case 15: PackedIntsDecompress.decode15(intBuffer, data); break;
      case 16: PackedIntsDecompress.decode16(intBuffer, data); break;
      case 17: PackedIntsDecompress.decode17(intBuffer, data); break;
      case 18: PackedIntsDecompress.decode18(intBuffer, data); break;
      case 19: PackedIntsDecompress.decode19(intBuffer, data); break;
      case 20: PackedIntsDecompress.decode20(intBuffer, data); break;
      case 21: PackedIntsDecompress.decode21(intBuffer, data); break;
      case 22: PackedIntsDecompress.decode22(intBuffer, data); break;
      case 23: PackedIntsDecompress.decode23(intBuffer, data); break;
      case 24: PackedIntsDecompress.decode24(intBuffer, data); break;
      case 25: PackedIntsDecompress.decode25(intBuffer, data); break;
      case 26: PackedIntsDecompress.decode26(intBuffer, data); break;
      case 27: PackedIntsDecompress.decode27(intBuffer, data); break;
      case 28: PackedIntsDecompress.decode28(intBuffer, data); break;
      case 29: PackedIntsDecompress.decode29(intBuffer, data); break;
      case 30: PackedIntsDecompress.decode30(intBuffer, data); break;
      case 31: PackedIntsDecompress.decode31(intBuffer, data); break;
      case 32: PackedIntsDecompress.decode32(intBuffer, data); break;
    }
  }

  static void encodeNormalValue(IntBuffer intBuffer, int pos, int value, int numBits) {
    final int globalBitPos = numBits*pos;           // position in bit stream
    final int localBitPos = globalBitPos & 31;      // position inside an int
    int intPos = globalBitPos/32; // which integer to locate 
    setBufferIntBits(intBuffer, intPos, localBitPos, numBits, value);
    if ((localBitPos + numBits) > 32) { // value does not fit in this int, fill tail
      setBufferIntBits(intBuffer, intPos+1, 0, 
                       (localBitPos+numBits-32), 
                       (value >>> (32-localBitPos)));
    }
  }

  static void setBufferIntBits(IntBuffer intBuffer, int intPos, int firstBitPos, int numBits, int value) {
    assert (value & ~MASK[numBits]) == 0;
    // safely discards those msb parts when firstBitPos+numBits>32
    intBuffer.put(intPos,
          (intBuffer.get(intPos) & ~(MASK[numBits] << firstBitPos)) 
          | (value << firstBitPos));
  }

  /**
   * Estimate best num of frame bits according to the largest value.
   */
  static int getNumBits(final int[] data) {
    if (isAllEqual(data)) {
      return 0;
    }
    int size=data.length;
    int optBits=1;
    for (int i=0; i<size; ++i) {
      while ((data[i] & ~MASK[optBits]) != 0) {
        optBits++;
      }
    }
    return optBits;
  }

  protected static boolean isAllEqual(final int[] data) {
    int len = data.length;
    int v = data[0];
    for (int i=1; i<len; i++) {
      if (data[i] != v) {
        return false;
      }
    }
    return true;
  }

  /** 
   * Generate the 4 byte header, which contains (from lsb to msb):
   *
   * 8 bits for encoded block int size (excluded header, this limits DEFAULT_BLOCK_SIZE <= 2^8)
   * 6 bits for num of frame bits (when 0, values in this block are all the same)
   * other bits unused
   *
   */
  static int getHeader(int encodedSize, int numBits) {
    return  (encodedSize)
          | ((numBits) << 8);
  }

  /** 
   * Expert: get metadata from header. 
   */
  public static int getEncodedSize(int header) {
    return ((header & MASK[8]))*4;
  }
  public static int getNumBits(int header) {
    return ((header >> 8) & MASK[6]);
  }
}
