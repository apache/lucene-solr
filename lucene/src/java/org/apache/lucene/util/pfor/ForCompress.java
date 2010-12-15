package org.apache.lucene.util.pfor;

/**
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

import org.apache.lucene.util.BitUtil;

//nocommit: make into static methods without state
public class ForCompress {
  /** Number of frame bits. 2**numFrameBits - 1 is the maximum compressed value. */
  protected int numFrameBits;
  
  /** IntBuffer for compressed data */
  protected IntBuffer compressedBuffer;

  /** Uncompressed data */
  protected int[] unCompressedData;
  /** Offset into unCompressedData */
  protected int offset;
  /** Size of unCompressedData, -1 when not available. */
  protected int unComprSize = -1;
  
  /** Integer buffer to hold the compressed data.<br>
   *  Compression and decompression do not affect the current buffer position,
   *  and the beginning of the compressed data should be or will be at the current
   *  buffer position.<br>
   *  When the buffer is not large enough, ArrayIndexOutOfBoundExceptions will occur
   *  during compression/decompression.<br>
   *  Without a buffer for compressed data, compress() will only determine the number
   *  of integers needed in the buffer, see compress().<br>
   *  Without a valid buffer, decompress() will throw a NullPointerException.<br>
   *  For optimal speed when the IntBuffer is a view on a ByteBuffer,
   *  the IntBuffer should have a byte offset of a  multiple of 4 bytes, possibly 0. <br>
   *  An IntBuffer is used here because 32 bits can efficiently accessed in the buffer
   *  on all current processors, and a positive int is normally large enough
   *  for various purposes in a Lucene index.
   *
   * @param compressedBuffer    The buffer to hold the compressed integers.
   *                            
   */
  public void setCompressedBuffer(IntBuffer compressedBuffer) {
    this.compressedBuffer = compressedBuffer;
  }

  /** Array with offset holding uncompressed data.
   * @param unCompressedData The array holding uncompressed integers.
   * @param offset offset in unCompressedData.
   * @param unComprSize The number of uncompressed integers, should be at least 1.
   */
  public void setUnCompressedData(int[] unCompressedData, int offset, int unComprSize) {
    assert unCompressedData != null;
    assert offset >= 0;
    assert unComprSize >= 1;
    assert (offset + unComprSize) <= unCompressedData.length;
    this.unCompressedData = unCompressedData;
    this.offset = offset;
    this.unComprSize = unComprSize;
  }
  
  /** Compress the uncompressed data into the buffer using the given number of
   * frame bits, storing only this number of least significant bits of the
   * uncompressed integers in the compressed buffer.
   * Should only be used after setUnCompressedData().
   * <br>
   * When setCompressBuffer() was not done, no actual compression is done.
   * Regardless of the use of setCompressBuffer(), bufferByteSize() will return
   * a valid value after calling compress().
   * <p>
   * When a buffer is available, the following is done.
   * A header is stored as a first integer into the buffer, encoding
   * the compression method, the number of frame bits and the number of compressed integers.
   * All uncompressed integers are stored sequentially in compressed form
   * in the buffer after the header.
   *
   * @param numFrameBits        The number of frame bits. Should be between 1 and 32.
   *                            Note that when this value is 32, no compression occurs.
   */
  public void compress(int numFrameBits) {
    assert numFrameBits >= 1;
    assert numFrameBits <= 32;
    this.numFrameBits = numFrameBits;
    encodeHeader(unComprSize);
    for (int i = 0; i < unComprSize; i++) {
      int v = unCompressedData[i + offset];
      encodeCompressedValue(i, v);
    }
  }

  /** As compress(), using the result of frameBitsForCompression() as the number of frame bits. */
  public void compress() {
    compress( frameBitsForCompression());
  }
  
  /** Determine the number of frame bits to be used for compression.
   * Use only after setUnCompressedData().
   * @return The number of bits needed to encode the maximum positive uncompressed value.
   * Negative uncompressed values have no influence on the result.
   */
  public int frameBitsForCompression() {
    int maxNonNegVal = 0;
    for (int i = offset; i < (offset + unComprSize); i++) {
      if (unCompressedData[i] > maxNonNegVal) {
        maxNonNegVal = unCompressedData[i];
      }
    }
    return BitUtil.logNextHigherPowerOfTwo(maxNonNegVal) + 1;
  }
  
  /** The 4 byte header (32 bits) contains:
   * <ul>
   * <li>
   *  <ul>
   *  <li>4 bits for the compression method: 0b0001 for FrameOfRef,
   *  <li>4 bits unused,
   *  </ul>
   * <li>
   *  <ul>
   *  <li>5 bits for (numFrameBits-1),
   *  <li>3 bit unused,
   *  </ul>
   * <li>8 bits for number of compressed integers - 1,
   * <li>8 bits unused.
   * </ul>
   */
  private void encodeHeader(int unComprSize) {
    assert numFrameBits >= 1;
    assert numFrameBits <= (1 << 5); // 32
    assert unComprSize >= 1;
    assert unComprSize <= (1 << 8); // 256
    if (compressedBuffer != null) {
      compressedBuffer.put(ForConstants.HEADER_INDEX,
                    ((unComprSize-1) << 16)
                    | ((numFrameBits-1) << 8)
                    | (ForConstants.FOR_COMPRESSION << 4));
    }
  }
  
  /** Encode an integer value by compressing it into the buffer.
   * @param compressedPos The index of the compressed integer in the compressed buffer.
   * @param value The non negative value to be stored in compressed form.
   *              This should fit into the number of frame bits.
   */
  protected void encodeCompressedValue(int compressedPos, int value) {
    encodeCompressedValueBase(compressedPos, value, numFrameBits); // FIXME: inline private method.
  }

  /** Encode a value into the compressed buffer.
   * Since numFrameBits is always smaller than the number of bits in an int,
   * at most two ints in the buffer will be affected.
   * <br>Has no effect when compressedBuffer == null.
   * <br>This could be specialized for numBits just like decompressFrame().
   */
  private void encodeCompressedValueBase(int compressedPos, int value, int numBits) {
    assert numBits >= 1;
    assert numBits <= 32;
    if (compressedBuffer == null) {
      return;
    }
    final int mask = (int) ((1L << numBits) - 1);
    assert ((value & mask) == value) : ("value " + value + ", mask " + mask + ", numBits " + numBits); // lossless compression
    final int compressedBitPos = numBits * compressedPos;
    final int firstBitPosition = compressedBitPos & 31;
    int intIndex = ForConstants.COMPRESSED_INDEX + (compressedBitPos >> 5);
    setBufferIntBits(intIndex, firstBitPosition, numBits, value);
    if ((firstBitPosition + numBits) > 32) { // value does not fit in first int
      setBufferIntBits(intIndex+1, 0, (firstBitPosition + numBits - 32), (value >>> (32 - firstBitPosition)));
    }
  }
  
  /** Change bits of an integer in the compressed buffer.
   * <br> A more efficient implementation is possible when the compressed
   * buffer is known to contain only zero bits, in that case one mask operation can be removed.
   * @param intIndex The index of the affected integer in the compressed buffer.
   * @param firstBitPosition The position of the least significant bit to be changed.
   * @param numBits The number of more significant bits to be changed.
   * @param value The new value of the bits to be changed, with the least significant bit at position zero.
   */
  protected void setBufferIntBits(int intIndex, int firstBitPosition, int numBits, int value) {
    final int mask = (int) ((1L << numBits) - 1);
    compressedBuffer.put(intIndex,
          (compressedBuffer.get(intIndex)
            & ~ (mask << firstBitPosition)) // masking superfluous on clear buffer
          | (value << firstBitPosition));
  }
  
  /** Return the number of integers used in IntBuffer.
   *  Only valid after compress() or decompress().
   */
  public int compressedSize() {
    return ForConstants.HEADER_SIZE + (unComprSize * numFrameBits + 31) / 32;
  }
}
