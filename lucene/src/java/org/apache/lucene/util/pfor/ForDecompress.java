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

/** Frame of Reference lossless integer compression/decompression.
 * For positive integers, the compression is done by leaving out
 * the most significant bits, and storing all numbers with a fixed number of bits
 * contiguously in a buffer of bits. This buffer is called the frame, and it
 * can store positive numbers in a range from 0 to a constant maximum fitting in
 * the number of bits available for a single compressed number.
 * <p>
 * This implementation uses 0 as the lower bound reference for the frame,
 * so small positive integers can be most effectively compressed.
 * <p>
 * Optimized code is used for decompression, see class ForDecompress and its subclasses.
 * <br>Use of the -server option helps performance for the Sun 1.6 jvm under Linux.
 * <p>
 * This class does not provide delta coding because the Lucene index
 * structures already have that.
 * <p>
 * To be done:
 * <ul>
 * <li>
 * Optimize compression code by specializing for number of frame bits.
 * <li>
 * IntBuffer.get() is somewhat faster that IntBuffer.get(index), adapt (de)compression to
 * use the relative get() method.
 * <li>
 * Check javadoc generation and generated javadocs. Add javadoc code references.
 * </ul>
 */

//nocommit: make into static methods without state
public class ForDecompress {
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

  /** Return the number of integers used in IntBuffer.
   *  Only valid after compress() or decompress().
   */
  public int compressedSize() {
    return ForConstants.HEADER_SIZE + (unComprSize * numFrameBits + 31) / 32;
  }

  protected void decodeHeader() {
    int header = compressedBuffer.get();
    // nocommit -- we know this will always be fixed (eg
    // 128)?  silly to encode in every frame?
    //unComprSize = ((header >>> 16) & 255) + 1;
    numFrameBits = ((header >>> 8) & 31) + 1;
    // verify compression method:
    assert ForConstants.FOR_COMPRESSION == ((header >>> 4) & 15);
  }

  /** Decompress from the buffer into output from a given offset. */
  public void decompress() {
    decodeHeader();
    decompressFrame();
  }

  /** Return the number of integers available for decompression.
   * Do not use before an IntBuffer was passed to setCompressBuffer.
   */
  public int decompressedSize() {
    decodeHeader();
    return unComprSize;
  }
  
  /** For performance, this delegates to classes with fixed numFrameBits. */
  private void decompressFrame() {
    switch (numFrameBits) {
      // CHECKME: two other implementations might be faster:
      // - array of static methods: Method[numFrameBits].invoke(null, [this]), 
      // - array of non static decompressors: ForDecompressor[numFrameBits].decompressFrame(this) .
      case 1: ForDecompressImpl.decode1(compressedBuffer, unCompressedData); break;
      case 2: ForDecompressImpl.decode2(compressedBuffer, unCompressedData); break;
      case 3: ForDecompressImpl.decode3(compressedBuffer, unCompressedData); break;
      case 4: ForDecompressImpl.decode4(compressedBuffer, unCompressedData); break;
      case 5: ForDecompressImpl.decode5(compressedBuffer, unCompressedData); break;
      case 6: ForDecompressImpl.decode6(compressedBuffer, unCompressedData); break;
      case 7: ForDecompressImpl.decode7(compressedBuffer, unCompressedData); break;
      case 8: ForDecompressImpl.decode8(compressedBuffer, unCompressedData); break;
      case 9: ForDecompressImpl.decode9(compressedBuffer, unCompressedData); break;
      case 10: ForDecompressImpl.decode10(compressedBuffer, unCompressedData); break;
      case 11: ForDecompressImpl.decode11(compressedBuffer, unCompressedData); break;
      case 12: ForDecompressImpl.decode12(compressedBuffer, unCompressedData); break;
      case 13: ForDecompressImpl.decode13(compressedBuffer, unCompressedData); break;
      case 14: ForDecompressImpl.decode14(compressedBuffer, unCompressedData); break;
      case 15: ForDecompressImpl.decode15(compressedBuffer, unCompressedData); break;
      case 16: ForDecompressImpl.decode16(compressedBuffer, unCompressedData); break;
      case 17: ForDecompressImpl.decode17(compressedBuffer, unCompressedData); break;
      case 18: ForDecompressImpl.decode18(compressedBuffer, unCompressedData); break;
      case 19: ForDecompressImpl.decode19(compressedBuffer, unCompressedData); break;
      case 20: ForDecompressImpl.decode20(compressedBuffer, unCompressedData); break;
      case 21: ForDecompressImpl.decode21(compressedBuffer, unCompressedData); break;
      case 22: ForDecompressImpl.decode22(compressedBuffer, unCompressedData); break;
      case 23: ForDecompressImpl.decode23(compressedBuffer, unCompressedData); break;
      case 24: ForDecompressImpl.decode24(compressedBuffer, unCompressedData); break;
      case 25: ForDecompressImpl.decode25(compressedBuffer, unCompressedData); break;
      case 26: ForDecompressImpl.decode26(compressedBuffer, unCompressedData); break;
      case 27: ForDecompressImpl.decode27(compressedBuffer, unCompressedData); break;
      case 28: ForDecompressImpl.decode28(compressedBuffer, unCompressedData); break;
      case 29: ForDecompressImpl.decode29(compressedBuffer, unCompressedData); break;
      case 30: ForDecompressImpl.decode30(compressedBuffer, unCompressedData); break;
      case 31: ForDecompressImpl.decode31(compressedBuffer, unCompressedData); break;
      default:
        throw new IllegalStateException("Unknown number of frame bits " + numFrameBits);
    }
  }

  public int getNumFrameBits() {
    return numFrameBits;
  }
}
