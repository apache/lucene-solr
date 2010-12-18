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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.lucene.store.DataInput;

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
public final class ForDecompress {
  /** IntBuffer for compressed data */
  final IntBuffer compressedBuffer;

  /** Uncompressed data */
  final int[] out;
  /** Offset into unCompressedData */
  final int offset;
  /** Size of unCompressedData, -1 when not available. */
  final int len;

  // nocommit -- can't hardwire 1024; it's a function of blockSize
  final ByteBuffer byteBuffer;
  final byte input[];
  final DataInput in;
  
  public ForDecompress(DataInput in, int out[], int offset, int len) {
    this.in = in;
    this.out = out;
    this.offset = offset;
    this.len = len;
    byteBuffer = ByteBuffer.allocate(1024);
    input = byteBuffer.array();
    compressedBuffer = byteBuffer.asIntBuffer();
  }

  /** Decompress from the buffer into output from a given offset. */
  public void decompress() throws IOException {
    int header = in.readInt();
    final int numFrameBits = ((header >>> 8) & 31) + 1;
    in.readBytes(input, 0, numFrameBits << 4);
    compressedBuffer.rewind();
    switch (numFrameBits) {
      // CHECKME: two other implementations might be faster:
      // - array of static methods: Method[numFrameBits].invoke(null, [this]), 
      // - array of non static decompressors: ForDecompressor[numFrameBits].decompressFrame(this) .
      case 1: ForDecompressImpl.decode1(compressedBuffer, out); break;
      case 2: ForDecompressImpl.decode2(compressedBuffer, out); break;
      case 3: ForDecompressImpl.decode3(compressedBuffer, out); break;
      case 4: ForDecompressImpl.decode4(compressedBuffer, out); break;
      case 5: ForDecompressImpl.decode5(compressedBuffer, out); break;
      case 6: ForDecompressImpl.decode6(compressedBuffer, out); break;
      case 7: ForDecompressImpl.decode7(compressedBuffer, out); break;
      case 8: ForDecompressImpl.decode8(compressedBuffer, out); break;
      case 9: ForDecompressImpl.decode9(compressedBuffer, out); break;
      case 10: ForDecompressImpl.decode10(compressedBuffer, out); break;
      case 11: ForDecompressImpl.decode11(compressedBuffer, out); break;
      case 12: ForDecompressImpl.decode12(compressedBuffer, out); break;
      case 13: ForDecompressImpl.decode13(compressedBuffer, out); break;
      case 14: ForDecompressImpl.decode14(compressedBuffer, out); break;
      case 15: ForDecompressImpl.decode15(compressedBuffer, out); break;
      case 16: ForDecompressImpl.decode16(compressedBuffer, out); break;
      case 17: ForDecompressImpl.decode17(compressedBuffer, out); break;
      case 18: ForDecompressImpl.decode18(compressedBuffer, out); break;
      case 19: ForDecompressImpl.decode19(compressedBuffer, out); break;
      case 20: ForDecompressImpl.decode20(compressedBuffer, out); break;
      case 21: ForDecompressImpl.decode21(compressedBuffer, out); break;
      case 22: ForDecompressImpl.decode22(compressedBuffer, out); break;
      case 23: ForDecompressImpl.decode23(compressedBuffer, out); break;
      case 24: ForDecompressImpl.decode24(compressedBuffer, out); break;
      case 25: ForDecompressImpl.decode25(compressedBuffer, out); break;
      case 26: ForDecompressImpl.decode26(compressedBuffer, out); break;
      case 27: ForDecompressImpl.decode27(compressedBuffer, out); break;
      case 28: ForDecompressImpl.decode28(compressedBuffer, out); break;
      case 29: ForDecompressImpl.decode29(compressedBuffer, out); break;
      case 30: ForDecompressImpl.decode30(compressedBuffer, out); break;
      case 31: ForDecompressImpl.decode31(compressedBuffer, out); break;
      default:
        throw new IllegalStateException("Unknown number of frame bits " + numFrameBits);
    }
  }
}
