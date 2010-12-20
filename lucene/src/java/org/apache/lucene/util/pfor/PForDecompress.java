package org.apache.lucene.util.pfor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.lucene.store.DataInput;
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

// nocommit need low level unit tests for this

/** Patched Frame of Reference PFOR compression/decompression.
 * <p>
 * As defined in:<br>
 * Super-Scalar RAM-CPU Cache Compression<br>
 * Marcin Zukowski, Sándor Héman, Niels Nes, Peter Boncz, 2006.<br>
 * with extensions from:<br>
 * Performance of Compressed Inverted List Caching in Search Engines<br>
 * Jiangong Zhang, Xiaohui Long, Torsten Suel, 2008.<br>
 * <p>
 * This class does not provide delta coding because the lucene index
 * structures already have that.
 * <p>
 * The implementation uses 0 as lower bound for the frame,
 * so small positive integers will be most effectively compressed.
 * <p>
 * Some optimized code is used for decompression,
 * see class ForDecompress and its subclasses.
 * <br>Good decompression performance will depend on the performance
 * of java.nio.IntBuffer indexed get() methods.
 * <br>Use of the -server option helps performance for the Sun 1.6 jvm under Linux.
 * <p>
 * The start point of first exception is at its natural boundary:
 * 2 byte exceptions at even byte position, 4 byte at quadruple.
 * <p>
 * To be done:
 * <ul>
 * <li>
 * Optimize compression code.
 * <li>
 * IntBuffer.get() is somewhat faster that IntBuffer.get(index), adapt (de)compression for to
 * use the relative get() method.
 * <li>
 * Check javadoc generation and generated javadocs. Add javadoc code references.
 * </ul>
 */
//nocommit: make into static methods without state
// nocommit -- need serious random unit test for these int encoders
public final class PForDecompress  {
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
  
  public PForDecompress(DataInput in, int out[], int offset, int len) {
    this.in = in;
    this.out = out;
    this.offset = offset;
    this.len = len;
    byteBuffer = ByteBuffer.allocate(1024);
    input = byteBuffer.array();
    compressedBuffer = byteBuffer.asIntBuffer();
  }
  
  /** Index on input and in compressed frame of first exception, -1 when no exceptions */
  private int firstExceptionIndex;
  
  /** How to encode PFor exceptions: 0: byte, 1: short, 2:int, unused: 3: long */
  private int exceptionCode = -1;

  /** Decode the exception values while going through the exception chain.
   * <br>For performance, delegate/subclass this to classes with fixed exceptionCode.
   * <br> Also, decoding exceptions is preferably done from an int border instead of
   * from a random byte directly after the compressed array. This will allow faster
   * decoding of exceptions, at the cost of at most 3 bytes.
   * <br>When ((numFrameBits * unComprSize) % 32) == 0, this cost will always be
   * zero bytes so specialize for these cases.
   */
  private void patchExceptions() {
    if (firstExceptionIndex == -1) {
      return;
    }

    int excIndex = firstExceptionIndex;

    switch (exceptionCode) {
      case 0: { // 1 byte exceptions
        int curIntValue = compressedBuffer.get();
        int firstBitPosition = 0;
        while(true) {
          final int excValue = (curIntValue >>> firstBitPosition) & ((1 << 8) - 1);
          excIndex = patch(excIndex, excValue);
          if (excIndex >= len) {
            break;
          }
          firstBitPosition += 8;
          if (firstBitPosition == 32) {
            firstBitPosition = 0;
            curIntValue = compressedBuffer.get();
          }
        }
      }
      break;

      case 1: { // 2 byte exceptions
        while (excIndex < len) {
          final int curIntValue = compressedBuffer.get();
          int excValue = curIntValue & ((1<<16)-1);
          excIndex = patch(excIndex, excValue);
          if (excIndex >= len) {
            break;
          }
          excValue = curIntValue >>> 16;
          excIndex = patch(excIndex, excValue);
        }
      }
      break;

      case 2: // 4 byte exceptions
        do {
          excIndex = patch(excIndex, compressedBuffer.get());
        } while (excIndex < len);
      break;
    }
  }

  /** Decompress from the buffer into output from a given offset. */
  public void decompress() throws IOException {
    int numBytes = in.readInt(); // nocommit: is it possible to encode # of exception bytes in header?
    in.readBytes(input, 0, numBytes);
    compressedBuffer.rewind();
    int header = compressedBuffer.get();
    final int numFrameBits = ((header >>> 8) & 31) + 1;

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
    firstExceptionIndex = ((header >>> 24) & 255) - 1; 
    exceptionCode = (header >>> 13) & 3;
    assert exceptionCode <= 2;
    patchExceptions();
  }
  
  /** Patch and return index of next exception */
  private int patch(int excIndex, int excValue) {
    int nextExceptionIndex = out[excIndex] + excIndex + 1; // chain offset
    out[excIndex + offset] = excValue; // patch
    assert nextExceptionIndex > excIndex;
    return nextExceptionIndex;
  }
}
