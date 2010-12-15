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

// nocommit need low level unit tests for this
// nocommit break out decompress seperately?

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
public class PForDecompress extends ForDecompress {
  /** Index on input and in compressed frame of first exception, -1 when no exceptions */
  private int firstExceptionIndex;
  
  /** How to encode PFor exceptions: 0: byte, 1: short, 2:int, unused: 3: long */
  private int exceptionCode = -1;
  
  /** Total number of exception values */
  private int numExceptions;
  
  /** Return the number bytes used for a single exception */
  private int exceptionByteSize() {
    assert exceptionCode >= 0;
    assert exceptionCode <= 2;
    return exceptionCode == 0 ? 1
          : exceptionCode == 1 ? 2
          : 4;
  }
  
  /** Return the number of exceptions.
   *  Only valid after compress() or decompress().
   */
  public int getNumExceptions() {
    return numExceptions;
  }

  private int compressedArrayByteSize() {
    assert unComprSize % 32 == 0;
    return (unComprSize>>3)*numFrameBits;
  }

  /** Return the number of integers used in IntBuffer.
   *  Only valid after compress() or decompress().
   */
  @Override
  public int compressedSize() {
    // numExceptions only valid after compress() or decompress()
    return ForConstants.HEADER_SIZE
           + ((compressedArrayByteSize()
               + exceptionByteSize() * numExceptions
               + 3) >> 2); // round up to next multiple of 4 and divide by 4
  }
  
  /** Decode the exception values while going through the exception chain.
   * <br>For performance, delegate/subclass this to classes with fixed exceptionCode.
   * <br> Also, decoding exceptions is preferably done from an int border instead of
   * from a random byte directly after the compressed array. This will allow faster
   * decoding of exceptions, at the cost of at most 3 bytes.
   * <br>When ((numFrameBits * unComprSize) % 32) == 0, this cost will always be
   * zero bytes so specialize for these cases.
   */
  private void patchExceptions() {
    numExceptions = 0;
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
          if (excIndex >= unComprSize) {
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
        while (excIndex < unComprSize) {
          final int curIntValue = compressedBuffer.get();
          int excValue = curIntValue & ((1<<16)-1);
          excIndex = patch(excIndex, excValue);
          if (excIndex >= unComprSize) {
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
        } while (excIndex < unComprSize);
      break;
    }
  }

  @Override
  protected void decodeHeader() {
    int header = compressedBuffer.get();
    firstExceptionIndex = ((header >>> 24) & 255) - 1; 
    //unComprSize = ((header >>> 16) & 255) + 1;
    numFrameBits = ((header >>> 8) & 31) + 1;
    assert numFrameBits > 0: numFrameBits;
    assert numFrameBits <= 32: numFrameBits;
    // verify compression method:
    assert ForConstants.PFOR_COMPRESSION == ((header >>> 4) & 15);
    exceptionCode = (header >>> 13) & 3;
    assert exceptionCode <= 2;
  }

  /** Decompress from the buffer into output from a given offset. */
  @Override
  public void decompress() {
    super.decompress();
    patchExceptions();
  }
  
  /** Patch and return index of next exception */
  private int patch(int excIndex, int excValue) {
    int nextExceptionIndex = unCompressedData[excIndex] + excIndex + 1; // chain offset
    unCompressedData[excIndex + offset] = excValue; // patch
    assert nextExceptionIndex > excIndex;
    numExceptions++;
    return nextExceptionIndex;
  }
}
