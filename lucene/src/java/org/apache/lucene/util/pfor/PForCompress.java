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
// nocommit merge w/ pfor2 before landing to trunk!

import java.util.Arrays;

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
public class PForCompress extends ForCompress {
  /** Index on input and in compressed frame of first exception, -1 when no exceptions */
  private int firstExceptionIndex;
  
  /** How to encode PFor exceptions: 0: byte, 1: short, 2:int, unused: 3: long */
  private int exceptionCode = -1;
  
  /** Total number of exception values */
  private int numExceptions;
  
  /** Compress the decompressed data into the buffer.
   * Should only be used after setUnCompressedData().
   * <br>
   * When setCompressBuffer() was not done, no actual compression is done.
   * Regardless of the use of setCompressBuffer(), bufferByteSize() will return
   * a valid value after calling compress().
   * <p>
   * When a buffer is available, the following is done.
   * A header is stored into the buffer, encoding a.o. numFrameBits and unComprSize.
   * All ints < 2**numFrameBits are stored sequentially in compressed form
   * in the buffer.
   * All other ints are stored in the buffer as exceptions after the compressed sequential ints,
   * using 1, 2 or 4 bytes per exception, starting at the first byte after the compressed
   * sequential ints.
   * <br>
   * The index of the first exception is encoded in the header in the buffer,
   * all later exceptions have the offset to the next exception as their value,
   * the last one offset to just after the available input size.
   * After the first exception, when the next exception index does not fit in
   * numFrameBits bits, an exception after 2**numFrameBits inputs is forced and inserted.
   * <br>
   * Exception values are stored in the order of the exceptions.
   * The number of bytes used for an exception is also encoded in the header.
   * This depends on the maximum exception value and does not vary between the exceptions.
   */
  @Override
  public void compress(int numFrameBits) {
    assert numFrameBits >= 1;
    assert numFrameBits <= 32;
    this.numFrameBits = numFrameBits;
    numExceptions = 0;
    int maxException = -1;
    firstExceptionIndex = -1;
    int lastExceptionIndex = -1;
    int i;
    int[] exceptionValues = new int[unComprSize];
    int maxNonExceptionMask = (int) ((1L << numFrameBits) - 1);
    int maxChain = 254; // maximum value of firstExceptionIndex in header
    // CHECKME: maxChain 1 off because of initial value of lastExceptionIndex and force exception test below?
    for (i = 0; i < unComprSize; i++) {
      int v = unCompressedData[i + offset];
      // FIXME: split this loop to avoid if statement in loop.
      // use predication for this: (someBool ? 1 : 0), and hope that the jit optimizes this.
      if ( (((v & maxNonExceptionMask) == v) // no value exception
           && (i < (lastExceptionIndex + maxChain)))) { // no forced exception
        encodeCompressedValue(i, v); // normal encoding
      } else { // exception
        exceptionValues[numExceptions] = v;
        numExceptions++;
        if (firstExceptionIndex == -1) {
          firstExceptionIndex = i;
          assert firstExceptionIndex <= 254; // maximum value of firstExceptionIndex in header
          maxException = v;
          maxChain = 1 << ((30 < numFrameBits) ? 30 : numFrameBits); // fewer bits available for exception chain value. 
        } else if (v > maxException) {
          maxException = v;
        }
        // encode the previous exception pointer
        if (lastExceptionIndex >= 0) {
          encodeCompressedValue(lastExceptionIndex, i - lastExceptionIndex - 1);
        }
        lastExceptionIndex = i;
      }
    }
    if (lastExceptionIndex >= 0) {
      encodeCompressedValue(lastExceptionIndex, i - lastExceptionIndex - 1); // end the exception chain.
    }
    //int bitsInArray = numFrameBits * unCompressedData.length;
    //int bytesInArray = (bitsInArray + 7) / 8;
    if (maxException < (1 << 8)) { // exceptions as byte
      exceptionCode = 0;
    } else if (maxException < (1 << 16)) { // exceptions as 2 bytes
      exceptionCode = 1;
    } else /* if (maxException < (1L << 32)) */ { // exceptions as 4 bytes
      exceptionCode = 2;
    }
    encodeHeader(unComprSize, firstExceptionIndex);
    encodeExceptionValues(exceptionValues);
  }

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
  
  private void encodeExceptionValues(int[] exceptionValues) {
    if ((compressedBuffer == null) || (numExceptions == 0)) {
      return;
    }
    int excByteOffset = compressedArrayByteSize();

    switch (exceptionCode) {
      case 0: { // 1 byte exceptions
        int i = 0;
        do { 
          int intIndex = ForConstants.COMPRESSED_INDEX + (excByteOffset >> 2); // round down here.
          setBufferIntBits(intIndex, ((excByteOffset & 3) * 8), 8, exceptionValues[i]);
          excByteOffset++;
        } while (++i < numExceptions);
      }
      break;

      case 1: { // 2 byte exceptions
        int excShortOffset = (excByteOffset + 1) >> 1; // to next multiple of two bytes.
        int intIndex = ForConstants.COMPRESSED_INDEX + (excShortOffset >> 1); // round down here.
        int i = 0;
        if ((excShortOffset & 1) != 0) { // encode first 2 byte exception in high 2 bytes of same int as last frame bits.
          setBufferIntBits(intIndex, 16, 16, exceptionValues[i]);
          intIndex++;
          i++;
        }
        for (; i < (numExceptions-1); i += 2) {
          compressedBuffer.put(intIndex++, (exceptionValues[i+1] << 16) | exceptionValues[i]);
        }
        if (i < numExceptions) {
          compressedBuffer.put(intIndex, exceptionValues[i]); // also clear the high 16 bits
        }
      }
      break;

      case 2: { // 4 byte exceptions
        int excIntOffSet = ForConstants.COMPRESSED_INDEX + ((excByteOffset + 3) >> 2); // to next multiple of four bytes, in ints.
        int i = 0;
        do {
          compressedBuffer.put(excIntOffSet + i, exceptionValues[i]);
        } while(++i < numExceptions);
      }
      break;
    }
  }

  /** The 4 byte header (32 bits) contains:
   *
   * - 4 bits for the compression method: 0b0001 for PFor
   * - 4 bits unused
   *
   * - 5 bits for (numFrameBits-1)
   * - 2 bits for the exception code: 0b00: byte, 0b01: short, 0b10: int, 0b11: long (unused).
   * - 1 bit unused
   *
   * - 8 bits for uncompressed input size - 1,
   *
   * - 8 bits for the index of the first exception + 1, (0 when no exceptions)
   */
  private void encodeHeader(int unComprSize, int firstExceptionIndex) {
    assert exceptionCode >= 0;
    assert exceptionCode <= 2; // 3 for long, but unused for now.
    assert numFrameBits >= 1;
    assert numFrameBits <= 32;
    assert unComprSize >= 1;
    assert unComprSize <= 128;
    assert firstExceptionIndex >= -1;
    assert firstExceptionIndex < unComprSize;
    if (compressedBuffer != null) {
      compressedBuffer.put(ForConstants.HEADER_INDEX,
              ((firstExceptionIndex+1) << 24)
            | ((unComprSize-1) << 16)
            | ((exceptionCode & 3) << 13) | ((numFrameBits-1) << 8) 
            | (ForConstants.PFOR_COMPRESSION << 4));
    }
  }

  /** Determine the number of frame bits to be used for compression.
   * Use only after setUnCompressedData().
   * This is done by taking a copy of the input, sorting it and using this
   * to determine the compressed size for each possible numbits in a single pass,
   * ignoring forced exceptions.
   * Finally an estimation of the number of forced exceptions is reduced to
   * less than 1 in 32 input numbers by increasing the number of frame bits.
   * This implementation works by determining the total number of bytes needed for
   * the compressed data, but does take into account alignment of exceptions
   * at 2 or 4 byte boundaries.
   */
  @Override
  public int frameBitsForCompression() {
    if ((offset + unComprSize) > unCompressedData.length) {
      throw new IllegalArgumentException( "(offset " + offset
                                          + " + unComprSize " + unComprSize
                                          + ") > unCompressedData.length " + unCompressedData.length);
    }
    int copy[] = Arrays.copyOfRange(unCompressedData, offset, offset + unComprSize);
    assert copy.length == unComprSize;
    Arrays.sort(copy);
    int maxValue = copy[copy.length-1];
    if (maxValue <= 1) {
      return 1;
    }
    int bytesPerException = (maxValue < (1 << 8)) ? 1 : (maxValue < (1 << 16)) ? 2 : 4;
    int frameBits = 1;
    int bytesForFrame = (copy.length * frameBits + 7) / 8;
    // initially assume all input is an exception.
    int totalBytes = bytesForFrame + copy.length * bytesPerException; // excluding the header.
    int bestBytes = totalBytes;
    int bestFrameBits = frameBits;
    int bestExceptions = copy.length;
    for (int i = 0; i < copy.length; i++) {
      // determine frameBits so that copy[i] is no more exception
      while (copy[i] >= (1 << frameBits)) {
        if (frameBits == 30) { // no point to increase further.
          return bestFrameBits;
        }
        ++frameBits;
        // increase bytesForFrame and totalBytes to correspond to frameBits
        int newBytesForFrame = (copy.length * frameBits + 7) / 8;
        totalBytes += newBytesForFrame - bytesForFrame;
        bytesForFrame = newBytesForFrame;
      }
      totalBytes -= bytesPerException; // no more need to store copy[i] as exception
      if (totalBytes <= bestBytes) { // <= : prefer fewer exceptions at higher number of frame bits.
        bestBytes = totalBytes;
        bestFrameBits = frameBits;
        bestExceptions = (copy.length - i - 1);
      }
    }
    if (bestExceptions > 0) { // check for forced exceptions.
      // This ignores the position of the first exception, for which enough bits are available in the header.
      int allowedNumExceptions = bestExceptions + (copy.length >> 5); // 1 in 32 is allowed to be forced.
      // (copy.length >> bestFrameBits): Minimum exception chain size including forced ones,
      // ignoring the position of the first exception.
      while (allowedNumExceptions < (copy.length >> bestFrameBits)) { // Too many forced?
        bestFrameBits++; // Reduce forced exceptions and perhaps reduce actual exceptions
        // Dilemma: decompression speed reduces with increasing number of frame bits,
        // so it may be better to increase no more than once or twice here.
      }
    }
    return bestFrameBits;
  }
}
