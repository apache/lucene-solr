package org.apache.lucene.codecs.pfor;
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
import java.nio.ByteBuffer;
import java.util.Arrays;

// Encode all small values and exception pointers in normal area, 
// Encode large values in exception area.
// Size per exception is variable, possibly: 1byte, 2bytes, or 4bytes
public final class PForUtil extends ForUtil {
  protected static final int[] PER_EXCEPTION_SIZE = {1,2,4};

  public static int compress(final int[] data, int size, IntBuffer intBuffer) {
    int numBits=getNumBits(data,size);
  
    int[] excValues = new int[size];
    int excNum = 0, excLastPos = -1, excFirstPos = -1;
    int excLastNonForcePos = -1; 
    int excNumBase = 0;       // num of exception until the last non-force exception
    int excBytes = 1;         // bytes per exception
    int excByteOffset = 0;    // bytes of preceeding codes like header and normal area
    long maxChain = (1<<8) - 2;  // header bits limits this to 254
    boolean conValue, conForce, conEnd;
    int i=0;

    // estimate exceptions
    for (i=0; i<size; ++i) {
      conValue = ((data[i] & MASK[numBits]) != data[i]); // value exception
      conForce = (i >= maxChain + excLastPos);           // force exception
      if (conValue || conForce) {
        excValues[excNum++] = data[i];
        if (excLastPos == -1) {
          maxChain = 1L<<numBits; 
          excFirstPos = i;
        }
        if (conValue) {
          excLastNonForcePos = i;
          excNumBase = excNum;
        }
        excLastPos = i;
      }
    }

    // encode normal area, record exception positions
    i=0;
    excNum = 0;
    if (excFirstPos < 0) { // no exception 
      for (; i<size; ++i) {
        encodeNormalValue(intBuffer,i,data[i], numBits);
      }
      excLastPos = -1;
    } else {
      for (; i<excFirstPos; ++i) {
        encodeNormalValue(intBuffer,i,data[i], numBits);
      }
      maxChain = 1L<<numBits;
      excLastPos = -1;
      for (; i<size; ++i) {
        conValue = ((data[i] & MASK[numBits]) != data[i]); // value exception
        conForce = (i >= maxChain + excLastPos);           // force exception
        conEnd = (excNum == excNumBase);                   // following forced ignored
        if ((!conValue && !conForce) || conEnd) {
          encodeNormalValue(intBuffer,i,data[i], numBits);
        } else {
          if (excLastPos >= 0) {
            encodeNormalValue(intBuffer, excLastPos, i-excLastPos-1, numBits); 
          }
          excNum++;
          excLastPos = i;
        }
      }
      if (excLastPos >= 0) { 
        encodeNormalValue(intBuffer, excLastPos, (i-excLastPos-1)&MASK[numBits], numBits); // mask out suppressed force exception
      }
    }
  
    // encode exception area
    i=0;
    for (; i<excNum; ++i) {
      if (excBytes < 2 && (excValues[i] & ~MASK[8]) != 0) {
        excBytes=2;
      }
      if (excBytes < 4 && (excValues[i] & ~MASK[16]) != 0) {
        excBytes=4;
      }
    }
    excByteOffset = HEADER_INT_SIZE*4 + (size*numBits + 7)/8;
    encodeExcValues(intBuffer, excValues, excNum, excBytes, excByteOffset);

    // encode header
    encodeHeader(intBuffer, size, numBits, excNum, excFirstPos, excBytes);

    return (excByteOffset + excBytes*excNum + 3)/4*4;
  }
  
  public static int decompress(IntBuffer intBuffer, int[] data) {
    intBuffer.rewind();
    int header = intBuffer.get();

    int numInts = (header & MASK[8]) + 1;
    int excNum = ((header >> 8) & MASK[8]) + 1;
    int excFirstPos = ((header >> 16) & MASK[8]) - 1;
    int excBytes = PER_EXCEPTION_SIZE[(header >> 29) & MASK[2]];
    int numBits = ((header >> 24) & MASK[5]) + 1;

    // TODO: PackedIntsDecompress is hardewired to size==128 only
    switch(numBits) {
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
      default:
        throw new IllegalStateException("Unknown numFrameBits " + numBits);
    }
    patchException(intBuffer,data,excNum,excFirstPos,excBytes);
    return numInts;
  }

  static void encodeHeader(IntBuffer intBuffer, int numInts, int numBits, int excNum, int excFirstPos, int excBytes) {
    int header = getHeader(numInts,numBits,excNum,excFirstPos,excBytes);
    intBuffer.put(0, header);
  }

  static void encodeExcValues(IntBuffer intBuffer, int[] values, int num, int perbytes, int byteOffset) {
    if (num == 0)
      return;
    if (perbytes == 1) {
      int curBytePos = byteOffset;
      for (int i=0; i<num; ++i) {
        int curIntPos = curBytePos / 4;
        setBufferIntBits(intBuffer, curIntPos, (curBytePos & 3)*8, 8, values[i]);
        curBytePos++;
      }
    } else if (perbytes == 2) {
      int shortOffset = (byteOffset+1)/2;
      int curIntPos = shortOffset/2;
      int i=0;
      if ((shortOffset & 1) == 1) {  // cut head to ensure remaining fit ints
        setBufferIntBits(intBuffer, curIntPos++, 16, 16, values[i++]); 
      }
      for (; i<num-1; i+=2) {
        intBuffer.put(curIntPos++, (values[i+1]<<16) | values[i]);
      }
      if (i<num) {
        intBuffer.put(curIntPos, values[i]); // cut tail, also clear high 16 bits
      }
    } else if (perbytes == 4) {
      int curIntPos = (byteOffset+3) / 4;
      for (int i=0; i<num; ++i) {
        intBuffer.put(curIntPos++, values[i]);
      }
    }
  }

  // TODO: since numInts===128, we don't need to rewind intBuffer.
  // however, tail of normal area may share a same int with head of exception area
  // which means patchException may lose heading exceptions.
  public static void patchException(IntBuffer intBuffer, int[] data, int excNum, int excFirstPos, int excBytes) {
    if (excFirstPos == -1) {
      return;
    }
    int curPos=excFirstPos;
    int i,j;

    if (excBytes == 1) {
      for (i=0; i+3<excNum; i+=4) {
        final int curInt = intBuffer.get();
        curPos = patch(data, curPos, (curInt) & MASK[8]);
        curPos = patch(data, curPos, (curInt >>> 8)  & MASK[8]);
        curPos = patch(data, curPos, (curInt >>> 16) & MASK[8]);
        curPos = patch(data, curPos, (curInt >>> 24) & MASK[8]);
      }
      if (i<excNum) { 
        final int curInt = intBuffer.get();
        for (j=0; j<32 && i<excNum; j+=8,i++) {
          curPos = patch(data, curPos, (curInt >>> j) & MASK[8]);
        }
      }
    } else if (excBytes == 2) {
      for (i=0; i+1<excNum; i+=2) {
        final int curInt = intBuffer.get();
        curPos = patch(data, curPos, (curInt) & MASK[16]);
        curPos = patch(data, curPos, (curInt >>> 16) & MASK[16]);
      }
      if (i<excNum) {
        final int curInt = intBuffer.get();
        curPos = patch(data, curPos, (curInt) & MASK[16]);
      }
    } else if (excBytes == 4) {
      for (i=0; i<excNum; i++) {
        curPos = patch(data, curPos, intBuffer.get());
      }
    }
  }

  static int patch(int[]data, int pos, int value) {
    int nextPos = data[pos] + pos + 1;
    data[pos] = value;
    assert nextPos > pos;
    return nextPos;
  }

  // TODO: shall we use 32 NumBits directly if it exceeds 28 bits?
  static int getNumBits(final int[] data, int size) {
    int optBits=1;
    int optSize=estimateCompressedSize(data,size,1);
    for (int i=2; i<=32; ++i) {
      int curSize=estimateCompressedSize(data,size,i);
      if (curSize<optSize) {
        optSize=curSize;
        optBits=i;
      }
    }
    return optBits;
  }

  // loosely estimate int size of each compressed block, based on parameter b
  // ignore force exceptions
  static int estimateCompressedSize(final int[] data, int size, int numBits) {
    int totalBytes=(numBits*size+7)/8;   // always round to byte
    int excNum=0;
    int curExcBytes=1;
    for (int i=0; i<size; ++i) {
      if ((data[i] & ~MASK[numBits]) != 0) {   // exception
        excNum++;
        if (curExcBytes<2 && (data[i] & ~MASK[8]) != 0) { // exceed 1 byte exception
          curExcBytes=2;
        }
        if (curExcBytes<4 && (data[i] & ~MASK[16]) != 0) { // exceed 2 byte exception
          curExcBytes=4;
        }
      }
    }
    if (curExcBytes==2) {
      totalBytes=((totalBytes+1)/2)*2;  // round up to 2x bytes before filling exceptions
    }
    else if (curExcBytes==4) {
      totalBytes=((totalBytes+3)/4)*4;  // round up to 4x bytes
    }
    totalBytes+=excNum*curExcBytes;

    return totalBytes/4*4+HEADER_INT_SIZE;  // round up to ints
  }
  /** The 4 byte header (32 bits) contains (from lsb to msb):
   *
   * - 8 bits for uncompressed int num - 1 (use up to 7 bits i.e 128 actually)
   *
   * - 8 bits for exception num - 1 (when no exceptions, this is undefined)
   *
   * - 8 bits for the index of the first exception + 1 (when no exception, this is 0)
   *
   * - 5 bits for num of frame bits - 1
   * - 2 bits for the exception code: 00: byte, 01: short, 10: int
   * - 1 bit unused
   *
   */
  static int getHeader(int numInts, int numBits, int excNum, int excFirstPos, int excBytes) {
    return  (numInts-1)
          | (((excNum-1) & MASK[8]) << 8)
          | ((excFirstPos+1) << 16)
          | ((numBits-1) << 24)
          | ((excBytes/2) << 29);
  }
}
