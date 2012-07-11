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
 * Encode all small values and exception pointers in normal area;
 * Encode large values in exception area;
 * Size per exception is variable, possibly: 1byte, 2bytes, or 4bytes
 */
public final class PForUtil extends ForUtil {

  protected static final int[] PER_EXCEPTION_SIZE = {1,2,4};

  /** Compress given int[] into Integer buffer, with PFor format
   *
   * @param data        uncompressed data
   * @param size        num of ints to compress
   * @param intBuffer   integer buffer to hold compressed data
   */
  public static int compress(final int[] data, int size, IntBuffer intBuffer) {
    /** estimate minimum compress size to determine numFrameBits */
    int numBits=getNumBits(data,size);
  
    int[] excValues = new int[size];
    int excNum = 0, excLastPos = -1, excFirstPos = -1, excLastNonForcePos = -1; 

    // num of exception until the last non-forced exception
    int excNumBase = 0;          

    // bytes per exception
    int excBytes = 1;

    // bytes before exception area, e.g. header and normal area
    int excByteOffset = 0;

    // the max value possible for current exception pointer, 
    // value of the first pointer is limited by header as 254
    long maxChain = (1<<8) - 2;  

    boolean conValue, conForce, conEnd;
    int i=0;

    /** estimate exceptions */
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

    /** encode normal area, record exception positions */
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
  
    /** encode exception area */
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

    /** encode header */
    encodeHeader(intBuffer, size, numBits, excNum, excFirstPos, excBytes);

    return (excByteOffset + excBytes*excNum + 3)/4*4;
  }
  
  /** Decompress given Integer buffer into int array.
   *
   * @param intBuffer   integer buffer to hold compressed data
   * @param data        int array to hold uncompressed data
   */
  public static int decompress(IntBuffer intBuffer, int[] data) {

    // since this buffer is reused at upper level, rewind first
    intBuffer.rewind();

    int header = intBuffer.get();
    int numInts = (header & MASK[8]) + 1;
    int excNum = ((header >> 8) & MASK[8]) + 1;
    int excFirstPos = ((header >> 16) & MASK[8]) - 1;
    int excBytes = PER_EXCEPTION_SIZE[(header >> 29) & MASK[2]];
    int numBits = ((header >> 24) & MASK[5]) + 1;

    decompressCore(intBuffer, data, numBits);

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

  /**
   * Decode exception values base on the exception pointers in normal area,
   * and values in exception area.
   * As for current implementation, numInts is hardwired as 128, so the
   * tail of normal area is naturally aligned to 32 bits, and we don't need to
   * rewind intBuffer here.
   * However, the normal area may share a same int with exception area, 
   * when numFrameBits * numInts % 32 != 0,
   * In this case we should preprocess patch several heading exceptions, 
   * before calling this method.
   *
   * TODO: blockSize is hardewired to size==128 only
   */
  public static void patchException(IntBuffer intBuffer, int[] data, int excNum, int excFirstPos, int excBytes) {
    if (excFirstPos == -1) {
      return;
    }
    int curPos=excFirstPos;
    int i,j;

    if (excBytes == 1) { // each exception consumes 1 byte
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
    } else if (excBytes == 2) { // each exception consumes 2 bytes
      for (i=0; i+1<excNum; i+=2) {
        final int curInt = intBuffer.get();
        curPos = patch(data, curPos, (curInt) & MASK[16]);
        curPos = patch(data, curPos, (curInt >>> 16) & MASK[16]);
      }
      if (i<excNum) {
        final int curInt = intBuffer.get();
        curPos = patch(data, curPos, (curInt) & MASK[16]);
      }
    } else if (excBytes == 4) { // each exception consumes 4 bytes
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

  /**
   * Estimate best number of frame bits according to minimum compressed size.
   * It will run 32 times.
   */
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

  /**
   * Iterate the whole block to get maximum exception bits, 
   * and estimate compressed size without forced exception.
   * TODO: foresee forced exception for better estimation
   */
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

  /** 
   * Generate the 4 byte header, which contains (from lsb to msb):
   *
   * 8 bits for uncompressed int num - 1 (use up to 7 bits i.e 128 actually)
   *
   * 8 bits for exception num - 1 (when no exceptions, this is undefined)
   *
   * 8 bits for the index of the first exception + 1 (when no exception, this is 0)
   *
   * 5 bits for num of frame bits - 1
   * 2 bits for the exception code: 00: byte, 01: short, 10: int
   * 1 bit unused
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
