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
// nocommit: this is only a test verison, change from PForUtil.java
import java.nio.IntBuffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

// Encode all values in normal area, based on the bit size for max value
public final class ForUtil {
  public static final int HEADER_INT_SIZE=1;
  private static final int[] MASK = {   0x00000000,
    0x00000001, 0x00000003, 0x00000007, 0x0000000f, 0x0000001f, 0x0000003f,
    0x0000007f, 0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff,
    0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff, 0x0001ffff, 0x0003ffff,
    0x0007ffff, 0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff,
    0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff,
    0x7fffffff, 0xffffffff};
  private static final int[] PER_EXCEPTION_SIZE = {1,2,4};

  public static int compress(final int[] data, int size, IntBuffer intBuffer) {
    int numBits=getNumBits(data,size);
  
    for (int i=0; i<size; ++i) {
      encodeNormalValue(intBuffer,i,data[i], numBits);
    }
    // encode header
    encodeHeader(intBuffer, size, numBits);

    return (HEADER_INT_SIZE+(size*numBits+31)/32)*4;
  }
  
  public static int decompress(IntBuffer intBuffer, int[] data) {
    intBuffer.rewind();
    int header = intBuffer.get();

    int numInts = (header & MASK[8]) + 1;
    int numBits = ((header >> 8) & MASK[5]) + 1;

    // TODO: ForDecompressImpl is hardewired to size==128 only
    switch(numBits) {
      case 1: ForDecompressImpl.decode1(intBuffer, data); break;
      case 2: ForDecompressImpl.decode2(intBuffer, data); break;
      case 3: ForDecompressImpl.decode3(intBuffer, data); break;
      case 4: ForDecompressImpl.decode4(intBuffer, data); break;
      case 5: ForDecompressImpl.decode5(intBuffer, data); break;
      case 6: ForDecompressImpl.decode6(intBuffer, data); break;
      case 7: ForDecompressImpl.decode7(intBuffer, data); break;
      case 8: ForDecompressImpl.decode8(intBuffer, data); break;
      case 9: ForDecompressImpl.decode9(intBuffer, data); break;
      case 10: ForDecompressImpl.decode10(intBuffer, data); break;
      case 11: ForDecompressImpl.decode11(intBuffer, data); break;
      case 12: ForDecompressImpl.decode12(intBuffer, data); break;
      case 13: ForDecompressImpl.decode13(intBuffer, data); break;
      case 14: ForDecompressImpl.decode14(intBuffer, data); break;
      case 15: ForDecompressImpl.decode15(intBuffer, data); break;
      case 16: ForDecompressImpl.decode16(intBuffer, data); break;
      case 17: ForDecompressImpl.decode17(intBuffer, data); break;
      case 18: ForDecompressImpl.decode18(intBuffer, data); break;
      case 19: ForDecompressImpl.decode19(intBuffer, data); break;
      case 20: ForDecompressImpl.decode20(intBuffer, data); break;
      case 21: ForDecompressImpl.decode21(intBuffer, data); break;
      case 22: ForDecompressImpl.decode22(intBuffer, data); break;
      case 23: ForDecompressImpl.decode23(intBuffer, data); break;
      case 24: ForDecompressImpl.decode24(intBuffer, data); break;
      case 25: ForDecompressImpl.decode25(intBuffer, data); break;
      case 26: ForDecompressImpl.decode26(intBuffer, data); break;
      case 27: ForDecompressImpl.decode27(intBuffer, data); break;
      case 28: ForDecompressImpl.decode28(intBuffer, data); break;
      case 29: ForDecompressImpl.decode29(intBuffer, data); break;
      case 30: ForDecompressImpl.decode30(intBuffer, data); break;
      case 31: ForDecompressImpl.decode31(intBuffer, data); break;
      case 32: ForDecompressImpl.decode32(intBuffer, data); break;
      default:
        throw new IllegalStateException("Unknown numFrameBits " + numBits);
    }
    return numInts;
  }

  static void encodeHeader(IntBuffer intBuffer, int numInts, int numBits) {
    int header = getHeader(numInts,numBits);
    intBuffer.put(0, header);
  }

  static void encodeNormalValue(IntBuffer intBuffer, int pos, int value, int numBits) {
    final int globalBitPos = numBits*pos;         // position in bit stream
    final int localBitPos = globalBitPos & 31;    // position inside an int
    int intPos = HEADER_INT_SIZE + globalBitPos/32;   // which integer to locate 
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

  // TODO: shall we use 32 NumBits directly if it exceeds 28 bits?
  static int getNumBits(final int[] data, int size) {
    int optBits=1;
    for (int i=0; i<size; ++i) {
      while ((data[i] & ~MASK[optBits]) != 0) {
        optBits++;
      }
    }
    return optBits;
  }
  /** The 4 byte header (32 bits) contains (from lsb to msb):
   *
   * - 8 bits for uncompressed int num - 1 (use up to 7 bits i.e 128 actually)
   *
   * - 5 bits for num of frame bits - 1
   *
   * - other bits unused
   *
   */
  static int getHeader(int numInts, int numBits) {
    return  (numInts-1)
          | ((numBits-1) << 8);
  }

  static void println(String format, Object... args) {
    System.out.println(String.format(format,args)); 
  }
  static void print(String format, Object... args) {
    System.out.print(String.format(format,args)); 
  }
  static void eprintln(String format, Object... args) {
    System.err.println(String.format(format,args)); 
  }
  public static String getHex( byte [] raw, int sz ) {
    final String HEXES = "0123456789ABCDEF";
    if ( raw == null ) return null;
    final StringBuilder hex = new StringBuilder( 2 * raw.length );
    for ( int i=0; i<sz; i++ ) {
      if (i>0 && (i)%16 == 0)
        hex.append("\n");
      byte b=raw[i];
      hex.append(HEXES.charAt((b & 0xF0) >> 4))
         .append(HEXES.charAt((b & 0x0F)))
         .append(" ");
    }
    return hex.toString();
  }
  public static String getHex( int [] raw, int sz ) {
    if ( raw == null ) return null;
    final StringBuilder hex = new StringBuilder( 4 * raw.length );
    for ( int i=0; i<sz; i++ ) {
      if (i>0 && i%8 == 0)
        hex.append("\n");
      hex.append(String.format("%08x ",raw[i]));
    }
    return hex.toString();
  }
}
