package org.apache.lucene.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

/**
 * Variable-length encoding of 32-bit integers, into 8-bit bytes. A number is encoded as follows:
 * <ul>
 * <li>If it is less than 127 and non-negative (i.e., if the number uses only 7 bits), it is encoded as 
 *  as single byte: 0bbbbbbb.
 * <li>If its highest nonzero bit is greater than bit 6 (0x40), it is represented as a series of
 * bytes, each byte's
 * 7 LSB containing bits from the original value, with the MSB set for all but the last
 * byte. The first encoded byte contains the highest nonzero bits from the
 * original; the second byte contains the next 7 MSB; and so on, with the last byte
 * containing the 7 LSB of the original.
 * </ul>
 * Examples: 
 * <ol>
 * <li>n = 117 = 1110101: This has fewer than 8 significant bits, and so is encoded as
 *   01110101 = 0x75.
 * <li>n = 100000 = (binary) 11000011010100000. This has 17 significant bits, and so needs 
 *   three Vint8 bytes. Left-zero-pad it to a multiple of 7 bits, then split it into chunks of 7 
 *   and add an MSB, 0 for the last byte, 1 for the others: 1|0000110 1|0001101 0|0100000
 *   = 0x86 0x8D 0x20.
 * </ol>   
 * This encoder/decoder will correctly handle any 32-bit integer, but for negative numbers,
 * and positive numbers with more than 28 significant bits, encoding requires 5 bytes; this
 * is not an efficient encoding scheme for large
 * positive numbers or any negative number.
 * <p>
 * <b>Compatibility:</b><br>
 * This class has been used in products that have shipped to customers, and is needed to
 * decode legacy data. Do not modify this class in ways that will break compatibility.
 * 
 * @lucene.experimental
 */
public class Vint8 {

  /**
   * Because Java lacks call-by-reference, this class boxes the decoding position, which
   * is initially set by the caller, and returned after decoding, incremented by the number
   * of bytes processed.
   */
  public static class Position {
    /**
     * Creates a position value set to zero.
     */
    public Position() {
      // The initial position is zero by default.
    }
    /**
     * Creates a position set to {@code initialPosition}.
     * @param initialPosition The starting decoding position in the source buffer.
     */
    public Position(int initialPosition) {
      this.pos = initialPosition;
    }
    /**
     * The value passed by reference.
     */
    public int pos;
  }

  /**
   * Returns the number of bytes needed to encode {@code number}.
   * @param number The number whose encoded length is needed.
   * @return The number of bytes needed to encode {@code number}.
   */
  public static int bytesNeeded(int number) {
    if ((number & ~0x7F) == 0) {
      return 1;
    } else if ((number & ~0x3FFF) == 0) {
      return 2;
    } else if ((number & ~0x1FFFFF) == 0) {
      return 3;
    } else if ((number & ~0xFFFFFFF) == 0) {
      return 4;
    } else {
      return 5;
    }
  }

  /**
   * The maximum number of bytes needed to encode a number using {@code Vint8}.
   */
  public static final int MAXIMUM_BYTES_NEEDED = 5;

  /**
   * Encodes {@code number} to {@code out}.
   * @param number The value to be written in encoded form, to {@code out}.
   * @param out The output stream receiving the encoded bytes.
   * @exception IOException If there is a problem writing to {@code out}.
   */
  public static void encode(int number, OutputStream out) throws IOException {
    if ((number & ~0x7F) == 0) {
      out.write(number);
    } else if ((number & ~0x3FFF) == 0) {
      out.write(0x80 | (number >> 7));
      out.write(0x7F & number);
    } else if ((number & ~0x1FFFFF) == 0) {
      out.write(0x80 | (number >> 14));
      out.write(0x80 | (number >> 7));
      out.write(0x7F & number);
    } else if ((number & ~0xFFFFFFF) == 0) {
      out.write(0x80 | (number >> 21));
      out.write(0x80 | (number >> 14));
      out.write(0x80 | (number >> 7));
      out.write(0x7F & number);
    } else {
      out.write(0x80 | (number >> 28));
      out.write(0x80 | (number >> 21));
      out.write(0x80 | (number >> 14));
      out.write(0x80 | (number >> 7));
      out.write(0x7F & number);
    }
  }

  /** 
   * Encodes {@code number} into {@code dest}, starting at offset {@code start} from
   * the beginning of the array. This method assumes {@code dest} is large enough to
   * hold the required number of bytes.
   * @param number The number to be encoded.
   * @param dest The destination array.
   * @param start The starting offset in the array.
   * @return The number of bytes used in the array.
   */
  public static int encode(int number, byte[] dest, int start) {
    if ((number & ~0x7F) == 0) {
      dest[start] = (byte) number;
      return 1;
    } else if ((number & ~0x3FFF) == 0) {
      dest[start] = (byte) (0x80 | ((number & 0x3F80) >> 7));
      dest[start + 1] = (byte) (number & 0x7F);
      return 2;
    } else if ((number & ~0x1FFFFF) == 0) {
      dest[start] = (byte) (0x80 | ((number & 0x1FC000) >> 14));
      dest[start + 1] = (byte) (0x80 | ((number & 0x3F80) >> 7));
      dest[start + 2] = (byte) (number & 0x7F);
      return 3;
    } else if ((number & ~0xFFFFFFF) == 0) {
      dest[start] = (byte) (0x80 | ((number & 0xFE00000) >> 21));
      dest[start + 1] = (byte) (0x80 | ((number & 0x1FC000) >> 14));
      dest[start + 2] = (byte) (0x80 | ((number & 0x3F80) >> 7));
      dest[start + 3] = (byte) (number & 0x7F);
      return 4;
    } else {
      dest[start] = (byte) (0x80 | ((number & 0xF0000000) >> 28));
      dest[start + 1] = (byte) (0x80 | ((number & 0xFE00000) >> 21));
      dest[start + 2] = (byte) (0x80 | ((number & 0x1FC000) >> 14));
      dest[start + 3] = (byte) (0x80 | ((number & 0x3F80) >> 7));
      dest[start + 4] = (byte) (number & 0x7F);
      return 5;
    }
  }

  /** 
   * Decodes a 32-bit integer from {@code bytes}, beginning at offset {@code pos.pos}.
   * The decoded value is returned, and {@code pos.pos} is incremented by the number of
   * bytes processed.
   * @param bytes The byte array containing an encoded value.
   * @param pos On entry, the starting position in the array; on return, one greater
   * than the position of the last byte decoded in the call.
   * @return The decoded value.
   */
  public static int decode(byte[] bytes, Position pos) {
    int value = 0;
    while (true) {
      byte first = bytes[pos.pos];
      ++pos.pos;
      value |= first & 0x7F;
      if ((first & 0x80) == 0) {
        return value;
      }
      value <<= 7;
    }
  }

  /**
   * Decodes a 32-bit integer from bytes read from {@code in}. Bytes are read,
   * one at a time, from {@code in}, and it is assumed they represent a 32-bit
   * integer encoded using this class's encoding scheme. The decoded value is
   * returned.
   * @param in The input stream containing the encoded bytes.
   * @return The decoded value.
   * @exception EOFException If the stream ends before a value has been decoded.
   */
  public static int decode(InputStream in) throws IOException {
    int value = 0;
    while (true) {
      int first = in.read();
      if (first < 0) {
        throw new EOFException();
      }
      value |= first & 0x7F;
      if ((first & 0x80) == 0) {
        return value;
      }
      value <<= 7;
    }
  }

  /**
   * The default ctor is made private because all methods of this class are static.
   */
  private Vint8() {
    // Just making it impossible to instantiate.
  }

}
