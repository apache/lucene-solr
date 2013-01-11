package org.apache.lucene.util.encoding;

import org.apache.lucene.util.BytesRef;

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
 * Variable-length encoding of 32-bit integers, into 8-bit bytes. A number is
 * encoded as follows:
 * <ul>
 * <li>If it is less than 127 and non-negative (i.e., if the number uses only 7
 * bits), it is encoded as as single byte: 0bbbbbbb.
 * <li>If its highest nonzero bit is greater than bit 6 (0x40), it is
 * represented as a series of bytes, each byte's 7 LSB containing bits from the
 * original value, with the MSB set for all but the last byte. The first encoded
 * byte contains the highest nonzero bits from the original; the second byte
 * contains the next 7 MSB; and so on, with the last byte containing the 7 LSB
 * of the original.
 * </ul>
 * Examples:
 * <ol>
 * <li>n = 117 = 1110101: This has fewer than 8 significant bits, and so is
 * encoded as 01110101 = 0x75.
 * <li>n = 100000 = (binary) 11000011010100000. This has 17 significant bits,
 * and so needs three Vint8 bytes. Left-zero-pad it to a multiple of 7 bits,
 * then split it into chunks of 7 and add an MSB, 0 for the last byte, 1 for the
 * others: 1|0000110 1|0001101 0|0100000 = 0x86 0x8D 0x20.
 * </ol>
 * {@link #encode(int, BytesRef)} and {@link #decode(BytesRef)} will correctly
 * handle any 32-bit integer, but for negative numbers, and positive numbers
 * with more than 28 significant bits, encoding requires 5 bytes; this is not an
 * efficient encoding scheme for large positive numbers or any negative number.
 * 
 * @lucene.experimental
 */
public class VInt8 {

  /** The maximum number of bytes needed to encode an integer. */
  public static final int MAXIMUM_BYTES_NEEDED = 5;
  
  /**
   * Decodes an int from the given bytes, starting at {@link BytesRef#offset}.
   * Returns the decoded bytes and updates {@link BytesRef#offset}.
   */
  public static int decode(BytesRef bytes) {
    /*
    This is the original code of this method, but a Hotspot bug
    corrupted the for-loop of DataInput.readVInt() (see LUCENE-2975)
    so the loop was unwounded here too, to be on the safe side
    int value = 0;
    while (true) {
      byte first = bytes.bytes[bytes.offset++];
      value |= first & 0x7F;
      if ((first & 0x80) == 0) {
        return value;
      }
      value <<= 7;
    }
    */

    // byte 1
    byte b = bytes.bytes[bytes.offset++];
    if (b >= 0) return b;
    
    // byte 2
    int value = b & 0x7F;
    b = bytes.bytes[bytes.offset++];
    value = (value << 7) | b & 0x7F;
    if (b >= 0) return value;
    
    // byte 3
    b = bytes.bytes[bytes.offset++];
    value = (value << 7) | b & 0x7F;
    if (b >= 0) return value;
    
    // byte 4
    b = bytes.bytes[bytes.offset++];
    value = (value << 7) | b & 0x7F;
    if (b >= 0) return value;
    
    // byte 5
    b = bytes.bytes[bytes.offset++];
    return (value << 7) | b & 0x7F;
  }

  /**
   * Encodes the given number into bytes, starting at {@link BytesRef#length}.
   * Assumes that the array is large enough.
   */
  public static void encode(int value, BytesRef bytes) {
    if ((value & ~0x7F) == 0) {
      bytes.bytes[bytes.length] = (byte) value;
      bytes.length++;
    } else if ((value & ~0x3FFF) == 0) {
      bytes.bytes[bytes.length] = (byte) (0x80 | ((value & 0x3F80) >> 7));
      bytes.bytes[bytes.length + 1] = (byte) (value & 0x7F);
      bytes.length += 2;
    } else if ((value & ~0x1FFFFF) == 0) {
      bytes.bytes[bytes.length] = (byte) (0x80 | ((value & 0x1FC000) >> 14));
      bytes.bytes[bytes.length + 1] = (byte) (0x80 | ((value & 0x3F80) >> 7));
      bytes.bytes[bytes.length + 2] = (byte) (value & 0x7F);
      bytes.length += 3;
    } else if ((value & ~0xFFFFFFF) == 0) {
      bytes.bytes[bytes.length] = (byte) (0x80 | ((value & 0xFE00000) >> 21));
      bytes.bytes[bytes.length + 1] = (byte) (0x80 | ((value & 0x1FC000) >> 14));
      bytes.bytes[bytes.length + 2] = (byte) (0x80 | ((value & 0x3F80) >> 7));
      bytes.bytes[bytes.length + 3] = (byte) (value & 0x7F);
      bytes.length += 4;
    } else {
      bytes.bytes[bytes.length] = (byte) (0x80 | ((value & 0xF0000000) >> 28));
      bytes.bytes[bytes.length + 1] = (byte) (0x80 | ((value & 0xFE00000) >> 21));
      bytes.bytes[bytes.length + 2] = (byte) (0x80 | ((value & 0x1FC000) >> 14));
      bytes.bytes[bytes.length + 3] = (byte) (0x80 | ((value & 0x3F80) >> 7));
      bytes.bytes[bytes.length + 4] = (byte) (value & 0x7F);
      bytes.length += 5;
    }
  }

  private VInt8() {
    // Just making it impossible to instantiate.
  }

}
