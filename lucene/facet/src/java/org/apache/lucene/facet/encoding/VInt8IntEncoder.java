package org.apache.lucene.facet.encoding;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

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
 * An {@link IntEncoder} which implements variable length encoding. A number is
 * encoded as follows:
 * <ul>
 * <li>If it is less than 127 and non-negative, i.e. uses only 7 bits, it is
 * encoded as a single byte: 0bbbbbbb.
 * <li>If it occupies more than 7 bits, it is represented as a series of bytes,
 * each byte carrying 7 bits. All but the last byte have the MSB set, the last
 * one has it unset.
 * </ul>
 * Example:
 * <ol>
 * <li>n = 117 = 01110101: This has less than 8 significant bits, therefore is
 * encoded as 01110101 = 0x75.
 * <li>n = 100000 = (binary) 11000011010100000. This has 17 significant bits,
 * thus needs three Vint8 bytes. Pad it to a multiple of 7 bits, then split it
 * into chunks of 7 and add an MSB, 0 for the last byte, 1 for the others:
 * 1|0000110 1|0001101 0|0100000 = 0x86 0x8D 0x20.
 * </ol>
 * <b>NOTE:</b> although this encoder is not limited to values &ge; 0, it is not
 * recommended for use with negative values, as their encoding will result in 5
 * bytes written to the output stream, rather than 4. For such values, either
 * use {@link SimpleIntEncoder} or write your own version of variable length
 * encoding, which can better handle negative values.
 * 
 * @lucene.experimental
 */
public final class VInt8IntEncoder extends IntEncoder {

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    buf.offset = buf.length = 0;
    int maxBytesNeeded = 5 * values.length; // at most 5 bytes per VInt
    if (buf.bytes.length < maxBytesNeeded) {
      buf.grow(maxBytesNeeded);
    }
    
    int upto = values.offset + values.length;
    for (int i = values.offset; i < upto; i++) {
      // it is better if the encoding is inlined like so, and not e.g.
      // in a utility method
      int value = values.ints[i];
      if ((value & ~0x7F) == 0) {
        buf.bytes[buf.length] = (byte) value;
        buf.length++;
      } else if ((value & ~0x3FFF) == 0) {
        buf.bytes[buf.length] = (byte) (0x80 | ((value & 0x3F80) >> 7));
        buf.bytes[buf.length + 1] = (byte) (value & 0x7F);
        buf.length += 2;
      } else if ((value & ~0x1FFFFF) == 0) {
        buf.bytes[buf.length] = (byte) (0x80 | ((value & 0x1FC000) >> 14));
        buf.bytes[buf.length + 1] = (byte) (0x80 | ((value & 0x3F80) >> 7));
        buf.bytes[buf.length + 2] = (byte) (value & 0x7F);
        buf.length += 3;
      } else if ((value & ~0xFFFFFFF) == 0) {
        buf.bytes[buf.length] = (byte) (0x80 | ((value & 0xFE00000) >> 21));
        buf.bytes[buf.length + 1] = (byte) (0x80 | ((value & 0x1FC000) >> 14));
        buf.bytes[buf.length + 2] = (byte) (0x80 | ((value & 0x3F80) >> 7));
        buf.bytes[buf.length + 3] = (byte) (value & 0x7F);
        buf.length += 4;
      } else {
        buf.bytes[buf.length] = (byte) (0x80 | ((value & 0xF0000000) >> 28));
        buf.bytes[buf.length + 1] = (byte) (0x80 | ((value & 0xFE00000) >> 21));
        buf.bytes[buf.length + 2] = (byte) (0x80 | ((value & 0x1FC000) >> 14));
        buf.bytes[buf.length + 3] = (byte) (0x80 | ((value & 0x3F80) >> 7));
        buf.bytes[buf.length + 4] = (byte) (value & 0x7F);
        buf.length += 5;
      }
    }
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return new VInt8IntDecoder();
  }

  @Override
  public String toString() {
    return "VInt8";
  }

} 
