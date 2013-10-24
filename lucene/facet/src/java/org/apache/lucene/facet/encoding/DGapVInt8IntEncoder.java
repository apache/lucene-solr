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
 * An {@link IntEncoder} which implements variable length encoding for the gap
 * between values. It's a specialized form of the combination of
 * {@link DGapIntEncoder} and {@link VInt8IntEncoder}.
 * 
 * @see VInt8IntEncoder
 * @see DGapIntEncoder
 * 
 * @lucene.experimental
 */
public final class DGapVInt8IntEncoder extends IntEncoder {

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    buf.offset = buf.length = 0;
    int maxBytesNeeded = 5 * values.length; // at most 5 bytes per VInt
    if (buf.bytes.length < maxBytesNeeded) {
      buf.grow(maxBytesNeeded);
    }
    
    int upto = values.offset + values.length;
    int prev = 0;
    for (int i = values.offset; i < upto; i++) {
      // it is better if the encoding is inlined like so, and not e.g.
      // in a utility method
      int value = values.ints[i] - prev;
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
      prev = values.ints[i];
    }
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return new DGapVInt8IntDecoder();
  }

  @Override
  public String toString() {
    return "DGapVInt8";
  }

} 
