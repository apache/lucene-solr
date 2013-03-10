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
 * A {@link ChunksIntEncoder} which encodes data in chunks of 8. Every group
 * starts with a single byte (called indicator) which represents 8 - 1 bit
 * flags, where the value:
 * <ul>
 * <li>1 means the encoded value is '1'
 * <li>0 means the value is encoded using {@link VInt8IntEncoder}, and the
 * encoded bytes follow the indicator.<br>
 * Since value 0 is illegal, and 1 is encoded in the indicator, the actual value
 * that is encoded is <code>value-2</code>, which saves some more bits.
 * </ul>
 * Encoding example:
 * <ul>
 * <li>Original values: 6, 16, 5, 9, 7, 1
 * <li>After sorting: 1, 5, 6, 7, 9, 16
 * <li>D-Gap computing: 1, 4, 1, 1, 2, 5 (so far - done by
 * {@link DGapIntEncoder})
 * <li>Encoding: 1,0,1,1,0,0,0,0 as the indicator, by 2 (4-2), 0 (2-2), 3 (5-2).
 * <li>Binary encode: <u>0 | 0 | 0 | 0 | 1 | 1 | 0 | 1</u> 00000010 00000000
 * 00000011 (indicator is <u>underlined</u>).<br>
 * <b>NOTE:</b> the order of the values in the indicator is lsb &rArr; msb,
 * which allows for more efficient decoding.
 * </ul>
 * 
 * @lucene.experimental
 */
public class EightFlagsIntEncoder extends ChunksIntEncoder {

  /*
   * Holds all combinations of <i>indicator</i> flags for fast encoding (saves
   * time on bit manipulation at encode time)
   */
  private static final byte[] ENCODE_TABLE = new byte[] { 0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, (byte) 0x80 };

  public EightFlagsIntEncoder() {
    super(8);
  }

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    buf.offset = buf.length = 0;
    int upto = values.offset + values.length;
    for (int i = values.offset; i < upto; i++) {
      int value = values.ints[i];
      if (value == 1) {
        indicator |= ENCODE_TABLE[ordinal];
      } else {
        encodeQueue.ints[encodeQueue.length++] = value - 2;
      }
      ++ordinal;
      
      // encode the chunk and the indicator
      if (ordinal == 8) {
        encodeChunk(buf);
      }
    }
    
    // encode remaining values
    if (ordinal != 0) {
      encodeChunk(buf);
    }
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return new EightFlagsIntDecoder();
  }

  @Override
  public String toString() {
    return "EightFlags(VInt)";
  }

}
