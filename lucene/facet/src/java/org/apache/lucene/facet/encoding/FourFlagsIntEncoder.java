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
 * A {@link ChunksIntEncoder} which encodes values in chunks of 4. Every group
 * starts with a single byte (called indicator) which represents 4 - 2 bit
 * flags, where the values:
 * <ul>
 * <li>1, 2 or 3 mean the encoded value is '1', '2' or '3' respectively.
 * <li>0 means the value is encoded using {@link VInt8IntEncoder}, and the
 * encoded bytes follow the indicator.<br>
 * Since value 0 is illegal, and 1-3 are encoded in the indicator, the actual
 * value that is encoded is <code>value-4</code>, which saves some more bits.
 * </ul>
 * Encoding example:
 * <ul>
 * <li>Original values: 6, 16, 5, 9, 7, 1, 11
 * <li>After sorting: 1, 5, 6, 7, 9, 11, 16
 * <li>D-Gap computing: 1, 4, 1, 1, 2, 5 (so far - done by
 * {@link DGapIntEncoder})
 * <li>Encoding: 1,0,1,1 as the first indicator, followed by 0 (4-4), than
 * 2,0,0,0 as the second indicator, followed by 1 (5-4) encoded with.
 * <li>Binary encode: <u>01 | 01 | 00 | 01</u> 00000000 <u>00 | 00 | 00 | 10</u>
 * 00000001 (indicators are <u>underlined</u>).<br>
 * <b>NOTE:</b> the order of the values in the indicator is lsb &rArr; msb,
 * which allows for more efficient decoding.
 * </ul>
 * 
 * @lucene.experimental
 */
public class FourFlagsIntEncoder extends ChunksIntEncoder {

  /*
   * Holds all combinations of <i>indicator</i> flags for fast encoding (saves
   * time on bit manipulation @ encode time)
   */
  private static final byte[][] ENCODE_TABLE = new byte[][] {
    new byte[] { 0x00, 0x00, 0x00, 0x00 },
    new byte[] { 0x01, 0x04, 0x10, 0x40 },
    new byte[] { 0x02, 0x08, 0x20, (byte) 0x80 },
    new byte[] { 0x03, 0x0C, 0x30, (byte) 0xC0 },
  };

  public FourFlagsIntEncoder() {
    super(4);
  }

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    buf.offset = buf.length = 0;
    int upto = values.offset + values.length;
    for (int i = values.offset; i < upto; i++) {
      int value = values.ints[i];
      if (value <= 3) {
        indicator |= ENCODE_TABLE[value][ordinal];
      } else {
        encodeQueue.ints[encodeQueue.length++] = value - 4;
      }
      ++ordinal;
      
      // encode the chunk and the indicator
      if (ordinal == 4) {
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
    return new FourFlagsIntDecoder();
  }

  @Override
  public String toString() {
    return "FourFlags(VInt)";
  }

}
