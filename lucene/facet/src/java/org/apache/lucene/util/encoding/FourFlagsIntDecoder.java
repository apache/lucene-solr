package org.apache.lucene.util.encoding;

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
 * Decodes values encoded with {@link FourFlagsIntEncoder}.
 * 
 * @lucene.experimental
 */
public class FourFlagsIntDecoder extends IntDecoder {

  /**
   * Holds all combinations of <i>indicator</i> for fast decoding (saves time
   * on real-time bit manipulation)
   */
  private final static byte[][] DECODE_TABLE = new byte[256][4];

  /** Generating all combinations of <i>indicator</i> into separate flags. */
  static {
    for (int i = 256; i != 0;) {
      --i;
      for (int j = 4; j != 0;) {
        --j;
        DECODE_TABLE[i][j] = (byte) ((i >>> (j << 1)) & 0x3);
      }
    }
  }

  @Override
  protected void doDecode(BytesRef buf, IntsRef values, int upto) {
    while (buf.offset < upto) {
      // read indicator
      int indicator = buf.bytes[buf.offset++] & 0xFF;
      int ordinal = 0;
      
      int capacityNeeded = values.length + 4;
      if (values.ints.length < capacityNeeded) {
        values.grow(capacityNeeded);
      }
      
      while (ordinal != 4) {
        byte decodeVal = DECODE_TABLE[indicator][ordinal++];
        if (decodeVal == 0) {
          if (buf.offset == upto) { // end of buffer
            return;
          }
          // decode the value from the stream.
          values.ints[values.length++] = VInt8.decode(buf) + 4;
        } else {
          values.ints[values.length++] = decodeVal;
        }
      }
    }
  }

  @Override
  public String toString() {
    return "FourFlags (VInt8)";
  }

}
