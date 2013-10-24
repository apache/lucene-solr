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
 * Decodes values encoded with {@link EightFlagsIntEncoder}.
 * 
 * @lucene.experimental
 */
public class EightFlagsIntDecoder extends IntDecoder {

  /*
   * Holds all combinations of <i>indicator</i> for fast decoding (saves time
   * on real-time bit manipulation)
   */
  private static final byte[][] DECODE_TABLE = new byte[256][8];

  /** Generating all combinations of <i>indicator</i> into separate flags. */
  static {
    for (int i = 256; i != 0;) {
      --i;
      for (int j = 8; j != 0;) {
        --j;
        DECODE_TABLE[i][j] = (byte) ((i >>> j) & 0x1);
      }
    }
  }

  @Override
  public void decode(BytesRef buf, IntsRef values) {
    values.offset = values.length = 0;
    int upto = buf.offset + buf.length;
    int offset = buf.offset;
    while (offset < upto) {
      // read indicator
      int indicator = buf.bytes[offset++] & 0xFF;
      int ordinal = 0;

      int capacityNeeded = values.length + 8;
      if (values.ints.length < capacityNeeded) {
        values.grow(capacityNeeded);
      }

      // process indicator, until we read 8 values, or end-of-buffer
      while (ordinal != 8) {
        if (DECODE_TABLE[indicator][ordinal++] == 0) {
          if (offset == upto) { // end of buffer
            return;
          }
          // it is better if the decoding is inlined like so, and not e.g.
          // in a utility method
          int value = 0;
          while (true) {
            byte b = buf.bytes[offset++];
            if (b >= 0) {
              values.ints[values.length++] = ((value << 7) | b) + 2;
              break;
            } else {
              value = (value << 7) | (b & 0x7F);
            }
          }
        } else {
          values.ints[values.length++] = 1;
        }
      }
    }
  }

  @Override
  public String toString() {
    return "EightFlags(VInt8)";
  }

}
