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
  protected void doDecode(BytesRef buf, IntsRef values, int upto) {
    while (buf.offset < upto) {
      // read indicator
      int indicator = buf.bytes[buf.offset++] & 0xFF;
      int ordinal = 0;

      int capacityNeeded = values.length + 8;
      if (values.ints.length < capacityNeeded) {
        values.grow(capacityNeeded);
      }

      // process indicator, until we read 8 values, or end-of-buffer
      while (ordinal != 8) {
        if (DECODE_TABLE[indicator][ordinal++] == 0) {
          if (buf.offset == upto) { // end of buffer
            return;
          }
          // decode the value from the stream.
          values.ints[values.length++] = VInt8.decode(buf) + 2; 
        } else {
          values.ints[values.length++] = 1;
        }
      }
    }
  }

  @Override
  public String toString() {
    return "EightFlags (VInt8)";
  }

}
