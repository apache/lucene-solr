package org.apache.lucene.facet.encoding;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;

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
 * Decodes values encoded by {@link VInt8IntEncoder}.
 * 
 * @lucene.experimental
 */
public final class VInt8IntDecoder extends IntDecoder {

  @Override
  public void decode(BytesRef buf, IntsRef values) {
    values.offset = values.length = 0;

    // grow the buffer up front, even if by a large number of values (buf.length)
    // that saves the need to check inside the loop for every decoded value if
    // the buffer needs to grow.
    if (values.ints.length < buf.length) {
      values.ints = new int[ArrayUtil.oversize(buf.length, RamUsageEstimator.NUM_BYTES_INT)];
    }

    // it is better if the decoding is inlined like so, and not e.g.
    // in a utility method
    int upto = buf.offset + buf.length;
    int value = 0;
    int offset = buf.offset;
    while (offset < upto) {
      byte b = buf.bytes[offset++];
      if (b >= 0) {
        values.ints[values.length++] = (value << 7) | b;
        value = 0;
      } else {
        value = (value << 7) | (b & 0x7F);
      }
    }
  }

  @Override
  public String toString() {
    return "VInt8";
  }

} 
