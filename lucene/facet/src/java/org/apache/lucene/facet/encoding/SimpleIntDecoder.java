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
 * Decodes values encoded with {@link SimpleIntEncoder}.
 * 
 * @lucene.experimental
 */
public final class SimpleIntDecoder extends IntDecoder {

  @Override
  public void decode(BytesRef buf, IntsRef values) {
    values.offset = values.length = 0;
    int numValues = buf.length / 4; // every value is 4 bytes
    if (values.ints.length < numValues) { // offset and length are 0
      values.ints = new int[ArrayUtil.oversize(numValues, RamUsageEstimator.NUM_BYTES_INT)];
    }
    
    int offset = buf.offset;
    int upto = buf.offset + buf.length;
    while (offset < upto) {
      values.ints[values.length++] = 
          ((buf.bytes[offset++] & 0xFF) << 24) | 
          ((buf.bytes[offset++] & 0xFF) << 16) | 
          ((buf.bytes[offset++] & 0xFF) <<  8) | 
          (buf.bytes[offset++] & 0xFF);
    }
  }

  @Override
  public String toString() {
    return "Simple";
  }

}
