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
 * A simple {@link IntEncoder}, writing an integer as 4 raw bytes. *
 * 
 * @lucene.experimental
 */
public final class SimpleIntEncoder extends IntEncoder {

  @Override
  public void encode(IntsRef values, BytesRef buf) {
    buf.offset = buf.length = 0;
    // ensure there's enough room in the buffer
    int bytesNeeded = values.length * 4;
    if (buf.bytes.length < bytesNeeded) {
      buf.grow(bytesNeeded);
    }
    
    int upto = values.offset + values.length;
    for (int i = values.offset; i < upto; i++) {
      int value = values.ints[i];
      buf.bytes[buf.length++] = (byte) (value >>> 24);
      buf.bytes[buf.length++] = (byte) ((value >> 16) & 0xFF);
      buf.bytes[buf.length++] = (byte) ((value >> 8) & 0xFF);
      buf.bytes[buf.length++] = (byte) (value & 0xFF);
    }
  }

  @Override
  public IntDecoder createMatchingDecoder() {
    return new SimpleIntDecoder();
  }

  @Override
  public String toString() {
    return "Simple";
  }

}
