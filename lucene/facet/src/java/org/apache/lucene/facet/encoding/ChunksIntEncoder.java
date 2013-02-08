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
 * An {@link IntEncoder} which encodes values in chunks. Implementations of this
 * class assume the data which needs encoding consists of small, consecutive
 * values, and therefore the encoder is able to compress them better. You can
 * read more on the two implementations {@link FourFlagsIntEncoder} and
 * {@link EightFlagsIntEncoder}.
 * <p>
 * Extensions of this class need to implement {@link #encode(IntsRef, BytesRef)}
 * in order to build the proper indicator (flags). When enough values were
 * accumulated (typically the batch size), extensions can call
 * {@link #encodeChunk(BytesRef)} to flush the indicator and the rest of the
 * values.
 * <p>
 * <b>NOTE:</b> flags encoders do not accept values &le; 0 (zero) in their
 * {@link #encode(IntsRef, BytesRef)}. For performance reasons they do not check
 * that condition, however if such value is passed the result stream may be
 * corrupt or an exception will be thrown. Also, these encoders perform the best
 * when there are many consecutive small values (depends on the encoder
 * implementation). If that is not the case, the encoder will occupy 1 more byte
 * for every <i>batch</i> number of integers, over whatever
 * {@link VInt8IntEncoder} would have occupied. Therefore make sure to check
 * whether your data fits into the conditions of the specific encoder.
 * <p>
 * For the reasons mentioned above, these encoders are usually chained with
 * {@link UniqueValuesIntEncoder} and {@link DGapIntEncoder}.
 * 
 * @lucene.experimental
 */
public abstract class ChunksIntEncoder extends IntEncoder {
  
  /** Holds the values which must be encoded, outside the indicator. */
  protected final IntsRef encodeQueue;
  
  /** Represents bits flag byte. */
  protected int indicator = 0;
  
  /** Counts the current ordinal of the encoded value. */
  protected byte ordinal = 0;
  
  protected ChunksIntEncoder(int chunkSize) {
    encodeQueue = new IntsRef(chunkSize);
  }
  
  /**
   * Encodes the values of the current chunk. First it writes the indicator, and
   * then it encodes the values outside the indicator.
   */
  protected void encodeChunk(BytesRef buf) {
    // ensure there's enough room in the buffer
    int maxBytesRequired = buf.length + 1 + encodeQueue.length * 4; /* indicator + at most 4 bytes per positive VInt */
    if (buf.bytes.length < maxBytesRequired) {
      buf.grow(maxBytesRequired);
    }
    
    buf.bytes[buf.length++] = ((byte) indicator);
    for (int i = 0; i < encodeQueue.length; i++) {
      // it is better if the encoding is inlined like so, and not e.g.
      // in a utility method
      int value = encodeQueue.ints[i];
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
    
    ordinal = 0;
    indicator = 0;
    encodeQueue.length = 0;
  }
  
}
