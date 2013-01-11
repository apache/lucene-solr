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
 * Decodes integers from a set {@link BytesRef}.
 * 
 * @lucene.experimental
 */
public abstract class IntDecoder {
  
  /**
   * Performs the actual decoding. Values should be read from
   * {@link BytesRef#offset} up to {@code upto}. Also, {@code values} offset and
   * length are set to 0 and the encoder is expected to update
   * {@link IntsRef#length}, but not {@link IntsRef#offset}.
   * 
   * <p>
   * <b>NOTE:</b> it is ok to use the buffer's offset as the current position in
   * the buffer (and modify it), it will be reset by
   * {@link #decode(BytesRef, IntsRef)}.
   */
  protected abstract void doDecode(BytesRef buf, IntsRef values, int upto);
  
  /**
   * Called before {@link #doDecode(BytesRef, IntsRef, int)} so that decoders
   * can reset their state.
   */
  protected void reset() {
    // do nothing by default
  }
  
  /**
   * Decodes the values from the buffer into the given {@link IntsRef}. Note
   * that {@code values.offset} and {@code values.length} are set to 0.
   */
  public final void decode(BytesRef buf, IntsRef values) {
    values.offset = values.length = 0; // must do that because we cannot grow() them otherwise
    
    // some decoders may use the buffer's offset as a position index, so save
    // current offset.
    int bufOffset = buf.offset;
    
    reset();
    doDecode(buf, values, buf.offset + buf.length);
    assert values.offset == 0 : "offset should not have been modified by the decoder.";
    
    // fix offset
    buf.offset = bufOffset;
  }

}
