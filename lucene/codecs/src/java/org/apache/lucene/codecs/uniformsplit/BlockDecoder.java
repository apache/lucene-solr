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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;

/**
 * Decodes the raw bytes of a block when the index is read, according to the {@link BlockEncoder}
 * used during the writing of the index.
 *
 * <p>For example, implementations may decompress or decrypt.
 *
 * @see BlockEncoder
 * @lucene.experimental
 */
public interface BlockDecoder {

  /**
   * Decodes all the bytes of one block in a single operation. The decoding is per block.
   *
   * @param blockBytes The input block bytes to read.
   * @param length The number of bytes to read from the input.
   * @return The decoded block bytes.
   * @throws IOException If a decoding error occurs.
   */
  // TODO: this method could return a new interface ReadableBytes which
  // would provide the size and create PositionableDataInput (get/set position)
  // implemented by ByteArrayDataInput and ByteBuffersDataInput.
  // Big length could be supported (> Integer.MAX_VALUE).
  BytesRef decode(DataInput blockBytes, long length) throws IOException;
}
