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
import org.apache.lucene.store.DataOutput;

/**
 * Encodes the raw bytes of a block when the index is written.
 * <p>
 * For example, implementations may compress or encrypt.
 *
 * @see BlockDecoder
 * @lucene.experimental
 */
public interface BlockEncoder {

  /**
   * Encodes all the bytes of one block in a single operation. The encoding is per block.
   * @param blockBytes The input block bytes to read.
   * @param length The number of bytes to read from the input.
   * @return The encoded block bytes.
   * @throws IOException If an encoding error occurs.
   */
  WritableBytes encode(DataInput blockBytes, long length) throws IOException;

  /**
   * Writable byte buffer.
   */
  interface WritableBytes {

    /**
     * Gets the number of bytes.
     */
    long size();

    /**
     * Writes the bytes to the provided {@link DataOutput}.
     */
    void writeTo(DataOutput dataOutput) throws IOException;
  }
}
