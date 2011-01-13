package org.apache.lucene.index.codecs;

/**
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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;

final class DeltaBytesWriter {

  // Must be bigger than
  // DocumentsWriter.MAX_TERM_LENGTH_UTF8.  If you change
  // this it's an index format change, so that change must be
  // versioned:
  final static int TERM_EOF = BYTE_BLOCK_SIZE;

  private byte[] lastBytes = new byte[10];
  private int lastLength;
  final IndexOutput out;

  DeltaBytesWriter(IndexOutput out) {
    this.out = out;
  }

  void reset() {
    lastLength = 0;
  }

  void write(BytesRef text) throws IOException {
    int start = 0;
    int upto = text.offset;
    final int length = text.length;
    final byte[] bytes = text.bytes;

    final int limit = length < lastLength ? length : lastLength;
    while(start < limit) {
      if (bytes[upto] != lastBytes[start]) {
        break;
      }
      start++;
      upto++;
    }

    final int suffix = length - start;
    out.writeVInt(start);                       // prefix
    out.writeVInt(suffix);                      // suffix
    out.writeBytes(bytes, upto, suffix);
    if (lastBytes.length < length) {
      lastBytes = ArrayUtil.grow(lastBytes, length);
    }
    // TODO: is this copy really necessary?  I don't think
    // caller actually modifies these bytes, so we can save
    // by reference?
    System.arraycopy(bytes, upto, lastBytes, start, suffix);
    lastLength = length;
  }
}
