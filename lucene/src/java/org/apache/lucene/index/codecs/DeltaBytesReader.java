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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

// Handles reading incremental UTF8 encoded terms
final class DeltaBytesReader {
  final BytesRef term = new BytesRef();
  final IndexInput in;

  DeltaBytesReader(IndexInput in) {
    this.in = in;
    term.bytes = new byte[10];
  }

  void reset(BytesRef text) {
    term.copy(text);
  }

  boolean read() throws IOException {
    final int start = in.readVInt();
    if (start == DeltaBytesWriter.TERM_EOF) {
      return false;
    }
    final int suffix = in.readVInt();
    assert start <= term.length: "start=" + start + " length=" + term.length;
    final int newLength = start+suffix;
    term.grow(newLength);
    in.readBytes(term.bytes, start, suffix);
    term.length = newLength;
    return true;
  }
}
