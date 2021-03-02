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
package org.apache.lucene.backward_codecs.lucene40.blocktree;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.compress.LowercaseAsciiCompression;

/** Compression algorithm used for suffixes of a block of terms. */
enum CompressionAlgorithm {
  NO_COMPRESSION(0x00) {

    @Override
    void read(DataInput in, byte[] out, int len) throws IOException {
      in.readBytes(out, 0, len);
    }
  },

  LOWERCASE_ASCII(0x01) {

    @Override
    void read(DataInput in, byte[] out, int len) throws IOException {
      LowercaseAsciiCompression.decompress(in, out, len);
    }
  },

  LZ4(0x02) {

    @Override
    void read(DataInput in, byte[] out, int len) throws IOException {
      org.apache.lucene.util.compress.LZ4.decompress(in, len, out, 0);
    }
  };

  private static final CompressionAlgorithm[] BY_CODE = new CompressionAlgorithm[3];

  static {
    for (CompressionAlgorithm alg : CompressionAlgorithm.values()) {
      BY_CODE[alg.code] = alg;
    }
  }

  /** Look up a {@link CompressionAlgorithm} by its {@link CompressionAlgorithm#code}. */
  static final CompressionAlgorithm byCode(int code) {
    if (code < 0 || code >= BY_CODE.length) {
      throw new IllegalArgumentException("Illegal code for a compression algorithm: " + code);
    }
    return BY_CODE[code];
  }

  public final int code;

  private CompressionAlgorithm(int code) {
    this.code = code;
  }

  abstract void read(DataInput in, byte[] out, int len) throws IOException;
}
