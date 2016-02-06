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
package org.apache.lucene.codecs.compressing.dummy;

import java.io.IOException;

import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** CompressionCodec that does not compress data, useful for testing. */
// In its own package to make sure the oal.codecs.compressing classes are
// visible enough to let people write their own CompressionMode
public class DummyCompressingCodec extends CompressingCodec {

  public static final CompressionMode DUMMY = new CompressionMode() {

    @Override
    public Compressor newCompressor() {
      return DUMMY_COMPRESSOR;
    }

    @Override
    public Decompressor newDecompressor() {
      return DUMMY_DECOMPRESSOR;
    }

    @Override
    public String toString() {
      return "DUMMY";
    }

  };

  private static final Decompressor DUMMY_DECOMPRESSOR = new Decompressor() {

    @Override
    public void decompress(DataInput in, int originalLength,
        int offset, int length, BytesRef bytes) throws IOException {
      assert offset + length <= originalLength;
      if (bytes.bytes.length < originalLength) {
        bytes.bytes = new byte[ArrayUtil.oversize(originalLength, 1)];
      }
      in.readBytes(bytes.bytes, 0, offset + length);
      bytes.offset = offset;
      bytes.length = length;
    }

    @Override
    public Decompressor clone() {
      return this;
    }

  };

  private static final Compressor DUMMY_COMPRESSOR = new Compressor() {

    @Override
    public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
      out.writeBytes(bytes, off, len);
    }

  };

  /** Constructor that allows to configure the chunk size. */
  public DummyCompressingCodec(int chunkSize, int maxDocsPerChunk, boolean withSegmentSuffix, int blockSize) {
    super("DummyCompressingStoredFields",
          withSegmentSuffix ? "DummyCompressingStoredFields" : "",
          DUMMY, chunkSize, maxDocsPerChunk, blockSize);
  }

  /** Default constructor. */
  public DummyCompressingCodec() {
    this(1 << 14, 128, false, 1024);
  }

}
