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
import org.apache.lucene.util.BytesRef;

/**
 * Test utility for simple ROT13 cipher (https://en.wikipedia.org/wiki/ROT13).
 */
public class Rot13CypherTestUtil {

  private static final int ENCODING_OFFSET = 7;
  private static final int ENCODING_ROTATION = 13;

  public static byte[] encode(DataInput bytesInput, int length) throws IOException {
    byte[] encodedBytes = new byte[length + ENCODING_OFFSET];
    for (int i = 0; i < length; i++) {
      encodedBytes[i + ENCODING_OFFSET] = (byte)(bytesInput.readByte() + ENCODING_ROTATION);
    }
    return encodedBytes;
  }

  public static byte[] decode(DataInput bytesInput, long length) throws IOException {
    length -= ENCODING_OFFSET;
    bytesInput.skipBytes(ENCODING_OFFSET);
    byte[] decodedBytes = new byte[Math.toIntExact(length)];
    for (int i = 0; i < length; i++) {
      decodedBytes[i] = (byte)(bytesInput.readByte() - ENCODING_ROTATION);
    }
    return decodedBytes;
  }

  public static BlockEncoder getBlockEncoder() {
    return (blockBytes, length) -> {
      byte[] encodedBytes = Rot13CypherTestUtil.encode(blockBytes, Math.toIntExact(length));
      return new BlockEncoder.WritableBytes() {
        @Override
        public long size() {
          return encodedBytes.length;
        }

        @Override
        public void writeTo(DataOutput dataOutput) throws IOException {
          dataOutput.writeBytes(encodedBytes, 0, encodedBytes.length);
        }
      };
    };
  }

  public static BlockDecoder getBlockDecoder() {
    return (blockBytes, length) -> new BytesRef(Rot13CypherTestUtil.decode(blockBytes, length));
  }
}