package org.apache.lucene.store;

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

/** @lucene.experimental */
public final class ByteArrayDataInput extends DataInput {

  private byte[] bytes;
  private int pos;

  // TODO: allow BytesRef (slice) too
  public ByteArrayDataInput(byte[] bytes) {
    this.bytes = bytes;
  }

  public void reset(byte[] bytes) {
    this.bytes = bytes;
    pos = 0;
  }

  public boolean eof() {
    return pos == bytes.length;
  }

  public void skipBytes(int count) {
    pos += count;
  }

  // NOTE: AIOOBE not EOF if you read too much
  @Override
  public byte readByte() {
    return bytes[pos++];
  }

  // NOTE: AIOOBE not EOF if you read too much
  @Override
  public void readBytes(byte[] b, int offset, int len) {
    System.arraycopy(bytes, pos, b, offset, len);
    pos += len;
  }
}
