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
package org.apache.lucene.store;


import org.apache.lucene.util.BytesRef;

/** 
 * DataInput backed by a byte array.
 * <b>WARNING:</b> This class omits all low-level checks.
 * @lucene.experimental 
 */
public final class ByteArrayDataInput extends DataInput {

  private byte[] bytes;

  private int pos;
  private int limit;

  public ByteArrayDataInput(byte[] bytes) {
    reset(bytes);
  }

  public ByteArrayDataInput(byte[] bytes, int offset, int len) {
    reset(bytes, offset, len);
  }

  public ByteArrayDataInput() {
    reset(BytesRef.EMPTY_BYTES);
  }

  public void reset(byte[] bytes) {
    reset(bytes, 0, bytes.length);
  }

  // NOTE: sets pos to 0, which is not right if you had
  // called reset w/ non-zero offset!!
  public void rewind() {
    pos = 0;
  }

  public int getPosition() {
    return pos;
  }
  
  public void setPosition(int pos) {
    this.pos = pos;
  }

  public void reset(byte[] bytes, int offset, int len) {
    this.bytes = bytes;
    pos = offset;
    limit = offset + len;
  }

  public int length() {
    return limit;
  }

  public boolean eof() {
    return pos == limit;
  }

  @Override
  public void skipBytes(long count) {
    pos += count;
  }

  @Override
  public short readShort() {
    byte b1 = bytes[pos++];
    byte b2 = bytes[pos++];
    return (short) ((b2 & 0xFF) << 8 | (b1 & 0xFF));
  }

  @Override
  public int readInt() {
    byte b1 = bytes[pos++];
    byte b2 = bytes[pos++];
    byte b3 = bytes[pos++];
    byte b4 = bytes[pos++];
    return (b4 & 0xFF) << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
  }

  @Override
  public long readLong() {
    byte b1 = bytes[pos++];
    byte b2 = bytes[pos++];
    byte b3 = bytes[pos++];
    byte b4 = bytes[pos++];
    byte b5 = bytes[pos++];
    byte b6 = bytes[pos++];
    byte b7 = bytes[pos++];
    byte b8 = bytes[pos++];
    return (b8 & 0xFFL) << 56 | (b7 & 0xFFL) << 48 | (b6 & 0xFFL) << 40 | (b5 & 0xFFL) << 32
            | (b4 & 0xFFL) << 24 | (b3 & 0xFFL) << 16 | (b2 & 0xFFL) << 8 | (b1 & 0xFFL);
  }

  @Override
  public int readVInt() {
    byte b = bytes[pos++];
    if (b >= 0) return b;
    int i = b & 0x7F;
    b = bytes[pos++];
    i |= (b & 0x7F) << 7;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7F) << 14;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7F) << 21;
    if (b >= 0) return i;
    b = bytes[pos++];
    // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
    i |= (b & 0x0F) << 28;
    if ((b & 0xF0) == 0) return i;
    throw new RuntimeException("Invalid vInt detected (too many bits)");
  }
 
  @Override
  public long readVLong() {
    byte b = bytes[pos++];
    if (b >= 0) return b;
    long i = b & 0x7FL;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 7;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 14;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 21;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 28;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 35;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 42;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 49;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 56;
    if (b >= 0) return i;
    throw new RuntimeException("Invalid vLong detected (negative values disallowed)");
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
