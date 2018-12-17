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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;

/**
 * An {@link IndexInput} implementing {@link RandomAccessInput} and backed
 * by a {@link ByteBuffersDataInput}.
 */
public final class ByteBuffersIndexInput extends IndexInput implements RandomAccessInput {
  private ByteBuffersDataInput in;

  public ByteBuffersIndexInput(ByteBuffersDataInput in, String resourceDescription) {
    super(resourceDescription);
    this.in = in;
  }

  @Override
  public void close() throws IOException {
    in = null;
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return in.position();
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    in.seek(pos);
  }

  @Override
  public long length() {
    ensureOpen();
    return in.size();
  }

  @Override
  public ByteBuffersIndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    ensureOpen();
    return new ByteBuffersIndexInput(in.slice(offset, length), 
        "(sliced) offset=" + offset + ", length=" + length + " " + toString() + " [slice=" + sliceDescription + "]");
  }

  @Override
  public byte readByte() throws IOException {
    ensureOpen();
    return in.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    ensureOpen();
    in.readBytes(b, offset, len);
  }

  @Override
  public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
    ensureOpen();
    return slice("", offset, length);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    ensureOpen();
    in.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public short readShort() throws IOException {
    ensureOpen();
    return in.readShort();
  }

  @Override
  public int readInt() throws IOException {
    ensureOpen();
    return in.readInt();
  }

  @Override
  public int readVInt() throws IOException {
    ensureOpen();
    return in.readVInt();
  }

  @Override
  public int readZInt() throws IOException {
    ensureOpen();
    return in.readZInt();
  }

  @Override
  public long readLong() throws IOException {
    ensureOpen();
    return in.readLong();
  }

  @Override
  public long readVLong() throws IOException {
    ensureOpen();
    return in.readVLong();
  }

  @Override
  public long readZLong() throws IOException {
    ensureOpen();
    return in.readZLong();
  }

  @Override
  public String readString() throws IOException {
    ensureOpen();
    return in.readString();
  }

  @Override
  public Map<String, String> readMapOfStrings() throws IOException {
    ensureOpen();
    return in.readMapOfStrings();
  }

  @Override
  public Set<String> readSetOfStrings() throws IOException {
    ensureOpen();
    return in.readSetOfStrings();
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    ensureOpen();
    super.skipBytes(numBytes);
  }

  @Override
  public byte readByte(long pos) throws IOException {
    ensureOpen();
    return in.readByte(pos);
  }

  @Override
  public short readShort(long pos) throws IOException {
    ensureOpen();
    return in.readShort(pos);
  }

  @Override
  public int readInt(long pos) throws IOException {
    ensureOpen();
    return in.readInt(pos);
  }

  @Override
  public long readLong(long pos) throws IOException {
    ensureOpen();
    return in.readLong(pos);
  }
  
  @Override
  public IndexInput clone() {
    ensureOpen();
    ByteBuffersIndexInput cloned = new ByteBuffersIndexInput(in.slice(0, in.size()), "(clone of) " + toString());
    try {
      cloned.seek(getFilePointer());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return cloned;
  }

  private void ensureOpen() {
    if (in == null) {
      throw new AlreadyClosedException("Already closed.");
    }
  }  
}
