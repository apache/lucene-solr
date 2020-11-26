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
import java.util.Map;
import java.util.Set;

/**
 * A {@link IndexInput} wrapper that changes the endianness of the provided
 * index input.
 *
 * @lucene.internal
 */
public final class EndiannessReverserIndexInput extends IndexInput {
  
  private final IndexInput in;
  
  public EndiannessReverserIndexInput(IndexInput in) {
    super("Endianness reverser Index Input wrapper");
    this.in = in;
  }

  @Override
  public byte readByte() throws IOException {
    return in.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len)
          throws IOException {
    in.readBytes(b, offset, len);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer)
          throws IOException
  {
    in.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public short readShort() throws IOException {
    return Short.reverseBytes(in.readShort());
  }

  @Override
  public int readInt() throws IOException {
    return Integer.reverseBytes(in.readInt());
  }

  @Override
  public int readVInt() throws IOException {
    return in.readVInt();
  }

  @Override
  public int readZInt() throws IOException {
    return in.readZInt();
  }

  @Override
  public long readLong() throws IOException {
    return Long.reverseBytes(in.readLong()); 
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    in.readLongs(dst, offset, length);
    for (int i = 0; i < length; i++) {
      dst[offset + i] = Long.reverseBytes(dst[offset + i]);
    }
  }

  @Override
  public long readVLong() throws IOException {
    return in.readVLong();
  }

  @Override
  public long readZLong() throws IOException {
    return in.readZLong();
  }

  @Override
  public String readString() throws IOException {
    return in.readString();
  }
  
  @Override
  public IndexInput clone() {
   return new EndiannessReverserIndexInput(in.clone());
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return new EndiannessReverserIndexInput(in.slice(sliceDescription, offset, length));
  }

  @Override
  public Map<String,String> readMapOfStrings() throws IOException {
    return in.readMapOfStrings();
  }

  @Override
  public Set<String> readSetOfStrings() throws IOException {
    return in.readSetOfStrings();
  }

  @Override
  public void skipBytes(final long numBytes) throws IOException {
    in.skipBytes(numBytes);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public long getFilePointer() {
    return in.getFilePointer();
  }

  @Override
  public void seek(long pos) throws IOException {
     in.seek(pos);
  }

  @Override
  public long length() {
    return in.length();
  }

  @Override
  public String toString() {
    return in.toString();
  }
  
  @Override
  public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
    return new EndiannessReverserRandomAccessInput(in.randomAccessSlice(offset, length));
  }
  
  public static class EndiannessReverserRandomAccessInput implements RandomAccessInput {
    
    private final RandomAccessInput in;
    
    public EndiannessReverserRandomAccessInput(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      return in.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
      return Short.reverseBytes(in.readShort(pos));
    }

    @Override
    public int readInt(long pos) throws IOException {
      return Integer.reverseBytes(in.readInt(pos));
    }

    @Override
    public long readLong(long pos) throws IOException {
      return Long.reverseBytes(in.readLong(pos));
    }
  }

}
