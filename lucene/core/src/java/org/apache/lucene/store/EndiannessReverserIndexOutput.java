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
 * A {@link IndexOutput} wrapper that changes the endianness of the provided
 * index output.
 *
 * @lucene.internal
 */
public final class EndiannessReverserIndexOutput extends IndexOutput {
  
  private final IndexOutput out;
  
  public EndiannessReverserIndexOutput(IndexOutput out) {
    super("Endianness reverser Index Output wrapper", out.getName());
    this.out = out;
  }

  @Override
  public String getName() {
    return out.getName();
  }

  /** Closes this stream to further operations. */
  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public long getFilePointer() {
    return out.getFilePointer();
  }

  @Override
  public long getChecksum() throws IOException {
    return out.getChecksum();
  }

  @Override
  public String toString() {
    return out.getName();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    out.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int length) throws IOException {
    writeBytes(b, 0, length);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    out.writeBytes(b, offset, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    out.writeInt(Integer.reverseBytes(i));
  }

  @Override
  public void writeShort(short i) throws IOException {
    out.writeShort(Short.reverseBytes(i));
  }

  @Override
  public void writeLong(long i) throws IOException {
   out.writeLong(Long.reverseBytes(i));
  }

  @Override
  public void writeString(String s) throws IOException {
   out.writeString(s);
  }

  @Override
  public void copyBytes(DataInput input, long numBytes) throws IOException {
   out.copyBytes(input, numBytes);
  }

  @Override
  public void writeMapOfStrings(Map<String,String> map) throws IOException {
    out.writeMapOfStrings(map);
  }

  @Override
  public void writeSetOfStrings(Set<String> set) throws IOException {
    out.writeSetOfStrings(set);
  }

}
