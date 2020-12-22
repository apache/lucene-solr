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

package org.apache.solr.blob;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

public class FilterIndexOutput extends IndexOutput {

  protected final IndexOutput delegate;

  public FilterIndexOutput(String resourceDescription, String name, IndexOutput delegate) {
    super(resourceDescription, name);
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public long getFilePointer() {
    return delegate.getFilePointer();
  }

  @Override
  public long getChecksum() throws IOException {
    return delegate.getChecksum();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    delegate.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    delegate.writeBytes(b, offset, length);
  }

  @Override
  public void writeBytes(byte[] b, int length) throws IOException {
    delegate.writeBytes(b, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    delegate.writeInt(i);
  }

  @Override
  public void writeShort(short i) throws IOException {
    delegate.writeShort(i);
  }

  @Override
  public void writeLong(long i) throws IOException {
    delegate.writeLong(i);
  }

  @Override
  public void writeString(String s) throws IOException {
    delegate.writeString(s);
  }

  @Override
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    delegate.copyBytes(input, numBytes);
  }

  @Override
  public void writeMapOfStrings(Map<String, String> map) throws IOException {
    delegate.writeMapOfStrings(map);
  }

  @Override
  public void writeSetOfStrings(Set<String> set) throws IOException {
    delegate.writeSetOfStrings(set);
  }
}
