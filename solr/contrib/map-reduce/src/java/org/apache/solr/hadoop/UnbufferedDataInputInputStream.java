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

package org.apache.solr.hadoop;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class UnbufferedDataInputInputStream extends org.apache.solr.common.util.DataInputInputStream {
  private final DataInputStream in;
  
  public UnbufferedDataInputInputStream(DataInput in) {
    this.in = new DataInputStream(DataInputInputStream.constructInputStream(in));
  }
  
  @Override
  public void readFully(byte[] b) throws IOException {
    in.readFully(b);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    in.readFully(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    return in.skipBytes(n);
  }

  @Override
  public boolean readBoolean() throws IOException {
    return in.readBoolean();
  }

  @Override
  public byte readByte() throws IOException {
    return in.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return in.readUnsignedByte();
  }

  @Override
  public short readShort() throws IOException {
    return in.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return in.readUnsignedShort();
  }

  @Override
  public char readChar() throws IOException {
    return in.readChar();
  }

  @Override
  public int readInt() throws IOException {
    return in.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return in.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return in.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return in.readDouble();
  }

  @Override
  public String readLine() throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    return reader.readLine();
  }

  @Override
  public String readUTF() throws IOException {
    return in.readUTF();
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

}
