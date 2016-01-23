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

package org.apache.solr.common.util;

import java.io.*;

/** Single threaded buffered InputStream
 *  Internal Solr use only, subject to change.
 */
public class FastInputStream extends InputStream implements DataInput {
  private final InputStream in;
  private final byte[] buf;
  private int pos;
  private int end;

  public FastInputStream(InputStream in) {
  // use default BUFSIZE of BufferedOutputStream so if we wrap that
  // it won't cause double buffering.
    this(in, new byte[8192], 0, 0);
  }

  public FastInputStream(InputStream in, byte[] tempBuffer, int start, int end) {
    this.in = in;
    this.buf = tempBuffer;
    this.pos = start;
    this.end = end;
  }


  public static FastInputStream wrap(InputStream in) {
    return (in instanceof FastInputStream) ? (FastInputStream)in : new FastInputStream(in);
  }

  @Override
  public int read() throws IOException {
    if (pos >= end) {
      refill();
      if (pos >= end) return -1;
    }
    return buf[pos++] & 0xff;     
  }

  public int readUnsignedByte() throws IOException {
    if (pos >= end) {
      refill();
      if (pos >= end) throw new EOFException();
    }
    return buf[pos++] & 0xff;
  }

  public void refill() throws IOException {
    // this will set end to -1 at EOF
    end = in.read(buf, 0, buf.length);
    pos = 0;
  }

  @Override
  public int available() throws IOException {
    return end - pos;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    int r=0;  // number of bytes read
    // first read from our buffer;
    if (end-pos > 0) {
      r = Math.min(end-pos, len);
      System.arraycopy(buf, pos, b, off, r);      
      pos += r;
    }

    if (r == len) return r;

    // amount left to read is >= buffer size
    if (len-r >= buf.length) {
      int ret = in.read(b, off+r, len-r);
      if (ret==-1) return r==0 ? -1 : r;
      r += ret;
      return r;
    }

    refill();

    // first read from our buffer;
    if (end-pos > 0) {
      int toRead = Math.min(end-pos, len-r);
      System.arraycopy(buf, pos, b, off+r, toRead);
      pos += toRead;
      r += toRead;
      return r;
    }
    
    return r > 0 ? r : -1;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  public void readFully(byte b[]) throws IOException {
    readFully(b, 0, b.length);
  }

  public void readFully(byte b[], int off, int len) throws IOException {
    while (len>0) {
      int ret = read(b, off, len);
      if (ret==-1) {
        throw new EOFException();
      }
      off += ret;
      len -= ret;
    }
  }

  public int skipBytes(int n) throws IOException {
    if (end-pos >= n) {
      pos += n;
      return n;
    }

    if (end-pos<0) return -1;
    
    int r = end-pos;
    pos = end;

    while (r < n) {
      refill();
      if (end-pos <= 0) return r;
      int toRead = Math.min(end-pos, n-r);
      r += toRead;
      pos += toRead;
    }

    return r;
  }

  public boolean readBoolean() throws IOException {
    return readByte()==1;
  }

  public byte readByte() throws IOException {
    if (pos >= end) {
      refill();
      if (pos >= end) throw new EOFException();
    }
    return buf[pos++];
  }


  public short readShort() throws IOException {
    return (short)((readUnsignedByte() << 8) | readUnsignedByte());
  }

  public int readUnsignedShort() throws IOException {
    return (readUnsignedByte() << 8) | readUnsignedByte();
  }

  public char readChar() throws IOException {
    return (char)((readUnsignedByte() << 8) | readUnsignedByte());
  }

  public int readInt() throws IOException {
    return  ((readUnsignedByte() << 24)
            |(readUnsignedByte() << 16)
            |(readUnsignedByte() << 8)
            | readUnsignedByte());
  }

  public long readLong() throws IOException {
    return  (((long)readUnsignedByte()) << 56)
            | (((long)readUnsignedByte()) << 48)
            | (((long)readUnsignedByte()) << 40)
            | (((long)readUnsignedByte()) << 32)
            | (((long)readUnsignedByte()) << 24)
            | (readUnsignedByte() << 16)
            | (readUnsignedByte() << 8)
            | (readUnsignedByte());
  }

  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());    
  }

  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());    
  }

  public String readLine() throws IOException {
    return new DataInputStream(this).readLine();
  }

  public String readUTF() throws IOException {
    return new DataInputStream(this).readUTF();
  }
}
