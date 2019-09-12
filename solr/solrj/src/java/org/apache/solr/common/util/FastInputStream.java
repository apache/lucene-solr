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
package org.apache.solr.common.util;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** Single threaded buffered InputStream
 *  Internal Solr use only, subject to change.
 */
public class FastInputStream extends DataInputInputStream {
  protected final InputStream in;
  protected final byte[] buf;
  protected int pos;
  protected int end;
  protected long readFromStream; // number of bytes read from the underlying inputstream

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

  @Override
  boolean readDirectUtf8(ByteArrayUtf8CharSequence utf8, int len) {
    if (in != null || end < pos + len) return false;
    utf8.reset(buf, pos, len, null);
    pos = pos + len;
    return true;
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

  public int peek() throws IOException {
    if (pos >= end) {
      refill();
      if (pos >= end) return -1;
    }
    return buf[pos] & 0xff;
  }


  @Override
  public int readUnsignedByte() throws IOException {
    if (pos >= end) {
      refill();
      if (pos >= end) {
        throw new EOFException();
      }
    }
    return buf[pos++] & 0xff;
  }

  public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
    if(in == null) return -1;
    return in.read(target, offset, len);
  }

  public long position() {
    return readFromStream - (end - pos);
  }

  public void refill() throws IOException {
    // this will set end to -1 at EOF
    end = readWrappedStream(buf, 0, buf.length);
    if (end > 0) readFromStream += end;
    pos = 0;
  }

  @Override
  public int available() throws IOException {
    return end - pos;
  }

  /** Returns the internal buffer used for caching */
  public byte[] getBuffer() {
    return buf;
  }

  /** Current position within the internal buffer */
  public int getPositionInBuffer() {
    return pos;
  }

  /** Current end-of-data position within the internal buffer.  This is one past the last valid byte. */
  public int getEndInBuffer() {
    return end;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    int r=0;  // number of bytes we have read

    // first read from our buffer;
    if (end-pos > 0) {
      r = Math.min(end-pos, len);
      System.arraycopy(buf, pos, b, off, r);
      pos += r;
    }

    if (r == len) return r;

    // amount left to read is >= buffer size
    if (len-r >= buf.length) {
      int ret = readWrappedStream(b, off+r, len-r);
      if (ret >= 0) {
        readFromStream += ret;
        r += ret;
        return r;
      } else {
        // negative return code
        return r > 0 ? r : -1;
      }
    }

    refill();

    // read rest from our buffer
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

  @Override
  public void readFully(byte b[]) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
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

  @Override
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

  @Override
  public boolean readBoolean() throws IOException {
    return readByte()==1;
  }

  @Override
  public byte readByte() throws IOException {
    if (pos >= end) {
      refill();
      if (pos >= end) throw new EOFException();
    }
    return buf[pos++];
  }


  @Override
  public short readShort() throws IOException {
    return (short)((readUnsignedByte() << 8) | readUnsignedByte());
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return (readUnsignedByte() << 8) | readUnsignedByte();
  }

  @Override
  public char readChar() throws IOException {
    return (char)((readUnsignedByte() << 8) | readUnsignedByte());
  }

  @Override
  public int readInt() throws IOException {
    return  ((readUnsignedByte() << 24)
            |(readUnsignedByte() << 16)
            |(readUnsignedByte() << 8)
            | readUnsignedByte());
  }

  @Override
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

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());    
  }

  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());    
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    return new DataInputStream(this).readUTF();
  }
}
