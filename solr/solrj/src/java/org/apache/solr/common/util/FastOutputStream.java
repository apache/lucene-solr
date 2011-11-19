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

/** Single threaded buffered OutputStream
 *  Internal Solr use only, subject to change.
 */
public class FastOutputStream extends OutputStream implements DataOutput {
  private final OutputStream out;
  private final byte[] buf;
  private long written;  // how many bytes written to the underlying stream
  private int pos;

  public FastOutputStream(OutputStream w) {
  // use default BUFSIZE of BufferedOutputStream so if we wrap that
  // it won't cause double buffering.
    this(w, new byte[8192], 0);
  }

  public FastOutputStream(OutputStream sink, byte[] tempBuffer, int start) {
    this.out = sink;
    this.buf = tempBuffer;
    this.pos = start;
  }


  public static FastOutputStream wrap(OutputStream sink) {
   return (sink instanceof FastOutputStream) ? (FastOutputStream)sink : new FastOutputStream(sink);
  }

  @Override
  public void write(int b) throws IOException {
    write((byte)b);
  }

  @Override
  public void write(byte b[]) throws IOException {
    write(b,0,b.length);
  }

  public void write(byte b) throws IOException {
    if (pos >= buf.length) {
      out.write(buf);
      written += pos;
      pos=0;
    }
    buf[pos++] = b;
  }

  @Override
  public void write(byte arr[], int off, int len) throws IOException {
    int space = buf.length - pos;
    if (len < space) {
      System.arraycopy(arr, off, buf, pos, len);
      pos += len;
    } else if (len<buf.length) {
      // if the data to write is small enough, buffer it.
      System.arraycopy(arr, off, buf, pos, space);
      out.write(buf);
      written += buf.length;
      pos = len-space;
      System.arraycopy(arr, off+space, buf, 0, pos);
    } else {
      if (pos>0) {
        out.write(buf,0,pos);  // flush
        written += pos;
        pos=0;
      }
      // don't buffer, just write to sink
      out.write(arr, off, len);
      written += len;            
    }
  }

  /** reserve at least len bytes at the end of the buffer.
   * Invalid if len > buffer.length
   * @param len
   */
  public void reserve(int len) throws IOException {
    if (len > (buf.length - pos))
      flushBuffer();
  }

  ////////////////// DataOutput methods ///////////////////
  public void writeBoolean(boolean v) throws IOException {
    write(v ? 1:0);
  }

  public void writeByte(int v) throws IOException {
    write((byte)v);
  }

  public void writeShort(int v) throws IOException {
    write((byte)(v >>> 8));
    write((byte)v);
  }

  public void writeChar(int v) throws IOException {
    writeShort(v);
  }

  public void writeInt(int v) throws IOException {
    reserve(4);
    buf[pos] = (byte)(v>>>24);
    buf[pos+1] = (byte)(v>>>16);
    buf[pos+2] = (byte)(v>>>8);
    buf[pos+3] = (byte)(v);
    pos+=4;
  }

  public void writeLong(long v) throws IOException {
    reserve(8);
    buf[pos] = (byte)(v>>>56);
    buf[pos+1] = (byte)(v>>>48);
    buf[pos+2] = (byte)(v>>>40);
    buf[pos+3] = (byte)(v>>>32);
    buf[pos+4] = (byte)(v>>>24);
    buf[pos+5] = (byte)(v>>>16);
    buf[pos+6] = (byte)(v>>>8);
    buf[pos+7] = (byte)(v);
    pos+=8;
  }

  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToRawIntBits(v));
  }

  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToRawLongBits(v));
  }

  public void writeBytes(String s) throws IOException {
    // non-optimized version, but this shouldn't be used anyway
    for (int i=0; i<s.length(); i++)
      write((byte)s.charAt(i));
  }

  public void writeChars(String s) throws IOException {
    // non-optimized version
    for (int i=0; i<s.length(); i++)
      writeChar(s.charAt(i)); 
  }

  public void writeUTF(String s) throws IOException {
    // non-optimized version, but this shouldn't be used anyway
    DataOutputStream daos = new DataOutputStream(this);
    daos.writeUTF(s);
  }


  @Override
  public void flush() throws IOException {
    flushBuffer();
    out.flush();
  }

  @Override
  public void close() throws IOException {
    flushBuffer();
    out.close();
  }

  /** Only flushes the buffer of the FastOutputStream, not that of the
   * underlying stream.
   */
  public void flushBuffer() throws IOException {
    if (pos > 0) {
      out.write(buf, 0, pos);
      written += pos;
      pos=0;
    }
  }

  public long size() {
    return written + pos;
  }

  /** Returns the number of bytes actually written to the underlying OutputStream, not including
   * anything currently buffered by this class itself.
   */
  public long written() {
    return written;
  }

  /** Resets the count returned by written() */
  public void setWritten(long written) {
    this.written = written;
  }

}
