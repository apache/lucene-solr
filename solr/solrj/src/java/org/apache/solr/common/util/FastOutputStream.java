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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Single threaded buffered OutputStream
 *  Internal Solr use only, subject to change.
 */
public class FastOutputStream extends OutputStream implements DataOutput {
  protected final OutputStream out;
  protected byte[] buf;
  protected long written;  // how many bytes written to the underlying stream
  protected int pos;

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
      written += pos;
      flush(buf, 0, buf.length);
      pos=0;
    }
    buf[pos++] = b;
  }

  @Override
  public void write(byte arr[], int off, int len) throws IOException {

    for(;;) {
      int space = buf.length - pos;

      if (len <= space) {
        System.arraycopy(arr, off, buf, pos, len);
        pos += len;
        return;
      } else if (len > buf.length) {
        if (pos>0) {
          flush(buf,0,pos);  // flush
          written += pos;
          pos=0;
        }
        // don't buffer, just write to sink
        flush(arr, off, len);
        written += len;
        return;
      }

      // buffer is too big to fit in the free space, but
      // not big enough to warrant writing on its own.
      // write whatever we can fit, then flush and iterate.

      System.arraycopy(arr, off, buf, pos, space);
      written += buf.length;  // important to do this first, since buf.length can change after a flush!
      flush(buf, 0, buf.length);
      pos = 0;
      off += space;
      len -= space;
    }
  }


  /** reserve at least len bytes at the end of the buffer.
   * Invalid if len &gt; buffer.length
   */
  public void reserve(int len) throws IOException {
    if (len > (buf.length - pos))
      flushBuffer();
  }

  ////////////////// DataOutput methods ///////////////////
  @Override
  public void writeBoolean(boolean v) throws IOException {
    write(v ? 1:0);
  }

  @Override
  public void writeByte(int v) throws IOException {
    write((byte)v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    write((byte)(v >>> 8));
    write((byte)v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    writeShort(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    reserve(4);
    buf[pos] = (byte)(v>>>24);
    buf[pos+1] = (byte)(v>>>16);
    buf[pos+2] = (byte)(v>>>8);
    buf[pos+3] = (byte)(v);
    pos+=4;
  }

  @Override
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

  @Override
  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToRawIntBits(v));
  }

  @Override
  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToRawLongBits(v));
  }

  @Override
  public void writeBytes(String s) throws IOException {
    // non-optimized version, but this shouldn't be used anyway
    for (int i=0; i<s.length(); i++)
      write((byte)s.charAt(i));
  }

  @Override
  public void writeChars(String s) throws IOException {
    // non-optimized version
    for (int i=0; i<s.length(); i++)
      writeChar(s.charAt(i)); 
  }

  @Override
  public void writeUTF(String s) throws IOException {
    // non-optimized version, but this shouldn't be used anyway
    DataOutputStream daos = new DataOutputStream(this);
    daos.writeUTF(s);
  }


  @Override
  public void flush() throws IOException {
    flushBuffer();
    if (out != null) out.flush();
  }

  @Override
  public void close() throws IOException {
    flushBuffer();
    if (out != null) out.close();
  }

  /** Only flushes the buffer of the FastOutputStream, not that of the
   * underlying stream.
   */
  public void flushBuffer() throws IOException {
    if (pos > 0) {
      written += pos;
      flush(buf, 0, pos);
      pos=0;
    }
  }

  /** All writes to the sink will go through this method */
  public void flush(byte[] buf, int offset, int len) throws IOException {
    out.write(buf, offset, len);
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

  /**Copies a {@link Utf8CharSequence} without making extra copies
   */
  public void writeUtf8CharSeq(Utf8CharSequence utf8) throws IOException {
    int start = 0;
    int totalWritten = 0;
    for (; ; ) {
      if (totalWritten >= utf8.size()) break;
      if (pos >= buf.length) flushBuffer();
      int sz = utf8.write(start, buf, pos);
      pos += sz;
      totalWritten += sz;
      start += sz;
    }
  }
}
