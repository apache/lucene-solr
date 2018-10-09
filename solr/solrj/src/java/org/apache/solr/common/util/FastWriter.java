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

import java.io.IOException;
import java.io.Writer;

/** Single threaded BufferedWriter
 *  Internal Solr use only, subject to change.
 */
public class FastWriter extends Writer {
  // use default BUFSIZE of BufferedWriter so if we wrap that
  // it won't cause double buffering.
  private static final int BUFSIZE = 8192;
  protected final Writer sink;
  protected char[] buf;
  protected int pos;

  public FastWriter(Writer w) {
    this(w, new char[BUFSIZE], 0);
  }

  public FastWriter(Writer sink, char[] tempBuffer, int start) {
    this.sink = sink;
    this.buf = tempBuffer;
    this.pos = start;
  }

  public static FastWriter wrap(Writer sink) {
    return (sink instanceof FastWriter) ? (FastWriter)sink : new FastWriter(sink);
  }

  @Override
  public void write(int c) throws IOException {
    write((char)c); 
  }

  public void write(char c) throws IOException {
    if (pos >= buf.length) {
      flush(buf,0,pos);
      pos=0;
    }
    buf[pos++] = c;
  }

  @Override
  public FastWriter append(char c) throws IOException {
    if (pos >= buf.length) {
      flush(buf,0,pos);
      pos=0;
    }
    buf[pos++] = c;
    return this;
  }

  @Override
  public void write(char arr[], int off, int len) throws IOException {
    for(;;) {
      int space = buf.length - pos;

      if (len <= space) {
        System.arraycopy(arr, off, buf, pos, len);
        pos += len;
        return;
      } else if (len > buf.length) {
        if (pos>0) {
          flush(buf,0,pos);  // flush
          pos=0;
        }
        // don't buffer, just write to sink
        flush(arr, off, len);
        return;
      }

      // buffer is too big to fit in the free space, but
      // not big enough to warrant writing on its own.
      // write whatever we can fit, then flush and iterate.

      System.arraycopy(arr, off, buf, pos, space);
      flush(buf, 0, buf.length);
      pos = 0;
      off += space;
      len -= space;
    }
  }

  @Override
  public void write(String str, int off, int len) throws IOException {
    for(;;) {
      int space = buf.length - pos;

      if (len <= space) {
        str.getChars(off, off+len, buf, pos);
        pos += len;
        return;
      } else if (len > buf.length) {
        if (pos>0) {
          flush(buf,0,pos);  // flush
          pos=0;
        }
        // don't buffer, just write to sink
        flush(str, off, len);
        return;
      }

      // buffer is too big to fit in the free space, but
      // not big enough to warrant writing on its own.
      // write whatever we can fit, then flush and iterate.

      str.getChars(off, off+space, buf, pos);
      flush(buf, 0, buf.length);
      pos = 0;
      off += space;
      len -= space;
    }
  }

  @Override
  public void flush() throws IOException {
    flush(buf, 0, pos);
    pos=0;
    if (sink != null) sink.flush();
  }

  public void flush(char[] buf, int offset, int len) throws IOException {
    sink.write(buf, offset, len);
  }

  public void flush(String str, int offset, int len) throws IOException {
    sink.write(str, offset, len);
  }

  @Override
  public void close() throws IOException {
    flush();
    if (sink != null) sink.close();
  }

  public void flushBuffer() throws IOException {
    flush(buf, 0, pos);
    pos=0;
  }
}
