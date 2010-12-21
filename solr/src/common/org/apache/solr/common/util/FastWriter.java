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

import java.io.Writer;
import java.io.IOException;

/** Single threaded BufferedWriter
 *  Internal Solr use only, subject to change.
 */
public class FastWriter extends Writer {
  // use default BUFSIZE of BufferedWriter so if we wrap that
  // it won't cause double buffering.
  private static final int BUFSIZE = 8192;
  protected final Writer sink;
  protected final char[] buf;
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
      sink.write(buf,0,pos);
      pos=0;
    }
    buf[pos++] = c;
  }

  @Override
  public FastWriter append(char c) throws IOException {
    if (pos >= buf.length) {
      sink.write(buf,0,pos);
      pos=0;
    }
    buf[pos++] = c;
    return this;
  }

  @Override
  public void write(char cbuf[], int off, int len) throws IOException {
    int space = buf.length - pos;
    if (len < space) {
      System.arraycopy(cbuf, off, buf, pos, len);
      pos += len;
    } else if (len<BUFSIZE) {
      // if the data to write is small enough, buffer it.
      System.arraycopy(cbuf, off, buf, pos, space);
      sink.write(buf, 0, buf.length);
      pos = len-space;
      System.arraycopy(cbuf, off+space, buf, 0, pos);
    } else {
      sink.write(buf,0,pos);  // flush
      pos=0;
      // don't buffer, just write to sink
      sink.write(cbuf, off, len);
    }
  }

  @Override
  public void write(String str, int off, int len) throws IOException {
    int space = buf.length - pos;
    if (len < space) {
      str.getChars(off, off+len, buf, pos);
      pos += len;
    } else if (len<BUFSIZE) {
      // if the data to write is small enough, buffer it.
      str.getChars(off, off+space, buf, pos);
      sink.write(buf, 0, buf.length);
      str.getChars(off+space, off+len, buf, 0);
      pos = len-space;
    } else {
      sink.write(buf,0,pos);  // flush
      pos=0;
      // don't buffer, just write to sink
      sink.write(str, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    sink.write(buf,0,pos);
    pos=0;
    sink.flush();
  }

  @Override
  public void close() throws IOException {
    flush();
    sink.close();
  }

  public void flushBuffer() throws IOException {
    sink.write(buf, 0, pos);
    pos=0;
  }
}
