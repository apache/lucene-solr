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
package org.apache.solr.servlet;

import java.io.IOException;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

import org.apache.solr.common.util.SuppressForbidden;

/**
 * Provides a convenient extension of the {@link ServletInputStream} class that can be subclassed by developers wishing
 * to adapt the behavior of a Stream. One such example may be to override {@link #close()} to instead be a no-op as in
 * SOLR-8933.
 *
 * This class implements the Wrapper or Decorator pattern. Methods default to calling through to the wrapped stream.
 */
@SuppressForbidden(reason = "delegate methods")
public class ServletInputStreamWrapper extends ServletInputStream {
  ServletInputStream stream;
  
  public ServletInputStreamWrapper(ServletInputStream stream) throws IOException {
    this.stream = stream;
  }
  
  public int hashCode() {
    return stream.hashCode();
  }

  public boolean equals(Object obj) {
    return stream.equals(obj);
  }

  public int available() throws IOException {
    return stream.available();
  }

  public void close() throws IOException {
    stream.close();
  }

  public boolean isFinished() {
    return stream.isFinished();
  }

  public boolean isReady() {
    return stream.isReady();
  }

  public int read() throws IOException {
    return stream.read();
  }

  public int read(byte[] b) throws IOException {
    return stream.read(b);
  }

  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  public void mark(int readlimit) {
    stream.mark(readlimit);
  }

  public boolean markSupported() {
    return stream.markSupported();
  }

  public int readLine(byte[] b, int off, int len) throws IOException {
    return stream.readLine(b, off, len);
  }

  public void reset() throws IOException {
    stream.reset();
  }

  public void setReadListener(ReadListener arg0) {
    stream.setReadListener(arg0);
  }

  public long skip(long n) throws IOException {
    return stream.skip(n);
  }

  public String toString() {
    return stream.toString();
  }
  
}
