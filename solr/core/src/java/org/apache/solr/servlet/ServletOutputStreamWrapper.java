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

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

import org.apache.solr.common.util.SuppressForbidden;

/**
 * Provides a convenient extension of the {@link ServletOutputStream} class that can be subclassed by developers wishing
 * to adapt the behavior of a Stream. One such example may be to override {@link #close()} to instead be a no-op as in
 * SOLR-8933.
 *
 * This class implements the Wrapper or Decorator pattern. Methods default to calling through to the wrapped stream.
 */
@SuppressForbidden(reason = "delegate methods")
public class ServletOutputStreamWrapper extends ServletOutputStream {
  ServletOutputStream stream;
  
  public ServletOutputStreamWrapper(ServletOutputStream stream) {
    this.stream = stream;
  }

  public int hashCode() {
    return stream.hashCode();
  }

  public boolean equals(Object obj) {
    return stream.equals(obj);
  }

  public void flush() throws IOException {
    stream.flush();
  }

  public void close() throws IOException {
    stream.close();
  }

  public boolean isReady() {
    return stream.isReady();
  }

  public void print(boolean arg0) throws IOException {
    stream.print(arg0);
  }

  public void print(char c) throws IOException {
    stream.print(c);
  }

  public void print(double d) throws IOException {
    stream.print(d);
  }

  public void print(float f) throws IOException {
    stream.print(f);
  }

  public void print(int i) throws IOException {
    stream.print(i);
  }

  public void print(long l) throws IOException {
    stream.print(l);
  }

  public void print(String arg0) throws IOException {
    stream.print(arg0);
  }

  public void println() throws IOException {
    stream.println();
  }

  public void println(boolean b) throws IOException {
    stream.println(b);
  }

  public void println(char c) throws IOException {
    stream.println(c);
  }

  public void println(double d) throws IOException {
    stream.println(d);
  }

  public void println(float f) throws IOException {
    stream.println(f);
  }

  public void println(int i) throws IOException {
    stream.println(i);
  }

  public void println(long l) throws IOException {
    stream.println(l);
  }

  public void println(String s) throws IOException {
    stream.println(s);
  }

  public void setWriteListener(WriteListener arg0) {
    stream.setWriteListener(arg0);
  }

  public void write(int b) throws IOException {
    stream.write(b);
  }

  public void write(byte[] b) throws IOException {
    stream.write(b);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
  }

  public String toString() {
    return stream.toString();
  }
}
