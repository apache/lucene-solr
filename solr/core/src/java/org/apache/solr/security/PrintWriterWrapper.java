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
package org.apache.solr.security;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;

/**
 * Wrapper for PrintWriter that delegates to constructor arg
 */
public class PrintWriterWrapper extends PrintWriter {
  private PrintWriter printWriter;

  public PrintWriterWrapper(PrintWriter printWriter) {
    super(new StringWriter());
    this.printWriter = printWriter;
  }

  @Override
  public PrintWriter append(char c) {
    return printWriter.append(c);
  }

  @Override
  public PrintWriter append(CharSequence csq) {
    return printWriter.append(csq);
  }

  @Override
  public PrintWriter append(CharSequence csq, int start, int end) {
    return printWriter.append(csq, start, end);
  }

  @Override
  public boolean checkError() {
    return printWriter.checkError();
  }

  @Override
  protected void clearError() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    printWriter.close();
  }

  @Override
  public void flush() {
    printWriter.flush();
  }

  @Override
  public PrintWriter format(Locale l, String format, Object... args) {
    return printWriter.format(l, format, args);
  }

  @Override
  public PrintWriter format(String format, Object... args) {
    throw new UnsupportedOperationException("Forbidden API");
  }

  @Override
  public void print(boolean b) {
    printWriter.print(b);
  }

  @Override
  public void print(char c) {
    printWriter.print(c);
  }

  @Override
  public void print(char[] s) {
    printWriter.print(s);
  }

  @Override
  public void print(double d) {
    printWriter.print(d);
  }

  @Override
  public void print(float f) {
    printWriter.print(f);
  }

  @Override
  public void print(int i) {
    printWriter.print(i);
  }

  @Override
  public void print(long l) {
    printWriter.print(l);
  }

  @Override
  public void print(Object obj) {
    printWriter.print(obj);
  }

  @Override
  public void print(String s) {
    printWriter.print(s);
  }

  @Override
  public PrintWriter printf(Locale l, String format, Object... args) {
    return printWriter.printf(l, format, args);
  }

  @Override
  public PrintWriter printf(String format, Object... args) {
    throw new UnsupportedOperationException("Forbidden API");
  }

  @Override
  public void println() {
    printWriter.println();
  }

  @Override
  public void println(boolean x) {
    printWriter.println(x);
  }

  @Override
  public void println(char x) {
    printWriter.println(x);
  }

  @Override
  public void println(char[] x) {
    printWriter.println(x);
  }

  @Override
  public void println(double x) {
    printWriter.println(x);
  }

  @Override
  public void println(float x) {
    printWriter.println(x);
  }

  @Override
  public void println(int x) {
    printWriter.println(x);
  }

  @Override
  public void println(long x) {
    printWriter.println(x);
  }

  @Override
  public void println(Object x) {
    printWriter.println(x);
  }

  @Override
  public void println(String x) {
    printWriter.println(x);
  }

  @Override
  protected void setError() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(char[] buf) {
    printWriter.write(buf);
  }

  @Override
  public void write(char[] buf, int off, int len) {
    printWriter.write(buf, off, len);
  }

  @Override
  public void write(int c) {
    printWriter.write(c);
  }

  @Override
  public void write(String s) {
    printWriter.write(s);
  }

  @Override
  public void write(String s, int off, int len) {
    printWriter.write(s, off, len);
  }
}
