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
package org.apache.lucene.gradle;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

public class SpillWriter extends Writer {
  private final static int MAX_BUFFERED = 2 * 1024;
  private final StringWriter buffer = new StringWriter(MAX_BUFFERED);

  private final Supplier<Path> spillPathSupplier;
  private Writer spill;
  private Path spillPath;

  public SpillWriter(Supplier<Path> spillPathSupplier) {
    this.spillPathSupplier = spillPathSupplier;
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    getSink(len).write(cbuf, off, len);
  }

  @Override
  public void write(int c) throws IOException {
    getSink(1).write(c);
  }

  @Override
  public void write(char[] cbuf) throws IOException {
    getSink(cbuf.length).write(cbuf);
  }

  @Override
  public void write(String str) throws IOException {
    getSink(str.length()).write(str);
  }

  @Override
  public void write(String str, int off, int len) throws IOException {
    getSink(len).write(str, off, len);
  }

  @Override
  public Writer append(CharSequence csq) throws IOException {
    getSink(csq.length()).append(csq);
    return this;
  }

  @Override
  public Writer append(CharSequence csq, int start, int end) throws IOException {
    getSink(Math.max(0, end - start)).append(csq, start, end);
    return this;
  }

  @Override
  public Writer append(char c) throws IOException {
    getSink(1).append(c);
    return this;
  }

  private Writer getSink(int expectedWriteChars) throws IOException {
    if (spill == null) {
      if (buffer.getBuffer().length() + expectedWriteChars <= MAX_BUFFERED) {
        return buffer;
      }

      spillPath = spillPathSupplier.get();
      spill = Files.newBufferedWriter(spillPath, StandardCharsets.UTF_8);
      spill.append(buffer.getBuffer());
      buffer.getBuffer().setLength(0);
    }

    return spill;
  }

  @Override
  public void flush() throws IOException {
    getSink(0).flush();
  }

  @Override
  public void close() throws IOException {
    buffer.close();
    if (spill != null) {
      spill.close();
      Files.delete(spillPath);
    }
  }

  public void copyTo(Writer writer) throws IOException {
    if (spill != null) {
      flush();
      try (Reader reader = Files.newBufferedReader(spillPath, StandardCharsets.UTF_8)) {
        reader.transferTo(writer);
      }
    } else {
      writer.append(buffer.getBuffer());
    }
  }

  public long length() throws IOException {
    flush();
    if (spill != null) {
      return Files.size(spillPath);
    } else {
      return buffer.getBuffer().length();
    }
  }
}
