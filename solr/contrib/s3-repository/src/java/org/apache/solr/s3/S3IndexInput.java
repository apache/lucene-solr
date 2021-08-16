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
package org.apache.solr.s3;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Locale;
import org.apache.lucene.store.BufferedIndexInput;

class S3IndexInput extends BufferedIndexInput {

  static final int LOCAL_BUFFER_SIZE = 16 * 1024;

  private final InputStream inputStream;
  private final long length;

  private long position;

  S3IndexInput(InputStream inputStream, String path, long length) {
    super(path);

    this.inputStream = inputStream;
    this.length = length;
  }

  @Override
  protected void readInternal(ByteBuffer b) throws IOException {

    int expectedLength = b.remaining();

    byte[] localBuffer;
    if (b.hasArray()) {
      localBuffer = b.array();
    } else {
      localBuffer = new byte[LOCAL_BUFFER_SIZE];
    }

    // We have no guarantee we read all the requested bytes from the underlying InputStream
    // in a single call. Loop until we reached the requested number of bytes.
    while (b.hasRemaining()) {
      int read;

      if (b.hasArray()) {
        read = inputStream.read(localBuffer, b.arrayOffset() + b.position(), b.remaining());
      } else {
        read = inputStream.read(localBuffer, 0, Math.min(b.remaining(), LOCAL_BUFFER_SIZE));
      }

      // Abort if we can't read any more data
      if (read < 0) {
        break;
      }

      if (b.hasArray()) {
        b.position(b.position() + read);
      } else {
        b.put(localBuffer, 0, read);
      }
    }

    if (b.remaining() > 0) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Failed to read %d bytes; only %d available",
              expectedLength,
              (expectedLength - b.remaining())));
    }

    position += expectedLength;
  }

  @Override
  protected void seekInternal(long toPosition) throws IOException {
    if (toPosition > length()) {
      throw new EOFException(
          "read past EOF: pos=" + toPosition + " vs length=" + length() + ": " + this);
    }

    // If we seek forward, skip unread bytes
    while (this.position < toPosition) {
      long skipped = inputStream.skip(toPosition - this.position);
      if (skipped == 0) {
        // If we didn't skip any bytes, make sure we are not at the end of the file
        if (inputStream.read() == -1) {
          // We are at the end of the file
          break;
        } else {
          // Not at the end of the file, and we did skip one byte with the read()
          skipped = 1;
        }
      }
      this.position += skipped;
    }
    if (this.position > toPosition) {
      throw new IOException("Cannot seek backward");
    }
  }

  @Override
  public final long length() {
    return length;
  }

  @Override
  public void close() throws IOException {
    this.inputStream.close();
  }
}
