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
package org.apache.lucene.store;

import java.io.IOException;

/**
 * Extension of IndexInput, computing checksum as it goes. Callers can retrieve the checksum via
 * {@link #getChecksum()}.
 */
public abstract class ChecksumIndexInput extends IndexInput {

  private static final int SKIP_BUFFER_SIZE = 1024;

  /* This buffer is used when skipping bytes in skipBytes(). Skipping bytes
   * still requires reading in the bytes we skip in order to update the checksum.
   * The reason we need to use an instance member instead of sharing a single
   * static instance across threads is that multiple instances invoking skipBytes()
   * concurrently on different threads can clobber the contents of a shared buffer,
   * corrupting the checksum. See LUCENE-5583 for additional context.
   */
  private byte[] skipBuffer;

  /**
   * resourceDescription should be a non-null, opaque string describing this resource; it's returned
   * from {@link #toString}.
   */
  protected ChecksumIndexInput(String resourceDescription) {
    super(resourceDescription);
  }

  /** Returns the current checksum value */
  public abstract long getChecksum() throws IOException;

  /**
   * {@inheritDoc}
   *
   * <p>{@link ChecksumIndexInput} can only seek forward and seeks are expensive since they imply to
   * read bytes in-between the current position and the target position in order to update the
   * checksum.
   */
  @Override
  public void seek(long pos) throws IOException {
    final long curFP = getFilePointer();
    final long skip = pos - curFP;
    if (skip < 0) {
      throw new IllegalStateException(
          getClass() + " cannot seek backwards (pos=" + pos + " getFilePointer()=" + curFP + ")");
    }
    skipByReading(skip);
  }

  /**
   * Skip over <code>numBytes</code> bytes. The contract on this method is that it should have the
   * same behavior as reading the same number of bytes into a buffer and discarding its content.
   * Negative values of <code>numBytes</code> are not supported.
   */
  private void skipByReading(long numBytes) throws IOException {
    if (skipBuffer == null) {
      skipBuffer = new byte[SKIP_BUFFER_SIZE];
    }
    assert skipBuffer.length == SKIP_BUFFER_SIZE;
    for (long skipped = 0; skipped < numBytes; ) {
      final int step = (int) Math.min(SKIP_BUFFER_SIZE, numBytes - skipped);
      readBytes(skipBuffer, 0, step, false);
      skipped += step;
    }
  }
}
