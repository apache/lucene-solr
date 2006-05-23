package org.apache.lucene.store;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

/**
 * A memory-resident {@link IndexOutput} implementation.
 *
 * @version $Id$
 */

public class RAMOutputStream extends BufferedIndexOutput {
  private RAMFile file;
  private long pointer = 0;

  /** Construct an empty output buffer. */
  public RAMOutputStream() {
    this(new RAMFile());
  }

  RAMOutputStream(RAMFile f) {
    file = f;
  }

  /** Copy the current contents of this buffer to the named output. */
  public void writeTo(IndexOutput out) throws IOException {
    flush();
    final long end = file.length;
    long pos = 0;
    int buffer = 0;
    while (pos < end) {
      int length = BUFFER_SIZE;
      long nextPos = pos + length;
      if (nextPos > end) {                        // at the last buffer
        length = (int)(end - pos);
      }
      out.writeBytes((byte[])file.buffers.elementAt(buffer++), length);
      pos = nextPos;
    }
  }

  /** Resets this to an empty buffer. */
  public void reset() {
    try {
      seek(0);
    } catch (IOException e) {                     // should never happen
      throw new RuntimeException(e.toString());
    }

    file.length = 0;
  }

  public void flushBuffer(byte[] src, int len) {
    byte[] buffer;
    int bufferPos = 0;
    while (bufferPos != len) {
      int bufferNumber = (int)(pointer/BUFFER_SIZE);
      int bufferOffset = (int)(pointer%BUFFER_SIZE);
      int bytesInBuffer = BUFFER_SIZE - bufferOffset;
      int remainInSrcBuffer = len - bufferPos;
      int bytesToCopy = bytesInBuffer >= remainInSrcBuffer ? remainInSrcBuffer : bytesInBuffer;

      if (bufferNumber == file.buffers.size()) {
        buffer = new byte[BUFFER_SIZE];
        file.buffers.addElement(buffer);
      } else {
        buffer = (byte[]) file.buffers.elementAt(bufferNumber);
      }

      System.arraycopy(src, bufferPos, buffer, bufferOffset, bytesToCopy);
      bufferPos += bytesToCopy;
      pointer += bytesToCopy;
    }

    if (pointer > file.length)
      file.length = pointer;

    file.lastModified = System.currentTimeMillis();
  }

  public void close() throws IOException {
    super.close();
  }

  public void seek(long pos) throws IOException {
    super.seek(pos);
    pointer = pos;
  }
  public long length() {
    return file.length;
  }
}
