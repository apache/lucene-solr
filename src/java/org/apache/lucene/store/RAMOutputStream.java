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

public class RAMOutputStream extends OutputStream {
  private RAMFile file;
  private int pointer = 0;

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
    int bufferNumber = pointer/BUFFER_SIZE;
    int bufferOffset = pointer%BUFFER_SIZE;
    int bytesInBuffer = BUFFER_SIZE - bufferOffset;
    int bytesToCopy = bytesInBuffer >= len ? len : bytesInBuffer;

    if (bufferNumber == file.buffers.size())
      file.buffers.addElement(new byte[BUFFER_SIZE]);

    byte[] buffer = (byte[])file.buffers.elementAt(bufferNumber);
    System.arraycopy(src, 0, buffer, bufferOffset, bytesToCopy);

    if (bytesToCopy < len) {			  // not all in one buffer
      int srcOffset = bytesToCopy;
      bytesToCopy = len - bytesToCopy;		  // remaining bytes
      bufferNumber++;
      if (bufferNumber == file.buffers.size())
        file.buffers.addElement(new byte[BUFFER_SIZE]);
      buffer = (byte[])file.buffers.elementAt(bufferNumber);
      System.arraycopy(src, srcOffset, buffer, 0, bytesToCopy);
    }
    pointer += len;
    if (pointer > file.length)
      file.length = pointer;

    file.lastModified = System.currentTimeMillis();
  }

  public void close() throws IOException {
    super.close();
  }

  public void seek(long pos) throws IOException {
    super.seek(pos);
    pointer = (int)pos;
  }
  public long length() {
    return file.length;
  }
}
