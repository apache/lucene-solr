package org.apache.lucene.store;

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

import java.io.IOException;
import java.io.EOFException;

/** A memory-resident {@link IndexInput} implementation. 
 *  
 *  @lucene.internal */
public class RAMInputStream extends IndexInput implements Cloneable {
  static final int BUFFER_SIZE = RAMOutputStream.BUFFER_SIZE;

  private RAMFile file;
  private long length;

  private byte[] currentBuffer;
  private int currentBufferIndex;
  
  private int bufferPosition;
  private long bufferStart;
  private int bufferLength;

  public RAMInputStream(String name, RAMFile f) throws IOException {
    super("RAMInputStream(name=" + name + ")");
    file = f;
    length = file.length;
    if (length/BUFFER_SIZE >= Integer.MAX_VALUE) {
      throw new IOException("RAMInputStream too large length=" + length + ": " + name); 
    }

    // make sure that we switch to the
    // first needed buffer lazily
    currentBufferIndex = -1;
    currentBuffer = null;
  }

  @Override
  public void close() {
    // nothing to do here
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public byte readByte() throws IOException {
    if (bufferPosition >= bufferLength) {
      currentBufferIndex++;
      switchCurrentBuffer(true);
    }
    return currentBuffer[bufferPosition++];
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      if (bufferPosition >= bufferLength) {
        currentBufferIndex++;
        switchCurrentBuffer(true);
      }

      int remainInBuffer = bufferLength - bufferPosition;
      int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
      System.arraycopy(currentBuffer, bufferPosition, b, offset, bytesToCopy);
      offset += bytesToCopy;
      len -= bytesToCopy;
      bufferPosition += bytesToCopy;
    }
  }

  private final void switchCurrentBuffer(boolean enforceEOF) throws IOException {
    bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
    if (currentBufferIndex >= file.numBuffers()) {
      // end of file reached, no more buffers left
      if (enforceEOF) {
        throw new EOFException("read past EOF: " + this);
      } else {
        // Force EOF if a read takes place at this position
        currentBufferIndex--;
        bufferPosition = BUFFER_SIZE;
      }
    } else {
      currentBuffer = file.getBuffer(currentBufferIndex);
      bufferPosition = 0;
      long buflen = length - bufferStart;
      bufferLength = buflen > BUFFER_SIZE ? BUFFER_SIZE : (int) buflen;
    }
  }

  @Override
  public long getFilePointer() {
    return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (currentBuffer==null || pos < bufferStart || pos >= bufferStart + BUFFER_SIZE) {
      currentBufferIndex = (int) (pos / BUFFER_SIZE);
      switchCurrentBuffer(false);
    }
    bufferPosition = (int) (pos % BUFFER_SIZE);
  }
}
