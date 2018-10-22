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
package org.apache.solr.store.blockcache;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

/**
 * @lucene.experimental
 */
public abstract class ReusedBufferedIndexOutput extends IndexOutput {
  
  public static final int BUFFER_SIZE = 1024;
  
  private int bufferSize = BUFFER_SIZE;
  
  protected byte[] buffer;
  
  /** position in the file of buffer */
  private long bufferStart = 0;
  /** end of valid bytes */
  private int bufferLength = 0;
  /** next byte to write */
  private int bufferPosition = 0;
  /** total length of the file */
  private long fileLength = 0;
  
  private final Store store;
  
  public ReusedBufferedIndexOutput(String resourceDescription, String name) {
    this(resourceDescription, name, BUFFER_SIZE);
  }
  
  public ReusedBufferedIndexOutput(String resourceDescription, String name, int bufferSize) {
    super(resourceDescription, name);
    checkBufferSize(bufferSize);
    this.bufferSize = bufferSize;
    store = BufferStore.instance(bufferSize);
    buffer = store.takeBuffer(this.bufferSize);
  }
  
  protected long getBufferStart() {
    return bufferStart;
  }
  
  private void checkBufferSize(int bufferSize) {
    if (bufferSize <= 0) throw new IllegalArgumentException(
        "bufferSize must be greater than 0 (got " + bufferSize + ")");
  }
  
  /** Write the buffered bytes to cache */
  protected void flushBufferToCache() throws IOException {
    writeInternal(buffer, 0, bufferLength);
    
    bufferStart += bufferLength;
    bufferLength = 0;
    bufferPosition = 0;
  }
  
  protected abstract void closeInternal() throws IOException;
  
  @Override
  public void close() throws IOException {
    flushBufferToCache();
    closeInternal();
    store.putBuffer(buffer);
    buffer = null;
  }
  
  @Override
  public long getFilePointer() {
    return bufferStart + bufferPosition;
  }
  
  @Override
  public void writeByte(byte b) throws IOException {
    if (bufferPosition >= bufferSize) {
      flushBufferToCache();
    }
    if (getFilePointer() >= fileLength) {
      fileLength++;
    }
    buffer[bufferPosition++] = b;
    if (bufferPosition > bufferLength) {
      bufferLength = bufferPosition;
    }
  }
  
  /**
   * Expert: implements buffer flushing to cache. Writes bytes to the current
   * position in the output.
   * 
   * @param b
   *          the array of bytes to write
   * @param offset
   *          the offset in the array of bytes to write
   * @param length
   *          the number of bytes to write
   */
  protected abstract void writeInternal(byte[] b, int offset, int length)
      throws IOException;
  
  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    if (getFilePointer() + length > fileLength) {
      fileLength = getFilePointer() + length;
    }
    if (length <= bufferSize - bufferPosition) {
      // the buffer contains enough space to satisfy this request
      if (length > 0) { // to allow b to be null if len is 0...
        System.arraycopy(b, offset, buffer, bufferPosition, length);
      }
      bufferPosition += length;
      if (bufferPosition > bufferLength) {
        bufferLength = bufferPosition;
      }
    } else {
      // the buffer does not have enough space. First buffer all we've got.
      int available = bufferSize - bufferPosition;
      if (available > 0) {
        System.arraycopy(b, offset, buffer, bufferPosition, available);
        offset += available;
        length -= available;
        bufferPosition = bufferSize;
        bufferLength = bufferSize;
      }
      
      flushBufferToCache();
      
      // and now, write the remaining 'length' bytes:
      if (length < bufferSize) {
        // If the amount left to write is small enough do it in the usual
        // buffered way:
        System.arraycopy(b, offset, buffer, 0, length);
        bufferPosition = length;
        bufferLength = length;
      } else {
        // The amount left to write is larger than the buffer
        // there's no performance reason not to write it all
        // at once.
        writeInternal(b, offset, length);
        bufferStart += length;
        bufferPosition = 0;
        bufferLength = 0;
      }
      
    }
  }
}
