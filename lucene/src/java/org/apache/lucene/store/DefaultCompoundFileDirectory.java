package org.apache.lucene.store;

/**
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

import org.apache.lucene.util.IOUtils;

/**
 * Default implementation of {@link CompoundFileDirectory}.
 * <p>
 * This implementation returns a BufferedIndexInput that wraps the underlying 
 * Directory's IndexInput for the compound file (using unbuffered reads).
 * @lucene.experimental
 */
public class DefaultCompoundFileDirectory extends CompoundFileDirectory {
  protected IndexInput stream;
  
  public DefaultCompoundFileDirectory(Directory directory, String fileName, IOContext context, boolean writeable) throws IOException {
    super(directory, fileName, context);
    if (!writeable) {
      try {
        stream = directory.openInput(fileName, context);
        initForRead(CompoundFileDirectory.readEntries(stream, directory, fileName));
      } catch (IOException e) {
        IOUtils.closeSafely(e, stream);
      }
    } else {
      initForWrite();
    }
  }
  
  @Override
  public IndexInput openInputSlice(String id, long offset, long length, int readBufferSize) throws IOException {
    return new CSIndexInput(stream, offset, length, readBufferSize);
  }
  
  @Override
  public synchronized void close() throws IOException {
    try {
      IOUtils.closeSafely(false, stream);
    } finally {
      super.close();
    }
  }

  /** Implementation of an IndexInput that reads from a portion of the
   *  compound file.
   */
  static final class CSIndexInput extends BufferedIndexInput {
    IndexInput base;
    long fileOffset;
    long length;
    
    CSIndexInput(final IndexInput base, final long fileOffset, final long length) {
      this(base, fileOffset, length, BufferedIndexInput.BUFFER_SIZE);
    }
    
    CSIndexInput(final IndexInput base, final long fileOffset, final long length, int readBufferSize) {
      super(readBufferSize);
      this.base = (IndexInput)base.clone();
      this.fileOffset = fileOffset;
      this.length = length;
    }
    
    @Override
    public Object clone() {
      CSIndexInput clone = (CSIndexInput)super.clone();
      clone.base = (IndexInput)base.clone();
      clone.fileOffset = fileOffset;
      clone.length = length;
      return clone;
    }
    
    /** Expert: implements buffer refill.  Reads bytes from the current
     *  position in the input.
     * @param b the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len the number of bytes to read
     */
    @Override
    protected void readInternal(byte[] b, int offset, int len) throws IOException {
      long start = getFilePointer();
      if(start + len > length)
        throw new IOException("read past EOF");
      base.seek(fileOffset + start);
      base.readBytes(b, offset, len, false);
    }
    
    /** Expert: implements seek.  Sets current position in this file, where
     *  the next {@link #readInternal(byte[],int,int)} will occur.
     * @see #readInternal(byte[],int,int)
     */
    @Override
    protected void seekInternal(long pos) {}
    
    /** Closes the stream to further operations. */
    @Override
    public void close() throws IOException {
      base.close();
    }
    
    @Override
    public long length() {
      return length;
    }
    
    @Override
    public void copyBytes(IndexOutput out, long numBytes) throws IOException {
      // Copy first whatever is in the buffer
      numBytes -= flushBuffer(out, numBytes);
      
      // If there are more bytes left to copy, delegate the copy task to the
      // base IndexInput, in case it can do an optimized copy.
      if (numBytes > 0) {
        long start = getFilePointer();
        if (start + numBytes > length) {
          throw new IOException("read past EOF");
        }
        base.seek(fileOffset + start);
        base.copyBytes(out, numBytes);
      }
    }
  }
}
