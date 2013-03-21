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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/** A straightforward implementation of {@link FSDirectory}
 *  using java.io.RandomAccessFile.  However, this class has
 *  poor concurrent performance (multiple threads will
 *  bottleneck) as it synchronizes when multiple threads
 *  read from the same file.  It's usually better to use
 *  {@link NIOFSDirectory} or {@link MMapDirectory} instead. */
public class SimpleFSDirectory extends FSDirectory {
    
  /** Create a new SimpleFSDirectory for the named location.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException if there is a low-level I/O error
   */
  public SimpleFSDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
  }
  
  /** Create a new SimpleFSDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public SimpleFSDirectory(File path) throws IOException {
    super(path, null);
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    final File path = new File(directory, name);
    RandomAccessFile raf = new RandomAccessFile(path, "r");
    return new SimpleFSIndexInput("SimpleFSIndexInput(path=\"" + path.getPath() + "\")", raf, context, getReadChunkSize());
  }

  @Override
  public IndexInputSlicer createSlicer(final String name,
      final IOContext context) throws IOException {
    ensureOpen();
    final File file = new File(getDirectory(), name);
    final RandomAccessFile descriptor = new RandomAccessFile(file, "r");
    return new IndexInputSlicer() {

      @Override
      public void close() throws IOException {
        descriptor.close();
      }

      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) {
        return new SimpleFSIndexInput("SimpleFSIndexInput(" + sliceDescription + " in path=\"" + file.getPath() + "\" slice=" + offset + ":" + (offset+length) + ")", descriptor, offset,
            length, BufferedIndexInput.bufferSize(context), getReadChunkSize());
      }
    };
  }

  /**
   * Reads bytes with {@link RandomAccessFile#seek(long)} followed by
   * {@link RandomAccessFile#read(byte[], int, int)}.  
   */
  protected static class SimpleFSIndexInput extends BufferedIndexInput {
    /** the file channel we will read from */
    protected final RandomAccessFile file;
    /** is this instance a clone and hence does not own the file to close it */
    boolean isClone = false;
    /** maximum read length on a 32bit JVM to prevent incorrect OOM, see LUCENE-1566 */ 
    protected final int chunkSize;
    /** start offset: non-zero in the slice case */
    protected final long off;
    /** end offset (start+length) */
    protected final long end;
    
    public SimpleFSIndexInput(String resourceDesc, RandomAccessFile file, IOContext context, int chunkSize) throws IOException {
      super(resourceDesc, context);
      this.file = file; 
      this.chunkSize = chunkSize;
      this.off = 0L;
      this.end = file.length();
    }
    
    public SimpleFSIndexInput(String resourceDesc, RandomAccessFile file, long off, long length, int bufferSize, int chunkSize) {
      super(resourceDesc, bufferSize);
      this.file = file;
      this.chunkSize = chunkSize;
      this.off = off;
      this.end = off + length;
      this.isClone = true;
    }
    
    @Override
    public void close() throws IOException {
      if (!isClone) {
        file.close();
      }
    }
    
    @Override
    public SimpleFSIndexInput clone() {
      SimpleFSIndexInput clone = (SimpleFSIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
    
    @Override
    public final long length() {
      return end - off;
    }
  
    /** IndexInput methods */
    @Override
    protected void readInternal(byte[] b, int offset, int len)
         throws IOException {
      synchronized (file) {
        long position = off + getFilePointer();
        file.seek(position);
        int total = 0;

        if (position + len > end) {
          throw new EOFException("read past EOF: " + this);
        }

        try {
          do {
            final int readLength;
            if (total + chunkSize > len) {
              readLength = len - total;
            } else {
              // LUCENE-1566 - work around JVM Bug by breaking very large reads into chunks
              readLength = chunkSize;
            }
            final int i = file.read(b, offset + total, readLength);
            total += i;
          } while (total < len);
        } catch (OutOfMemoryError e) {
          // propagate OOM up and add a hint for 32bit VM Users hitting the bug
          // with a large chunk size in the fast path.
          final OutOfMemoryError outOfMemoryError = new OutOfMemoryError(
              "OutOfMemoryError likely caused by the Sun VM Bug described in "
              + "https://issues.apache.org/jira/browse/LUCENE-1566; try calling FSDirectory.setReadChunkSize "
              + "with a value smaller than the current chunk size (" + chunkSize + ")");
          outOfMemoryError.initCause(e);
          throw outOfMemoryError;
        } catch (IOException ioe) {
          throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }
      }
    }
  
    @Override
    protected void seekInternal(long position) {
    }
    
    boolean isFDValid() throws IOException {
      return file.getFD().valid();
    }
  }
}
