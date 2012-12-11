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
    return new SimpleFSIndexInput("SimpleFSIndexInput(path=\"" + path.getPath() + "\")", path, context, getReadChunkSize());
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

      @Override
      public IndexInput openFullSlice() {
        try {
          return openSlice("full-slice", 0, descriptor.length());
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  /**
   * Reads bytes with {@link RandomAccessFile#seek(long)} followed by
   * {@link RandomAccessFile#read(byte[], int, int)}.  
   */
  protected static class SimpleFSIndexInput extends FSIndexInput {
  
    public SimpleFSIndexInput(String resourceDesc, File path, IOContext context, int chunkSize) throws IOException {
      super(resourceDesc, path, context, chunkSize);
    }
    
    public SimpleFSIndexInput(String resourceDesc, RandomAccessFile file, long off, long length, int bufferSize, int chunkSize) {
      super(resourceDesc, file, off, length, bufferSize, chunkSize);
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
  }
}
