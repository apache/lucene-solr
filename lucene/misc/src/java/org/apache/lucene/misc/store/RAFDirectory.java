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
package org.apache.lucene.misc.store;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.SuppressForbidden;

/**
 * A straightforward implementation of {@link FSDirectory} using java.io.RandomAccessFile. However,
 * this class has poor concurrent performance (multiple threads will bottleneck) as it synchronizes
 * when multiple threads read from the same file. It's usually better to use {@link NIOFSDirectory}
 * or {@link MMapDirectory} instead.
 *
 * <p>NOTE: Because this uses RandomAccessFile, it will generally not work with non-default
 * filesystem providers. It is only provided for applications that relied on the fact that
 * RandomAccessFile's IO was not interruptible.
 */
@SuppressForbidden(reason = "java.io.File: RAFDirectory is legacy API")
public class RAFDirectory extends FSDirectory {

  /**
   * Create a new RAFDirectory for the named location. The directory is created at the named
   * location if it does not yet exist.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use
   * @throws IOException if there is a low-level I/O error
   */
  public RAFDirectory(Path path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
    path.toFile(); // throw exception if we can't get a File
  }

  /**
   * Create a new RAFDirectory for the named location and {@link FSLockFactory#getDefault()}. The
   * directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public RAFDirectory(Path path) throws IOException {
    this(path, FSLockFactory.getDefault());
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    ensureCanRead(name);
    final File path = directory.resolve(name).toFile();
    RandomAccessFile raf = new RandomAccessFile(path, "r");
    return new RAFIndexInput("SimpleFSIndexInput(path=\"" + path.getPath() + "\")", raf, context);
  }

  /**
   * Reads bytes with {@link RandomAccessFile#seek(long)} followed by {@link
   * RandomAccessFile#read(byte[], int, int)}.
   */
  @SuppressForbidden(reason = "java.io.File: RAFDirectory is legacy API")
  static final class RAFIndexInput extends BufferedIndexInput {
    /**
     * The maximum chunk size is 8192 bytes, because {@link RandomAccessFile} mallocs a native
     * buffer outside of stack if the read buffer size is larger.
     */
    private static final int CHUNK_SIZE = 8192;

    /** the file channel we will read from */
    protected final RandomAccessFile file;
    /** is this instance a clone and hence does not own the file to close it */
    boolean isClone = false;
    /** start offset: non-zero in the slice case */
    protected final long off;
    /** end offset (start+length) */
    protected final long end;

    public RAFIndexInput(String resourceDesc, RandomAccessFile file, IOContext context)
        throws IOException {
      super(resourceDesc, context);
      this.file = file;
      this.off = 0L;
      this.end = file.length();
    }

    public RAFIndexInput(
        String resourceDesc, RandomAccessFile file, long off, long length, int bufferSize) {
      super(resourceDesc, bufferSize);
      this.file = file;
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
    public RAFIndexInput clone() {
      RAFIndexInput clone = (RAFIndexInput) super.clone();
      clone.isClone = true;
      return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      if (offset < 0 || length < 0 || offset + length > this.length()) {
        throw new IllegalArgumentException(
            "slice() " + sliceDescription + " out of bounds: " + this);
      }
      return new RAFIndexInput(sliceDescription, file, off + offset, length, getBufferSize());
    }

    @Override
    public final long length() {
      return end - off;
    }

    /** IndexInput methods */
    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
      synchronized (file) {
        long position = off + getFilePointer();
        file.seek(position);

        if (position + b.remaining() > end) {
          throw new EOFException("read past EOF: " + this);
        }

        try {
          while (b.hasRemaining()) {
            final int toRead = Math.min(CHUNK_SIZE, b.remaining());
            final int i = file.read(b.array(), b.position(), toRead);
            if (i < 0) {
              // be defensive here, even though we checked before hand, something could have changed
              throw new EOFException(
                  "read past EOF: "
                      + this
                      + " off: "
                      + b.position()
                      + " len: "
                      + b.remaining()
                      + " chunkLen: "
                      + toRead
                      + " end: "
                      + end);
            }
            assert i > 0
                : "RandomAccessFile.read with non zero-length toRead must always read at least one byte";
            b.position(b.position() + i);
          }
        } catch (IOException ioe) {
          throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }
      }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
      if (pos > length()) {
        throw new EOFException(
            "read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
      }
    }

    boolean isFDValid() throws IOException {
      return file.getFD().valid();
    }
  }
}
