package org.apache.lucene.store;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.File;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException; // javadoc @link
import java.nio.channels.FileChannel;
import java.util.concurrent.Future; // javadoc

/**
 * An {@link FSDirectory} implementation that uses java.nio's FileChannel's
 * positional read, which allows multiple threads to read from the same file
 * without synchronizing.
 * <p>
 * This class only uses FileChannel when reading; writing is achieved with
 * {@link FSDirectory.FSIndexOutput}.
 * <p>
 * <b>NOTE</b>: NIOFSDirectory is not recommended on Windows because of a bug in
 * how FileChannel.read is implemented in Sun's JRE. Inside of the
 * implementation the position is apparently synchronized. See <a
 * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6265734">here</a>
 * for details.
 * </p>
 * <p>
 * <font color="red"><b>NOTE:</b> Accessing this class either directly or
 * indirectly from a thread while it's interrupted can close the
 * underlying file descriptor immediately if at the same time the thread is
 * blocked on IO. The file descriptor will remain closed and subsequent access
 * to {@link NIOFSDirectory} will throw a {@link ClosedChannelException}. If
 * your application uses either {@link Thread#interrupt()} or
 * {@link Future#cancel(boolean)} you should use {@link SimpleFSDirectory} in
 * favor of {@link NIOFSDirectory}.</font>
 * </p>
 */
public class NIOFSDirectory extends FSDirectory {

  /** Create a new NIOFSDirectory for the named location.
   * 
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException if there is a low-level I/O error
   */
  public NIOFSDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
  }

  /** Create a new NIOFSDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public NIOFSDirectory(File path) throws IOException {
    super(path, null);
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    return new NIOFSIndexInput(new File(getDirectory(), name), context, getReadChunkSize());
  }
  
  @Override
  public IndexInputSlicer createSlicer(final String name,
      final IOContext context) throws IOException {
    ensureOpen();
    final File path = new File(getDirectory(), name);
    final RandomAccessFile descriptor = new RandomAccessFile(path, "r");
    return new Directory.IndexInputSlicer() {

      @Override
      public void close() throws IOException {
        descriptor.close();
      }

      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) {
        return new NIOFSIndexInput(sliceDescription, path, descriptor, descriptor.getChannel(), offset,
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
   * Reads bytes with {@link FileChannel#read(ByteBuffer, long)}
   */
  protected static class NIOFSIndexInput extends FSIndexInput {

    private ByteBuffer byteBuf; // wraps the buffer for NIO

    final FileChannel channel;

    public NIOFSIndexInput(File path, IOContext context, int chunkSize) throws IOException {
      super("NIOFSIndexInput(path=\"" + path + "\")", path, context, chunkSize);
      channel = file.getChannel();
    }
    
    public NIOFSIndexInput(String sliceDescription, File path, RandomAccessFile file, FileChannel fc, long off, long length, int bufferSize, int chunkSize) {
      super("NIOFSIndexInput(" + sliceDescription + " in path=\"" + path + "\" slice=" + off + ":" + (off+length) + ")", file, off, length, bufferSize, chunkSize);
      channel = fc;
      isClone = true;
    }

    @Override
    protected void newBuffer(byte[] newBuffer) {
      super.newBuffer(newBuffer);
      byteBuf = ByteBuffer.wrap(newBuffer);
    }

    @Override
    protected void readInternal(byte[] b, int offset, int len) throws IOException {

      final ByteBuffer bb;

      // Determine the ByteBuffer we should use
      if (b == buffer && 0 == offset) {
        // Use our own pre-wrapped byteBuf:
        assert byteBuf != null;
        byteBuf.clear();
        byteBuf.limit(len);
        bb = byteBuf;
      } else {
        bb = ByteBuffer.wrap(b, offset, len);
      }

      int readOffset = bb.position();
      int readLength = bb.limit() - readOffset;
      assert readLength == len;

      long pos = getFilePointer() + off;
      
      if (pos + len > end) {
        throw new EOFException("read past EOF: " + this);
      }

      try {
        while (readLength > 0) {
          final int limit;
          if (readLength > chunkSize) {
            // LUCENE-1566 - work around JVM Bug by breaking
            // very large reads into chunks
            limit = readOffset + chunkSize;
          } else {
            limit = readOffset + readLength;
          }
          bb.limit(limit);
          int i = channel.read(bb, pos);
          pos += i;
          readOffset += i;
          readLength -= i;
        }
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

    @Override
    protected void seekInternal(long pos) throws IOException {}
  }

}
