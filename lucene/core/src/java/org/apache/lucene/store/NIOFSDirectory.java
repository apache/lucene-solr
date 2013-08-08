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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException; // javadoc @link
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
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
    File path = new File(getDirectory(), name);
    FileChannel fc = FileChannel.open(path.toPath(), StandardOpenOption.READ);
    return new NIOFSIndexInput("NIOFSIndexInput(path=\"" + path + "\")", fc, context, getReadChunkSize());
  }
  
  @Override
  public IndexInputSlicer createSlicer(final String name,
      final IOContext context) throws IOException {
    ensureOpen();
    final File path = new File(getDirectory(), name);
    final FileChannel descriptor = FileChannel.open(path.toPath(), StandardOpenOption.READ);
    return new Directory.IndexInputSlicer() {

      @Override
      public void close() throws IOException {
        descriptor.close();
      }

      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) {
        return new NIOFSIndexInput("NIOFSIndexInput(" + sliceDescription + " in path=\"" + path + "\" slice=" + offset + ":" + (offset+length) + ")", descriptor, offset,
            length, BufferedIndexInput.bufferSize(context), getReadChunkSize());
      }
    };
  }

  /**
   * Reads bytes with {@link FileChannel#read(ByteBuffer, long)}
   */
  protected static class NIOFSIndexInput extends BufferedIndexInput {
    /** the file channel we will read from */
    protected final FileChannel channel;
    /** is this instance a clone and hence does not own the file to close it */
    boolean isClone = false;
    /** maximum read length on a 32bit JVM to prevent incorrect OOM, see LUCENE-1566 */ 
    protected final int chunkSize;
    /** start offset: non-zero in the slice case */
    protected final long off;
    /** end offset (start+length) */
    protected final long end;
    
    private ByteBuffer byteBuf; // wraps the buffer for NIO

    public NIOFSIndexInput(String resourceDesc, FileChannel fc, IOContext context, int chunkSize) throws IOException {
      super(resourceDesc, context);
      this.channel = fc; 
      this.chunkSize = chunkSize;
      this.off = 0L;
      this.end = fc.size();
    }
    
    public NIOFSIndexInput(String resourceDesc, FileChannel fc, long off, long length, int bufferSize, int chunkSize) {
      super(resourceDesc, bufferSize);
      this.channel = fc;
      this.chunkSize = chunkSize;
      this.off = off;
      this.end = off + length;
      this.isClone = true;
    }
    
    @Override
    public void close() throws IOException {
      if (!isClone) {
        channel.close();
      }
    }
    
    @Override
    public NIOFSIndexInput clone() {
      NIOFSIndexInput clone = (NIOFSIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
    
    @Override
    public final long length() {
      return end - off;
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
          if (i < 0){//be defensive here, even though we checked before hand, something could have changed
            throw new EOFException("read past EOF: " + this + " off: " + offset + " len: " + len + " pos: " + pos + " limit: " + limit + " end: " + end);
          }
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
