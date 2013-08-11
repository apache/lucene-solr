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
    return new NIOFSIndexInput(new File(getDirectory(), name), context);
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
            length, BufferedIndexInput.bufferSize(context));
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
    /**
     * The maximum chunk size for reads of 16384 bytes.
     */
    private static final int CHUNK_SIZE = 16384;
    
    private ByteBuffer byteBuf; // wraps the buffer for NIO

    final FileChannel channel;

    public NIOFSIndexInput(File path, IOContext context) throws IOException {
      super("NIOFSIndexInput(path=\"" + path + "\")", path, context);
      channel = file.getChannel();
    }
    
    public NIOFSIndexInput(String sliceDescription, File path, RandomAccessFile file, FileChannel fc, long off, long length, int bufferSize) {
      super("NIOFSIndexInput(" + sliceDescription + " in path=\"" + path + "\" slice=" + off + ":" + (off+length) + ")", file, off, length, bufferSize);
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
      if (b == buffer) {
        // Use our own pre-wrapped byteBuf:
        assert byteBuf != null;
        bb = byteBuf;
        byteBuf.clear().position(offset);
      } else {
        bb = ByteBuffer.wrap(b, offset, len);
      }

      long pos = getFilePointer() + off;
      
      if (pos + len > end) {
        throw new EOFException("read past EOF: " + this);
      }

      try {
        int readLength = len;
        while (readLength > 0) {
          final int toRead = Math.min(CHUNK_SIZE, readLength);
          bb.limit(bb.position() + toRead);
          assert bb.remaining() == toRead;
          final int i = channel.read(bb, pos);
          if (i < 0) { // be defensive here, even though we checked before hand, something could have changed
            throw new EOFException("read past EOF: " + this + " off: " + offset + " len: " + len + " pos: " + pos + " chunkLen: " + toRead + " end: " + end);
          }
          assert i > 0 : "FileChannel.read with non zero-length bb.remaining() must always read at least one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";
          pos += i;
          readLength -= i;
        }
        assert readLength == 0;
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + this, ioe);
      }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {}
  }

}
