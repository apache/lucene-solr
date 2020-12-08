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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SuppressForbidden;

// TODO
//   - newer Linux kernel versions (after 2.6.29) have
//     improved MADV_SEQUENTIAL (and hopefully also
//     FADV_SEQUENTIAL) interaction with the buffer
//     cache; we should explore using that instead of direct
//     IO when context is merge

/**
 * A {@link Directory} implementation for all Unixes and Windows that uses
 * DIRECT I/O to bypass OS level IO caching during
 * merging.  For all other cases (searching, writing) we delegate
 * to the provided Directory instance.
 *
 * <p>See <a
 * href="{@docRoot}/overview-summary.html#DirectIODirectory">Overview</a>
 * for more details.
 *
 * <p><b>WARNING</b>: this code is very new and quite easily
 * could contain horrible bugs.
 *
 * <p>This directory passes Solr and Lucene tests on Linux, OS X,
 * and Windows; other systems should work but have not been
 * tested! Use at your own risk.
 *
 * <p>@throws UnsupportedOperationException if the operating system or
 * file system does not support Direct I/O or a sufficient equivalent.
 *
 * @lucene.experimental
 */
public class DirectIODirectory extends FSDirectory {

  // TODO: this is OS dependent, but likely 512 is the LCD
  private final static long ALIGN = 512;
  private final static long ALIGN_NOT_MASK = ~(ALIGN-1);

  /** Default buffer size before writing to disk (256 KB);
   *  larger means less IO load but more RAM and direct
   *  buffer storage space consumed during merging. */

  public final static int DEFAULT_MERGE_BUFFER_SIZE = 262144;

  /** Default min expected merge size before direct IO is
   *  used (10 MB): */
  public final static long DEFAULT_MIN_BYTES_DIRECT = 10*1024*1024;

  private final int mergeBufferSize;
  private final long minBytesDirect;
  private final Directory delegate;

  /** Create a new NIOFSDirectory for the named location.
   * 
   * @param path the path of the directory
   * @param lockFactory to use
   * @param mergeBufferSize Size of buffer to use for
   *    merging.  See {@link #DEFAULT_MERGE_BUFFER_SIZE}.
   * @param minBytesDirect Merges, or files to be opened for
   *   reading, smaller than this will
   *   not use direct IO.  See {@link
   *   #DEFAULT_MIN_BYTES_DIRECT}
   * @param delegate fallback Directory for non-merges
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(Path path, int mergeBufferSize, long minBytesDirect, LockFactory lockFactory, Directory delegate) throws IOException {
    super(path, lockFactory);
    if ((mergeBufferSize & ALIGN) != 0) {
      throw new IllegalArgumentException("mergeBufferSize must be 0 mod " + ALIGN + " (got: " + mergeBufferSize + ")");
    }
    this.mergeBufferSize = mergeBufferSize;
    this.minBytesDirect = minBytesDirect;
    this.delegate = delegate;
  }
  
  /** Create a new NIOFSDirectory for the named location.
   * 
   * @param path the path of the directory
   * @param lockFactory the lock factory to use
   * @param delegate fallback Directory for non-merges
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(Path path, LockFactory lockFactory, Directory delegate) throws IOException {
    this(path, DEFAULT_MERGE_BUFFER_SIZE, DEFAULT_MIN_BYTES_DIRECT, lockFactory, delegate);
  }  

  /** Create a new NIOFSDirectory for the named location with {@link FSLockFactory#getDefault()}.
   * 
   * @param path the path of the directory
   * @param delegate fallback Directory for non-merges
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(Path path, Directory delegate) throws IOException {
    this(path, DEFAULT_MERGE_BUFFER_SIZE, DEFAULT_MIN_BYTES_DIRECT, FSLockFactory.getDefault(), delegate);
  }  

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    ensureCanRead(name);
    if (context.context != Context.MERGE || context.mergeInfo.estimatedMergeBytes < minBytesDirect || fileLength(name) < minBytesDirect) {
      return delegate.openInput(name, context);
    } else {
      return new DirectIOIndexInput(getDirectory().resolve(name), mergeBufferSize);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (context.context != Context.MERGE || context.mergeInfo.estimatedMergeBytes < minBytesDirect) {
      return delegate.createOutput(name, context);
    } else {
      return new DirectIOIndexOutput(getDirectory().resolve(name), name, mergeBufferSize);
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    delegate.deleteFile(name);
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    return delegate.getPendingDeletions();
  }

  @Override
  public String[] listAll() throws IOException {
    return delegate.listAll();
  }

  @Override
  public long fileLength(String name) throws IOException {
    return delegate.fileLength(name);
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    delegate.rename(source, dest);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(delegate);
    super.close();
  }

  private final static class DirectIOIndexOutput extends IndexOutput {
    private final ByteBuffer buffer;
    private final FileChannel channel;
    private final int bufferSize;

    //private final File path;

    private int bufferPos;
    private long filePos;
    private long fileLength;
    private boolean isOpen;

    /**
     * Creates a new instance of DirectIOIndexOutput for writing index output with direct IO bypassing OS buffer
     * @throws UnsupportedOperationException if the operating system or
     * file system does not support Direct I/O or a sufficient equivalent.
     */
    @SuppressForbidden(reason = "com.sun.nio.file.ExtendedOpenOption: Direct I/O with FileChannel requires the use of internal proprietary API ExtendedOpenOption.DIRECT")
    public DirectIOIndexOutput(Path path, String name, int bufferSize) throws IOException {
      super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

      channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                                  com.sun.nio.file.ExtendedOpenOption.DIRECT);
      int blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
      this.bufferSize = bufferSize;
      buffer = ByteBuffer.allocateDirect(bufferSize).alignedSlice(blockSize);

      isOpen = true;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      assert bufferPos == buffer.position(): "bufferPos=" + bufferPos + " vs buffer.position()=" + buffer.position();
      buffer.put(b);
      if (++bufferPos == bufferSize) {
        dump();
      }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      int toWrite = len;
      while(true) {
        final int left = bufferSize - bufferPos;
        if (left <= toWrite) {
          buffer.put(src, offset, left);
          toWrite -= left;
          offset += left;
          bufferPos = bufferSize;
          dump();
        } else {
          buffer.put(src, offset, toWrite);
          bufferPos += toWrite;
          break;
        }
      }
    }

    //@Override
    //public void setLength() throws IOException {
    //   TODO -- how to impl this?  neither FOS nor
    //   FileChannel provides an API?
    //}

    private void dump() throws IOException {
      buffer.flip();
      final long limit = filePos + buffer.limit();
      if (limit > fileLength) {
        // this dump extends the file
        fileLength = limit;
      } else {
        // we had seek'd back & wrote some changes
      }

      // must always round to next block
      buffer.limit((int) ((buffer.limit() + ALIGN - 1) & ALIGN_NOT_MASK));

      assert (buffer.limit() & ALIGN_NOT_MASK) == buffer.limit() : "limit=" + buffer.limit() + " vs " + (buffer.limit() & ALIGN_NOT_MASK);
      assert (filePos & ALIGN_NOT_MASK) == filePos;
      //System.out.println(Thread.currentThread().getName() + ": dump to " + filePos + " limit=" + buffer.limit() + " fos=" + fos);
      channel.write(buffer, filePos);
      filePos += bufferPos;
      bufferPos = 0;
      buffer.clear();
      //System.out.println("dump: done");

      // TODO: the case where we'd seek'd back, wrote an
      // entire buffer, we must here read the next buffer;
      // likely Lucene won't trip on this since we only
      // write smallish amounts on seeking back
    }

    @Override
    public long getFilePointer() {
      return filePos + bufferPos;
    }

    @Override
    public long getChecksum() throws IOException {
      throw new UnsupportedOperationException("this directory currently does not work at all!");
    }

    @Override
    public void close() throws IOException {
      if (isOpen) {
        isOpen = false;
        try {
          dump();
        } finally {
          try {
            //System.out.println("direct close set len=" + fileLength + " vs " + channel.size() + " path=" + path);
            channel.truncate(fileLength);
            //System.out.println("  now: " + channel.size());
          } finally {
            channel.close();
          }
        }
      }
    }
  }

  private final static class DirectIOIndexInput extends IndexInput {
    private final ByteBuffer buffer;
    private final FileChannel channel;
    private final int blockSize;
    private final int bufferSize;

    private boolean isOpen;
    private boolean isClone;
    private long filePos;
    private int bufferPos;

    /**
     * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing OS buffer
     * @throws UnsupportedOperationException if the operating system or
     * file system does not support Direct I/O or a sufficient equivalent.
     */
    @SuppressForbidden(reason = "com.sun.nio.file.ExtendedOpenOption: Direct I/O with FileChannel requires the use of internal proprietary API ExtendedOpenOption.DIRECT")
    public DirectIOIndexInput(Path path, int bufferSize) throws IOException {
      super("DirectIOIndexInput(path=\"" + path + "\")");

      channel = FileChannel.open(path, StandardOpenOption.READ, com.sun.nio.file.ExtendedOpenOption.DIRECT);
      blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
      this.bufferSize = bufferSize;
      buffer = ByteBuffer.allocateDirect(bufferSize).alignedSlice(blockSize);

      isOpen = true;
      isClone = false;
      filePos = -bufferSize;
      bufferPos = bufferSize;
      //System.out.println("D open " + path + " this=" + this);
    }

    // for clone
    public DirectIOIndexInput(DirectIOIndexInput other) throws IOException {
      super(other.toString());
      channel = other.channel;
      this.bufferSize = other.bufferSize;
      this.blockSize = other.blockSize;
      buffer = ByteBuffer.allocateDirect(bufferSize).alignedSlice(blockSize);
      filePos = -bufferSize;
      bufferPos = bufferSize;
      isOpen = true;
      isClone = true;
      //System.out.println("D clone this=" + this);
      seek(other.getFilePointer());
    }

    @Override
    public void close() throws IOException {
      if (isOpen && !isClone) {
        channel.close();
      }
    }

    @Override
    public long getFilePointer() {
      return filePos + bufferPos;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos != getFilePointer()) {
        final long alignedPos = pos & ALIGN_NOT_MASK;
        filePos = alignedPos-bufferSize;
        
        final int delta = (int) (pos - alignedPos);
        if (delta != 0) {
          refill();
          buffer.position(delta);
          bufferPos = delta;
        } else {
          // force refill on next read
          bufferPos = bufferSize;
        }
      }
    }

    @Override
    public long length() {
      try {
        return channel.size();
      } catch (IOException ioe) {
        throw new RuntimeException("IOException during length(): " + this, ioe);
      }
    }

    @Override
    public byte readByte() throws IOException {
      // NOTE: we don't guard against EOF here... ie the
      // "final" buffer will typically be filled to less
      // than bufferSize
      if (bufferPos == bufferSize) {
        refill();
      }
      assert bufferPos == buffer.position() : "bufferPos=" + bufferPos + " vs buffer.position()=" + buffer.position();
      bufferPos++;
      return buffer.get();
    }

    private void refill() throws IOException {
      buffer.clear();
      filePos += bufferSize;
      bufferPos = 0;
      assert (filePos & ALIGN_NOT_MASK) == filePos : "filePos=" + filePos + " anded=" + (filePos & ALIGN_NOT_MASK);
      //System.out.println("X refill filePos=" + filePos);
      int n;
      try {
        n = channel.read(buffer, filePos);
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + this, ioe);
      }
      if (n < 0) {
        throw new EOFException("read past EOF: " + this);
      }
      buffer.rewind();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      int toRead = len;
      //System.out.println("\nX readBytes len=" + len + " fp=" + getFilePointer() + " size=" + length() + " this=" + this);
      while(true) {
        final int left = bufferSize - bufferPos;
        if (left < toRead) {
          //System.out.println("  copy " + left);
          buffer.get(dst, offset, left);
          toRead -= left;
          offset += left;
          refill();
        } else {
          //System.out.println("  copy " + toRead);
          buffer.get(dst, offset, toRead);
          bufferPos += toRead;
          //System.out.println("  readBytes done");
          break;
        }
      }
    }

    @Override
    public DirectIOIndexInput clone() {
      try {
        return new DirectIOIndexInput(this);
      } catch (IOException ioe) {
        throw new RuntimeException("IOException during clone: " + this, ioe);
      }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      // TODO: is this the right thing to do?
      return BufferedIndexInput.wrap(sliceDescription, this, offset, length);
    }
  }
}
