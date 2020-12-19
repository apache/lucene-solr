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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.lucene.store.*;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.util.IOUtils;

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
 * <p>@throws UnsupportedOperationException if the operating system, file system or JDK
 * does not support Direct I/O or a sufficient equivalent.
 *
 * @lucene.experimental
 */
public class DirectIODirectory extends FilterDirectory {

  /** Default buffer size before writing to disk (256 KB);
   *  larger means less IO load but more RAM and direct
   *  buffer storage space consumed during merging. */

  public final static int DEFAULT_MERGE_BUFFER_SIZE = 262144;

  /** Default min expected merge size before direct IO is
   *  used (10 MB): */
  public final static long DEFAULT_MIN_BYTES_DIRECT = 10*1024*1024;

  private final int blockSize, mergeBufferSize;
  private final long minBytesDirect;
  private final Directory delegate;
  private final Path path;
  
  /** Reference to {@code com.sun.nio.file.ExtendedOpenOption.DIRECT} by reflective class and enum lookup.
   * There are two reasons for using this instead of directly referencing ExtendedOpenOption.DIRECT:
   * <ol>
   * <li> ExtendedOpenOption.DIRECT is OpenJDK's internal proprietary API. This API causes un-suppressible(?) warning to be emitted
   *  when compiling with --release flag and value N, where N is smaller than the the version of javac used for compilation.</li>
   * <li> It is possible that Lucene is run using JDK that does not support ExtendedOpenOption.DIRECT. In such a
   *  case, dynamic lookup allows us to bail out with UnsupportedOperationException with meaningful error message.</li>
   * </ol>
   * <p>This reference is {@code null}, if the JDK does not support direct I/O.
   */
  static final OpenOption ExtendedOpenOption_DIRECT; // visible for test
  static {
    OpenOption option;
    try {
      final Class<? extends OpenOption> clazz = Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
      option = Arrays.stream(clazz.getEnumConstants())
                      .filter(e -> e.toString().equalsIgnoreCase("DIRECT"))
                      .findFirst()
                      .orElse(null);
    } catch (Exception e) {
      option = null;
    }
    ExtendedOpenOption_DIRECT = option;
  }

  /** Create a new DirectIODirectory for the named location.
   * 
   * @param path the path of the directory
   * @param mergeBufferSize Size of buffer to use for
   *    merging.
   * @param minBytesDirect Merges, or files to be opened for
   *   reading, smaller than this will
   *   not use direct IO.  See {@link
   *   #DEFAULT_MIN_BYTES_DIRECT}
   * @param delegate fallback Directory for non-merges
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(Path path, int mergeBufferSize, long minBytesDirect, Directory delegate) throws IOException {
    super(delegate);
    this.blockSize = Math.toIntExact(Files.getFileStore(path).getBlockSize());
    this.mergeBufferSize = mergeBufferSize;
    this.minBytesDirect = minBytesDirect;
    this.delegate = delegate;
    this.path = path.toRealPath();
  }

  /** Create a new DirectIODirectory for the named location.
   * 
   * @param path the path of the directory
   * @param delegate fallback Directory for non-merges
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(Path path, Directory delegate) throws IOException {
    this(path, DEFAULT_MERGE_BUFFER_SIZE, DEFAULT_MIN_BYTES_DIRECT, delegate);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (context.context != Context.MERGE || context.mergeInfo.estimatedMergeBytes < minBytesDirect || fileLength(name) < minBytesDirect) {
      return delegate.openInput(name, context);
    } else {
      return new DirectIOIndexInput(path.resolve(name), blockSize, mergeBufferSize);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (context.context != Context.MERGE || context.mergeInfo.estimatedMergeBytes < minBytesDirect) {
      return delegate.createOutput(name, context);
    } else {
      return new DirectIOIndexOutput(path.resolve(name), name, blockSize, mergeBufferSize);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(delegate);
    super.close();
  }
  
  private static OpenOption getDirectOpenOption() {
    if (ExtendedOpenOption_DIRECT == null) {
      throw new UnsupportedOperationException("com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version.");
    }
    return ExtendedOpenOption_DIRECT;
  }

  private final static class DirectIOIndexOutput extends IndexOutput {
    private final ByteBuffer buffer;
    private final FileChannel channel;

    private long filePos;
    private boolean isOpen;

    /**
     * Creates a new instance of DirectIOIndexOutput for writing index output with direct IO bypassing OS buffer
     * @throws UnsupportedOperationException if the operating system, file system or JDK
     * does not support Direct I/O or a sufficient equivalent.
     */
    public DirectIOIndexOutput(Path path, String name, int blockSize, int bufferSize) throws IOException {
      super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

      channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE, getDirectOpenOption());
      buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);

      isOpen = true;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      buffer.put(b);
      if (!buffer.hasRemaining()) {
        dump();
      }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      int toWrite = len;
      while(true) {
        final int left = buffer.remaining();
        if (left <= toWrite) {
          buffer.put(src, offset, left);
          toWrite -= left;
          offset += left;
          dump();
        } else {
          buffer.put(src, offset, toWrite);
          break;
        }
      }
    }

    private void dump() throws IOException {
      buffer.flip();

      //System.out.println(Thread.currentThread().getName() + ": dump to " + filePos + " limit=" + buffer.limit() + " fos=" + fos);
      channel.write(buffer, filePos);
      filePos += buffer.position();

      buffer.clear();
      //System.out.println("dump: done");
    }

    @Override
    public long getFilePointer() {
      return filePos + buffer.position();
    }

    @Override
    public long getChecksum() {
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
            channel.truncate(getFilePointer());
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

    private boolean isOpen;
    private boolean isClone;
    private long filePos;

    /**
     * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing OS buffer
     * @throws UnsupportedOperationException if the operating system, file system or JDK
     * does not support Direct I/O or a sufficient equivalent.
     */
    public DirectIOIndexInput(Path path, int blockSize, int bufferSize) throws IOException {
      super("DirectIOIndexInput(path=\"" + path + "\")");
      this.blockSize = blockSize;

      this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
      this.buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);

      isOpen = true;
      isClone = false;
      filePos = -bufferSize;
      buffer.position(bufferSize);
      //System.out.println("D open " + path + " this=" + this);
    }

    // for clone
    private DirectIOIndexInput(DirectIOIndexInput other) throws IOException {
      super(other.toString());
      this.channel = other.channel;
      this.blockSize = other.blockSize;
      
      final int bufferSize = other.buffer.capacity();
      this.buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);
      
      filePos = -bufferSize;
      buffer.position(bufferSize);
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
      return filePos + buffer.position();
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos != getFilePointer()) {
        final long alignedPos = pos - (pos % blockSize);
        filePos = alignedPos-buffer.capacity();
        
        final int delta = (int) (pos - alignedPos);
        if (delta != 0) {
          refill();
          buffer.position(delta);
        } else {
          // force refill on next read
          buffer.position(buffer.limit());
        }
      }
    }

    @Override
    public long length() {
      try {
        return channel.size();
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }

    @Override
    public byte readByte() throws IOException {
      // NOTE: we don't guard against EOF here... ie the
      // "final" buffer will typically be filled to less
      // than bufferSize
      if (buffer.hasRemaining()) {
        refill();
      }
      return buffer.get();
    }

    private void refill() throws IOException {
      filePos += buffer.capacity();
      //System.out.println("X refill filePos=" + filePos);
      
      buffer.clear();
      int n;
      try {
        n = channel.read(buffer, filePos);
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + this, ioe);
      }
      if (n < 0) {
        throw new EOFException("read past EOF: " + this);
      }
      buffer.flip();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      int toRead = len;
      //System.out.println("\nX readBytes len=" + len + " fp=" + getFilePointer() + " size=" + length() + " this=" + this);
      while(true) {
        final int left = buffer.remaining();
        if (left < toRead) {
          //System.out.println("  copy " + left);
          buffer.get(dst, offset, left);
          toRead -= left;
          offset += left;
          refill();
        } else {
          //System.out.println("  copy " + toRead);
          buffer.get(dst, offset, toRead);
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
        throw new UncheckedIOException(ioe);
      }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
      // TODO: is this the right thing to do?
      return BufferedIndexInput.wrap(sliceDescription, this, offset, length);
    }
  }
}
