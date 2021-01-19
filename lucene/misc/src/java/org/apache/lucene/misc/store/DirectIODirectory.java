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
import java.util.OptionalLong;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.lucene.store.*;
import org.apache.lucene.store.IOContext.Context;

/**
 * A {@link Directory} implementation for all Unixes and Windows that uses DIRECT I/O to bypass OS
 * level IO caching during merging. For all other cases (searching, writing) we delegate to the
 * provided Directory instance.
 *
 * <p>See <a href="{@docRoot}/overview-summary.html#DirectIODirectory">Overview</a> for more
 * details.
 *
 * <p><b>WARNING</b>: this code is very new and quite easily could contain horrible bugs.
 *
 * <p>This directory passes Solr and Lucene tests on Linux, OS X, and Windows; other systems should
 * work but have not been tested! Use at your own risk.
 *
 * <p>@throws UnsupportedOperationException if the operating system, file system or JDK does not
 * support Direct I/O or a sufficient equivalent.
 *
 * @lucene.experimental
 */
public class DirectIODirectory extends FilterDirectory {

  /**
   * Default buffer size before writing to disk (256 KB); larger means less IO load but more RAM and
   * direct buffer storage space consumed during merging.
   */
  public static final int DEFAULT_MERGE_BUFFER_SIZE = 256 * 1024;

  /** Default min expected merge size before direct IO is used (10 MB): */
  public static final long DEFAULT_MIN_BYTES_DIRECT = 10 * 1024 * 1024;

  private final int blockSize, mergeBufferSize;
  private final long minBytesDirect;

  volatile boolean isOpen = true;

  /**
   * Reference to {@code com.sun.nio.file.ExtendedOpenOption.DIRECT} by reflective class and enum
   * lookup. There are two reasons for using this instead of directly referencing
   * ExtendedOpenOption.DIRECT:
   *
   * <ol>
   *   <li>ExtendedOpenOption.DIRECT is OpenJDK's internal proprietary API. This API causes
   *       un-suppressible(?) warning to be emitted when compiling with --release flag and value N,
   *       where N is smaller than the the version of javac used for compilation. For details,
   *       please refer to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8259039.
   *   <li>It is possible that Lucene is run using JDK that does not support
   *       ExtendedOpenOption.DIRECT. In such a case, dynamic lookup allows us to bail out with
   *       UnsupportedOperationException with meaningful error message.
   * </ol>
   *
   * <p>This reference is {@code null}, if the JDK does not support direct I/O.
   */
  static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

  static {
    OpenOption option;
    try {
      final Class<? extends OpenOption> clazz =
          Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
      option =
          Arrays.stream(clazz.getEnumConstants())
              .filter(e -> e.toString().equalsIgnoreCase("DIRECT"))
              .findFirst()
              .orElse(null);
    } catch (Exception e) {
      option = null;
    }
    ExtendedOpenOption_DIRECT = option;
  }

  /**
   * Create a new DirectIODirectory for the named location.
   *
   * @param delegate Directory for non-merges, also used as reference to file system path.
   * @param mergeBufferSize Size of buffer to use for merging.
   * @param minBytesDirect Merges, or files to be opened for reading, smaller than this will not use
   *     direct IO. See {@link #DEFAULT_MIN_BYTES_DIRECT} and {@link #useDirectIO}.
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(FSDirectory delegate, int mergeBufferSize, long minBytesDirect)
      throws IOException {
    super(delegate);
    this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
    this.mergeBufferSize = mergeBufferSize;
    this.minBytesDirect = minBytesDirect;
  }

  /**
   * Create a new DirectIODirectory for the named location.
   *
   * @param delegate Directory for non-merges, also used as reference to file system path.
   * @throws IOException If there is a low-level I/O error
   */
  public DirectIODirectory(FSDirectory delegate) throws IOException {
    this(delegate, DEFAULT_MERGE_BUFFER_SIZE, DEFAULT_MIN_BYTES_DIRECT);
  }

  /** @return the underlying file system directory */
  public Path getDirectory() {
    return ((FSDirectory) in).getDirectory();
  }

  @Override
  protected void ensureOpen() throws AlreadyClosedException {
    if (!isOpen) {
      throw new AlreadyClosedException("this Directory is closed");
    }
  }

  /**
   * Determines if direct IO should be used for a file. By default this tests if it is a merge
   * context and if the merge or file length extends the minimum size (see {@link
   * #DEFAULT_MIN_BYTES_DIRECT}). Subclasses may override method to enforce direct IO for specific
   * file types.
   *
   * @param name file name (unused by default implementation)
   * @param context information about merge size
   * @param fileLength if available, gives the file length. Will be empty when requesting an {@link
   *     IndexOutput}.
   * @return {@code true} if direct IO should be used; {@code false} if input/output should be
   *     requested from delegate directory.
   */
  protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
    return context.context == Context.MERGE
        && context.mergeInfo.estimatedMergeBytes >= minBytesDirect
        && fileLength.orElse(minBytesDirect) >= minBytesDirect;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (useDirectIO(name, context, OptionalLong.of(fileLength(name)))) {
      return new DirectIOIndexInput(getDirectory().resolve(name), blockSize, mergeBufferSize);
    } else {
      return in.openInput(name, context);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (useDirectIO(name, context, OptionalLong.empty())) {
      return new DirectIOIndexOutput(
          getDirectory().resolve(name), name, blockSize, mergeBufferSize);
    } else {
      return in.createOutput(name, context);
    }
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    super.close();
  }

  private static OpenOption getDirectOpenOption() {
    if (ExtendedOpenOption_DIRECT == null) {
      throw new UnsupportedOperationException(
          "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version.");
    }
    return ExtendedOpenOption_DIRECT;
  }

  private static final class DirectIOIndexOutput extends IndexOutput {
    private final ByteBuffer buffer;
    private final FileChannel channel;
    private final Checksum digest;

    private long filePos;
    private boolean isOpen;

    /**
     * Creates a new instance of DirectIOIndexOutput for writing index output with direct IO
     * bypassing OS buffer
     *
     * @throws UnsupportedOperationException if the operating system, file system or JDK does not
     *     support Direct I/O or a sufficient equivalent.
     */
    public DirectIOIndexOutput(Path path, String name, int blockSize, int bufferSize)
        throws IOException {
      super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

      channel =
          FileChannel.open(
              path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, getDirectOpenOption());
      buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);
      digest = new BufferedChecksum(new CRC32());

      isOpen = true;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      buffer.put(b);
      digest.update(b);
      if (!buffer.hasRemaining()) {
        dump();
      }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      int toWrite = len;
      while (true) {
        final int left = buffer.remaining();
        if (left <= toWrite) {
          buffer.put(src, offset, left);
          digest.update(src, offset, left);
          toWrite -= left;
          offset += left;
          dump();
        } else {
          buffer.put(src, offset, toWrite);
          digest.update(src, offset, toWrite);
          break;
        }
      }
    }

    private void dump() throws IOException {
      final int size = buffer.position();

      // we need to rewind, as we have to write full blocks (we truncate file later):
      buffer.rewind();

      channel.write(buffer, filePos);
      filePos += size;

      buffer.clear();
    }

    @Override
    public long getFilePointer() {
      return filePos + buffer.position();
    }

    @Override
    public long getChecksum() {
      return digest.getValue();
    }

    @Override
    public void close() throws IOException {
      if (isOpen) {
        isOpen = false;
        try {
          dump();
        } finally {
          try (FileChannel ch = channel) {
            ch.truncate(getFilePointer());
          }
        }
      }
    }
  }

  private static final class DirectIOIndexInput extends IndexInput {
    private final ByteBuffer buffer;
    private final FileChannel channel;
    private final int blockSize;

    private boolean isOpen;
    private boolean isClone;
    private long filePos;

    /**
     * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing
     * OS buffer
     *
     * @throws UnsupportedOperationException if the operating system, file system or JDK does not
     *     support Direct I/O or a sufficient equivalent.
     */
    public DirectIOIndexInput(Path path, int blockSize, int bufferSize) throws IOException {
      super("DirectIOIndexInput(path=\"" + path + "\")");
      this.blockSize = blockSize;

      this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
      this.buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);

      isOpen = true;
      isClone = false;
      filePos = -bufferSize;
      buffer.limit(0);
    }

    // for clone
    private DirectIOIndexInput(DirectIOIndexInput other) throws IOException {
      super(other.toString());
      this.channel = other.channel;
      this.blockSize = other.blockSize;

      final int bufferSize = other.buffer.capacity();
      this.buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);

      isOpen = true;
      isClone = true;
      filePos = -bufferSize;
      buffer.limit(0);
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
      long filePointer = filePos + buffer.position();

      // opening the input and immediately calling getFilePointer without calling readX (and thus
      // refill) first,
      // will result in negative value equal to bufferSize being returned,
      // due to the initialization method filePos = -bufferSize used in constructor.
      assert filePointer == -buffer.capacity() || filePointer >= 0
          : "filePointer should either be initial value equal to negative buffer capacity, or larger than or equal to 0";
      return Math.max(filePointer, 0);
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos != getFilePointer()) {
        final long alignedPos = pos - (pos % blockSize);
        filePos = alignedPos - buffer.capacity();

        final int delta = (int) (pos - alignedPos);
        refill();
        try {
          buffer.position(delta);
        } catch (IllegalArgumentException iae) {
          throw new EOFException("read past EOF: " + this);
        }
      }
      assert pos == getFilePointer();
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
      if (!buffer.hasRemaining()) {
        refill();
      }
      return buffer.get();
    }

    private void refill() throws IOException {
      filePos += buffer.capacity();

      // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
      // hence throwing EOFException early to maintain buffer state (position in particular)
      if (filePos > channel.size()) {
        throw new EOFException("read past EOF: " + this);
      }

      buffer.clear();
      try {
        // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
        // EOF
        // when filePos > channel.size(), an EOFException will be thrown from above
        channel.read(buffer, filePos);
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + this, ioe);
      }

      buffer.flip();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      int toRead = len;
      while (true) {
        final int left = buffer.remaining();
        if (left < toRead) {
          buffer.get(dst, offset, left);
          toRead -= left;
          offset += left;
          refill();
        } else {
          buffer.get(dst, offset, toRead);
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
