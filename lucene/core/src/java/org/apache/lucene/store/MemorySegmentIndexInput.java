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
package org.apache.lucene.store;

import java.io.EOFException;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemorySegment;

/**
 * Base IndexInput implementation that uses an array of MemorySegments to represent a file.
 *
 * <p>For efficiency, this class requires that the buffers are a power-of-two
 * (<code>chunkSizePower</code>).
 */
public abstract class MemorySegmentIndexInput extends IndexInput implements RandomAccessInput {
  // We pass 1L as alignment, because currently Lucene file formats are heavy unaligned: :(
  static final VarHandle VH_getByte = MemoryHandles.varHandle(byte.class, 1L, ByteOrder.BIG_ENDIAN).withInvokeExactBehavior();
  static final VarHandle VH_getShort = MemoryHandles.varHandle(short.class, 1L, ByteOrder.BIG_ENDIAN).withInvokeExactBehavior();
  static final VarHandle VH_getInt = MemoryHandles.varHandle(int.class, 1L, ByteOrder.BIG_ENDIAN).withInvokeExactBehavior();
  static final VarHandle VH_getLong = MemoryHandles.varHandle(long.class, 1L, ByteOrder.BIG_ENDIAN).withInvokeExactBehavior();
  
  final long length;
  final long chunkSizeMask;
  final int chunkSizePower;

  MemorySegment[] buffers;
  int curBufIndex = -1;
  MemorySegment curBuf; // redundant for speed: buffers[curBufIndex]
  long curPosition;

  final boolean isClone;

  public static MemorySegmentIndexInput newInstance(
      String resourceDescription,
      MemorySegment[] segments,
      long length,
      int chunkSizePower) {
    if (segments.length == 1) {
      return new SingleBufferImpl(resourceDescription, segments[0], length, chunkSizePower, false);
    } else {
      return new MultiBufferImpl(resourceDescription, segments, 0, length, chunkSizePower, false);
    }
  }

  private MemorySegmentIndexInput(
      String resourceDescription,
      MemorySegment[] buffers,
      long length,
      int chunkSizePower,
      boolean isClone) {
    super(resourceDescription);
    this.buffers = buffers;
    this.length = length;
    this.chunkSizePower = chunkSizePower;
    this.chunkSizeMask = (1L << chunkSizePower) - 1L;
    this.isClone = isClone;
  }
  
  void ensureOpen() {
    if (this.buffers == null) {
      throw alreadyClosed();
    }
  }

  RuntimeException wrapAlreadyClosedException(RuntimeException e) {
    if (e instanceof NullPointerException) {
      return alreadyClosed();
    }
    // TODO: maybe open a JDK issue to have a separate, more
    // meaningful exception for unmapped segments:
    if (e.getMessage() != null && e.getMessage().contains("closed")) {
      return alreadyClosed();
    }
    return e;
  }
  
  private AlreadyClosedException alreadyClosed() {
    return new AlreadyClosedException("Already closed: " + this);
  }
  
  @Override
  public final byte readByte() throws IOException {
    try {
      final byte v = (byte) VH_getByte.get(curBuf, curPosition);
      curPosition++;
      return v;
    } catch (IndexOutOfBoundsException e) {
      do {
        curBufIndex++;
        if (curBufIndex >= buffers.length) {
          throw new EOFException("read past EOF: " + this);
        }
        curBuf = buffers[curBufIndex];
        curPosition = 0L;
      } while (curBuf.byteSize() == 0L);
      final byte v = (byte) VH_getByte.get(curBuf, curPosition);
      curPosition++;
      return v;
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @SuppressWarnings("resource")
  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException {
    try (final MemorySegment arraySegment = MemorySegment.ofArray(b)) {
      MemorySegment targetSlice = arraySegment.asSlice(offset, len);
      try {
        targetSlice.copyFrom(curBuf.asSlice(curPosition, len));
        curPosition += len;
      } catch (IndexOutOfBoundsException e) {
        long curAvail = curBuf.byteSize() - curPosition;
        while (len > curAvail) {
          targetSlice.copyFrom(curBuf.asSlice(curPosition, curAvail));
          len -= curAvail;
          offset += curAvail;
          curBufIndex++;
          if (curBufIndex >= buffers.length) {
            throw new EOFException("read past EOF: " + this);
          }
          curBuf = buffers[curBufIndex];
          curPosition = 0L;
          curAvail = curBuf.byteSize();
          targetSlice = arraySegment.asSlice(offset, len);
        }
        targetSlice.copyFrom(curBuf.asSlice(curPosition, len));
        curPosition += len;
      }
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public void readLELongs(long[] dst, int offset, int length) throws IOException {
    if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
      super.readLELongs(dst, offset, length);
      return;
    }
    try (final MemorySegment targetSlice =  MemorySegment.ofArray(dst)
        .asSlice((long) offset << 3, (long) length << 3)) {
      targetSlice.copyFrom(curBuf.asSlice(curPosition, targetSlice.byteSize()));
      curPosition += targetSlice.byteSize();
    } catch (IndexOutOfBoundsException iobe) {
      super.readLELongs(dst, offset, length);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public final short readShort() throws IOException {
    try {
      final short v = (short) VH_getShort.get(curBuf, curPosition);
      curPosition += Short.BYTES;
      return v;
    } catch (IndexOutOfBoundsException e) {
      return super.readShort();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public final int readInt() throws IOException {
    try {
      final int v = (int) VH_getInt.get(curBuf, curPosition);
      curPosition += Integer.BYTES;
      return v;
    } catch (IndexOutOfBoundsException e) {
      return super.readInt();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public final long readLong() throws IOException {
    try {
      final long v = (long) VH_getLong.get(curBuf, curPosition);
      curPosition += Long.BYTES;
      return v;
    } catch (IndexOutOfBoundsException e) {
      return super.readLong();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return (((long) curBufIndex) << chunkSizePower) + curPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    // we use >> here to preserve negative, so we will catch AIOOBE,
    // in case pos + offset overflows.
    final int bi = (int) (pos >> chunkSizePower);
    try {
      if (bi != curBufIndex) {
        final MemorySegment b = buffers[bi];
        // write values, on exception all is unchanged
        this.curBufIndex = bi;
        this.curBuf = b;
      }
      this.curPosition = Objects.checkIndex(pos & chunkSizeMask, curBuf.byteSize() + 1);
    } catch (IndexOutOfBoundsException e) {
      throw new EOFException("seek past EOF: " + this);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public byte readByte(long pos) throws IOException {
    try {
      final int bi = (int) (pos >> chunkSizePower);
      return (byte) VH_getByte.get(buffers[bi], pos & chunkSizeMask);
    } catch (IndexOutOfBoundsException ioobe) {
      throw new EOFException("seek past EOF: " + this);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  // used only by random access methods to handle reads across boundaries
  private void setPos(long pos, int bi) throws IOException {
    try {
      final MemorySegment b = buffers[bi];
      // write values, on exception above all is unchanged
      this.curPosition = pos & chunkSizeMask;
      this.curBufIndex = bi;
      this.curBuf = b;
    } catch (IndexOutOfBoundsException ioobe) {
      throw new EOFException("seek past EOF: " + this);
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public short readShort(long pos) throws IOException {
    final int bi = (int) (pos >> chunkSizePower);
    try {
      return (short) VH_getShort.get(buffers[bi], pos & chunkSizeMask);
    } catch (IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, bi);
      return readShort();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public int readInt(long pos) throws IOException {
    final int bi = (int) (pos >> chunkSizePower);
    try {
      return (int) VH_getInt.get(buffers[bi], pos & chunkSizeMask);
    } catch (IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, bi);
      return readInt();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public long readLong(long pos) throws IOException {
    final int bi = (int) (pos >> chunkSizePower);
    try {
      return (long) VH_getLong.get(buffers[bi], pos & chunkSizeMask);
    } catch (IndexOutOfBoundsException ioobe) {
      // either it's a boundary, or read past EOF, fall back:
      setPos(pos, bi);
      return readLong();
    } catch (NullPointerException | IllegalStateException e) {
      throw wrapAlreadyClosedException(e);
    }
  }

  @Override
  public final long length() {
    return length;
  }

  @Override
  public final MemorySegmentIndexInput clone() {
    final MemorySegmentIndexInput clone = buildSlice((String) null, 0L, this.length);
    try {
      clone.seek(getFilePointer());
    } catch (IOException ioe) {
      throw new AssertionError(ioe);
    }

    return clone;
  }

  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice
   * is seeked to the beginning.
   */
  @Override
  public final MemorySegmentIndexInput slice(String sliceDescription, long offset, long length) {
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IllegalArgumentException(
          "slice() "
              + sliceDescription
              + " out of bounds: offset="
              + offset
              + ",length="
              + length
              + ",fileLength="
              + this.length
              + ": "
              + this);
    }

    return buildSlice(sliceDescription, offset, length);
  }

  /** Builds the actual sliced IndexInput (may apply extra offset in subclasses). * */
  MemorySegmentIndexInput buildSlice(String sliceDescription, long offset, long length) {
    ensureOpen();
    
    final MemorySegment newBuffers[] = buildSlice(buffers, offset, length);
    offset = offset & chunkSizeMask;
    final String newResourceDescription = getFullSliceDescription(sliceDescription);
    
    if (newBuffers.length == 1) {
      return new SingleBufferImpl(
          newResourceDescription, newBuffers[0].asSlice(offset, length), length, chunkSizePower, true);
    } else {
      return new MultiBufferImpl(
          newResourceDescription, newBuffers, offset, length, chunkSizePower, true);
    }
  }

  /**
   * Returns a sliced view from a set of already-existing buffers: the last segments size will be
   * correct, but you must deal with offset separately (the first segment will not be adjusted)
   */
  private MemorySegment[] buildSlice(MemorySegment[] buffers, long offset, long length) {
    final long sliceEnd = offset + length;

    final int startIndex = (int) (offset >>> chunkSizePower);
    final int endIndex = (int) (sliceEnd >>> chunkSizePower);

    // we always allocate one more slice, the last one may be a 0 byte one after truncating with asSlice():
    final MemorySegment slices[] = Arrays.copyOfRange(buffers, startIndex, endIndex + 1);

    // set the last buffer's limit for the sliced view.
    slices[slices.length - 1] = slices[slices.length - 1].asSlice(0L, sliceEnd & chunkSizeMask);

    return slices;
  }

  @Override
  public final void close() throws IOException {
    try {
      if (buffers == null) return;

      // make local copy, then un-set early
      final MemorySegment[] bufs = buffers;
      unsetBuffers();

      if (isClone) return;

      // nocommit: use IOUtils#close() here to be safe that we try to close all segments, although some might fail!
      for (MemorySegment b : bufs) {
        b.close();
      }
    } finally {
      unsetBuffers();
    }
  }

  /** Called to remove all references to byte buffers, so we can throw AlreadyClosed on NPE. */
  private void unsetBuffers() {
    buffers = null;
    curBuf = null;
    curBufIndex = 0;
  }

  /** Optimization of MemorySegmentIndexInput for when there is only one buffer */
  static final class SingleBufferImpl extends MemorySegmentIndexInput {

    SingleBufferImpl(
        String resourceDescription,
        MemorySegment buffer,
        long length,
        int chunkSizePower,
        boolean isClone) {
      super(resourceDescription, new MemorySegment[] {buffer}, length, chunkSizePower, isClone);
      this.curBufIndex = 0;
      this.curBuf = buffer;
    }

    @Override
    public void seek(long pos) throws IOException {
      ensureOpen();
      try {
        curPosition = Objects.checkIndex(pos, length + 1);
      } catch (IndexOutOfBoundsException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      }
    }

    @Override
    public long getFilePointer() {
      ensureOpen();
      return curPosition;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      try {
        return (byte) VH_getByte.get(curBuf, pos);
      } catch (IndexOutOfBoundsException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }

    @Override
    public short readShort(long pos) throws IOException {
      try {
        return (short) VH_getShort.get(curBuf, pos);
      } catch (IndexOutOfBoundsException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }

    @Override
    public int readInt(long pos) throws IOException {
      try {
        return (int) VH_getInt.get(curBuf, pos);
      } catch (IndexOutOfBoundsException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }

    @Override
    public long readLong(long pos) throws IOException {
      try {
        return (long) VH_getLong.get(curBuf, pos);
      } catch (IndexOutOfBoundsException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (NullPointerException | IllegalStateException e) {
        throw wrapAlreadyClosedException(e);
      }
    }
  }

  /** This class adds offset support to MemorySegmentIndexInput, which is needed for slices. */
  static final class MultiBufferImpl extends MemorySegmentIndexInput {
    private final long offset;

    MultiBufferImpl(
        String resourceDescription,
        MemorySegment[] buffers,
        long offset,
        long length,
        int chunkSizePower,
        boolean isClone) {
      super(resourceDescription, buffers, length, chunkSizePower, isClone);
      this.offset = offset;
      try {
        seek(0L);
      } catch (IOException ioe) {
        throw new AssertionError(ioe);
      }
    }

    @Override
    public void seek(long pos) throws IOException {
      assert pos >= 0L;
      super.seek(pos + offset);
    }

    @Override
    public long getFilePointer() {
      return super.getFilePointer() - offset;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      return super.readByte(pos + offset);
    }

    @Override
    public short readShort(long pos) throws IOException {
      return super.readShort(pos + offset);
    }

    @Override
    public int readInt(long pos) throws IOException {
      return super.readInt(pos + offset);
    }

    @Override
    public long readLong(long pos) throws IOException {
      return super.readLong(pos + offset);
    }

    @Override
    MemorySegmentIndexInput buildSlice(String sliceDescription, long ofs, long length) {
      return super.buildSlice(sliceDescription, this.offset + ofs, length);
    }
  }
}
