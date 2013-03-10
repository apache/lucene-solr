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
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.lucene.util.WeakIdentityMap;

/**
 * Base IndexInput implementation that uses an array
 * of ByteBuffers to represent a file.
 * <p>
 * Because Java's ByteBuffer uses an int to address the
 * values, it's necessary to access a file greater
 * Integer.MAX_VALUE in size using multiple byte buffers.
 * <p>
 * For efficiency, this class requires that the buffers
 * are a power-of-two (<code>chunkSizePower</code>).
 */
abstract class ByteBufferIndexInput extends IndexInput {
  private ByteBuffer[] buffers;
  
  private final long chunkSizeMask;
  private final int chunkSizePower;
  
  private int offset;
  private long length;
  private String sliceDescription;

  private int curBufIndex;

  private ByteBuffer curBuf; // redundant for speed: buffers[curBufIndex]

  private boolean isClone = false;
  private final WeakIdentityMap<ByteBufferIndexInput,Boolean> clones;
  
  ByteBufferIndexInput(String resourceDescription, ByteBuffer[] buffers, long length, int chunkSizePower, boolean trackClones) throws IOException {
    super(resourceDescription);
    this.buffers = buffers;
    this.length = length;
    this.chunkSizePower = chunkSizePower;
    this.chunkSizeMask = (1L << chunkSizePower) - 1L;
    this.clones = trackClones ? WeakIdentityMap.<ByteBufferIndexInput,Boolean>newConcurrentHashMap() : null;
    
    assert chunkSizePower >= 0 && chunkSizePower <= 30;   
    assert (length >>> chunkSizePower) < Integer.MAX_VALUE;

    seek(0L);
  }
  
  @Override
  public final byte readByte() throws IOException {
    try {
      return curBuf.get();
    } catch (BufferUnderflowException e) {
      do {
        curBufIndex++;
        if (curBufIndex >= buffers.length) {
          throw new EOFException("read past EOF: " + this);
        }
        curBuf = buffers[curBufIndex];
        curBuf.position(0);
      } while (!curBuf.hasRemaining());
      return curBuf.get();
    } catch (NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException {
    try {
      curBuf.get(b, offset, len);
    } catch (BufferUnderflowException e) {
      int curAvail = curBuf.remaining();
      while (len > curAvail) {
        curBuf.get(b, offset, curAvail);
        len -= curAvail;
        offset += curAvail;
        curBufIndex++;
        if (curBufIndex >= buffers.length) {
          throw new EOFException("read past EOF: " + this);
        }
        curBuf = buffers[curBufIndex];
        curBuf.position(0);
        curAvail = curBuf.remaining();
      }
      curBuf.get(b, offset, len);
    } catch (NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final short readShort() throws IOException {
    try {
      return curBuf.getShort();
    } catch (BufferUnderflowException e) {
      return super.readShort();
    } catch (NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final int readInt() throws IOException {
    try {
      return curBuf.getInt();
    } catch (BufferUnderflowException e) {
      return super.readInt();
    } catch (NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final long readLong() throws IOException {
    try {
      return curBuf.getLong();
    } catch (BufferUnderflowException e) {
      return super.readLong();
    } catch (NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }
  
  @Override
  public final long getFilePointer() {
    try {
      return (((long) curBufIndex) << chunkSizePower) + curBuf.position() - offset;
    } catch (NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final void seek(long pos) throws IOException {
    // necessary in case offset != 0 and pos < 0, but pos >= -offset
    if (pos < 0L) {
      throw new IllegalArgumentException("Seeking to negative position: " + this);
    }
    pos += offset;
    // we use >> here to preserve negative, so we will catch AIOOBE,
    // in case pos + offset overflows.
    final int bi = (int) (pos >> chunkSizePower);
    try {
      final ByteBuffer b = buffers[bi];
      b.position((int) (pos & chunkSizeMask));
      // write values, on exception all is unchanged
      this.curBufIndex = bi;
      this.curBuf = b;
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      throw new EOFException("seek past EOF: " + this);
    } catch (IllegalArgumentException iae) {
      throw new EOFException("seek past EOF: " + this);
    } catch (NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final long length() {
    return length;
  }

  @Override
  public final ByteBufferIndexInput clone() {
    final ByteBufferIndexInput clone = buildSlice(0L, this.length);
    try {
      clone.seek(getFilePointer());
    } catch(IOException ioe) {
      throw new RuntimeException("Should never happen: " + this, ioe);
    }
    
    return clone;
  }
  
  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice is seeked to the beginning.
   */
  public final ByteBufferIndexInput slice(String sliceDescription, long offset, long length) {
    if (isClone) { // well we could, but this is stupid
      throw new IllegalStateException("cannot slice() " + sliceDescription + " from a cloned IndexInput: " + this);
    }
    final ByteBufferIndexInput clone = buildSlice(offset, length);
    clone.sliceDescription = sliceDescription;
    try {
      clone.seek(0L);
    } catch(IOException ioe) {
      throw new RuntimeException("Should never happen: " + this, ioe);
    }
    
    return clone;
  }
  
  private ByteBufferIndexInput buildSlice(long offset, long length) {
    if (buffers == null) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
    if (offset < 0 || length < 0 || offset+length > this.length) {
      throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset + ",length=" + length + ",fileLength="  + this.length + ": "  + this);
    }
    
    // include our own offset into the final offset:
    offset += this.offset;
    
    final ByteBufferIndexInput clone = (ByteBufferIndexInput)super.clone();
    clone.isClone = true;
    // we keep clone.clones, so it shares the same map with original and we have no additional cost on clones
    assert clone.clones == this.clones;
    clone.buffers = buildSlice(buffers, offset, length);
    clone.offset = (int) (offset & chunkSizeMask);
    clone.length = length;

    // register the new clone in our clone list to clean it up on closing:
    if (clones != null) {
      this.clones.put(clone, Boolean.TRUE);
    }
    
    return clone;
  }
  
  /** Returns a sliced view from a set of already-existing buffers: 
   *  the last buffer's limit() will be correct, but
   *  you must deal with offset separately (the first buffer will not be adjusted) */
  private ByteBuffer[] buildSlice(ByteBuffer[] buffers, long offset, long length) {
    final long sliceEnd = offset + length;
    
    final int startIndex = (int) (offset >>> chunkSizePower);
    final int endIndex = (int) (sliceEnd >>> chunkSizePower);

    // we always allocate one more slice, the last one may be a 0 byte one
    final ByteBuffer slices[] = new ByteBuffer[endIndex - startIndex + 1];
    
    for (int i = 0; i < slices.length; i++) {
      slices[i] = buffers[startIndex + i].duplicate();
    }

    // set the last buffer's limit for the sliced view.
    slices[slices.length - 1].limit((int) (sliceEnd & chunkSizeMask));
    
    return slices;
  }

  private void unsetBuffers() {
    buffers = null;
    curBuf = null;
    curBufIndex = 0;
  }

  @Override
  public final void close() throws IOException {
    try {
      if (buffers == null) return;
      
      // make local copy, then un-set early
      final ByteBuffer[] bufs = buffers;
      unsetBuffers();
      if (clones != null) {
        clones.remove(this);
      }
      
      if (isClone) return;
      
      // for extra safety unset also all clones' buffers:
      if (clones != null) {
        for (Iterator<ByteBufferIndexInput> it = this.clones.keyIterator(); it.hasNext();) {
          final ByteBufferIndexInput clone = it.next();
          assert clone.isClone;
          clone.unsetBuffers();
        }
        this.clones.clear();
      }
      
      for (final ByteBuffer b : bufs) {
        freeBuffer(b);
      }
    } finally {
      unsetBuffers();
    }
  }
  
  /**
   * Called when the contents of a buffer will be no longer needed.
   */
  protected abstract void freeBuffer(ByteBuffer b) throws IOException;

  @Override
  public final String toString() {
    if (sliceDescription != null) {
      return super.toString() + " [slice=" + sliceDescription + "]";
    } else {
      return super.toString();
    }
  }
}
