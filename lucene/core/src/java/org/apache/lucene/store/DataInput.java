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


import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Abstract base class for performing read operations of Lucene's low-level
 * data types.
 *
 * <p>{@code DataInput} may only be used from one thread, because it is not
 * thread safe (it keeps internal state like file position). To allow
 * multithreaded use, every {@code DataInput} instance must be cloned before
 * used in another thread. Subclasses must therefore implement {@link #clone()},
 * returning a new {@code DataInput} which operates on the same underlying
 * resource, but positioned independently.
 */
public abstract class DataInput implements Cloneable {

  private static final int SKIP_BUFFER_SIZE = 1024;

  /* This buffer is used to skip over bytes with the default implementation of
   * skipBytes. The reason why we need to use an instance member instead of
   * sharing a single instance across threads is that some delegating
   * implementations of DataInput might want to reuse the provided buffer in
   * order to eg. update the checksum. If we shared the same buffer across
   * threads, then another thread might update the buffer while the checksum is
   * being computed, making it invalid. See LUCENE-5583 for more information.
   */
  private byte[] skipBuffer;

  /**
   * Reads and returns a single byte.
   *
   * @see DataOutput#writeByte(byte)
   */
  public abstract byte readByte() throws IOException;

  /**
   * Reads a specified number of bytes into an array at the specified offset.
   *
   * @param b      the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len    the number of bytes to read
   * @see DataOutput#writeBytes(byte[], int)
   */
  public abstract void readBytes(byte[] b, int offset, int len)
      throws IOException;

  /**
   * Reads a specified number of bytes into an array at the
   * specified offset with control over whether the read
   * should be buffered (callers who have their own buffer
   * should pass in "false" for useBuffer).  Currently only
   * {@link BufferedIndexInput} respects this parameter.
   *
   * @param b         the array to read bytes into
   * @param offset    the offset in the array to start storing bytes
   * @param len       the number of bytes to read
   * @param useBuffer set to false if the caller will handle
   *                  buffering.
   * @see DataOutput#writeBytes(byte[], int)
   */
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer)
      throws IOException {
    // Default to ignoring useBuffer entirely
    readBytes(b, offset, len);
  }

  /**
   * Skip over <code>numBytes</code> bytes. The contract on this method is that it
   * should have the same behavior as reading the same number of bytes into a
   * buffer and discarding its content. Negative values of <code>numBytes</code>
   * are not supported.
   */
  public void skipBytes(final long numBytes) throws IOException {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
    }
    if (skipBuffer == null) {
      skipBuffer = new byte[SKIP_BUFFER_SIZE];
    }
    assert skipBuffer.length == SKIP_BUFFER_SIZE;
    for (long skipped = 0; skipped < numBytes; ) {
      final int step = (int) Math.min(SKIP_BUFFER_SIZE, numBytes - skipped);
      readBytes(skipBuffer, 0, step, false);
      skipped += step;
    }
  }

  /**
   * Returns a clone of this stream.
   *
   * <p>Clones of a stream access the same data, and are positioned at the same
   * point as the stream they were cloned from.
   *
   * <p>Expert: Subclasses must ensure that clones may be positioned at
   * different points in the input from each other and from the stream they
   * were cloned from.
   */
  @Override
  public DataInput clone() {
    try {
      return (DataInput) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new Error("This cannot happen: Failing to clone DataInput");
    }
  }

  public TypeReader<? extends DataInput> getTypeReader(ByteOrder byteOrder) {
    // This is the "default" implementation of acquiring a byte-order-specific reader view.
    // DataInput classes that have the possibility to optimize the defaults for a particular
    // byte order (or both) should do so, see ByteArrayDataInput for an example.
    //
    // If this class already implements a TypeReader with the requested byte order,
    // just return the same object (this should be typical case if the implementation
    // and new codecs use the same byte order).
    if (this instanceof TypeReader && ((TypeReader<?>) this).getByteOrder().equals(byteOrder)) {
      @SuppressWarnings("unchecked")
      TypeReader<DataInput> self = (TypeReader<DataInput>) this;
      return self;
    } else {
      // Otherwise use the delegating wrapper. This should still optimize fairly well.
      if (ByteOrder.BIG_ENDIAN.equals(byteOrder)) {
        return new DelegatingTypeReaderBigEndian<>(this);
      } else {
        return new DelegatingTypeReaderLittleEndian<>(this);
      }
    }
  }
}
