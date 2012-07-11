package org.apache.lucene.util;

import java.io.IOException;
import java.io.OutputStream;

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

/**
 * This class is used as a wrapper to a byte array, extending
 * {@link OutputStream}. Data is written in the given byte[] buffer, until its
 * length is insufficient. Than the buffer size is doubled and the data is
 * written.
 * 
 * This class is Unsafe as it is using a buffer which potentially can be changed
 * from the outside. Moreover, when {@link #toByteArray()} is called, the buffer
 * itself is returned, and not a copy.
 * 
 * @lucene.experimental
 */
public class UnsafeByteArrayOutputStream extends OutputStream {

  private byte[] buffer;
  private int index;
  private int startIndex;

  /**
   * Constructs a new output stream, with a default allocated buffer which can
   * later be obtained via {@link #toByteArray()}.
   */
  public UnsafeByteArrayOutputStream() {
    reInit(new byte[32], 0);
  }

  /**
   * Constructs a new output stream, with a given buffer. Writing will start
   * at index 0 as a default.
   * 
   * @param buffer
   *            some space to which writing will be made
   */
  public UnsafeByteArrayOutputStream(byte[] buffer) {
    reInit(buffer, 0);
  }

  /**
   * Constructs a new output stream, with a given buffer. Writing will start
   * at a given index.
   * 
   * @param buffer
   *            some space to which writing will be made.
   * @param startPos
   *            an index (inclusive) from white data will be written.
   */
  public UnsafeByteArrayOutputStream(byte[] buffer, int startPos) {
    reInit(buffer, startPos);
  }

  private void grow(int newLength) {
    // It actually should be: (Java 1.7, when its intrinsic on all machines)
    // buffer = Arrays.copyOf(buffer, newLength);
    byte[] newBuffer = new byte[newLength];
    System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
    buffer = newBuffer;
  }

  /**
   * For reuse-ability, this stream object can be re-initialized with another
   * given buffer and starting position.
   * 
   * @param buffer some space to which writing will be made.
   * @param startPos an index (inclusive) from white data will be written.
   */
  public void reInit(byte[] buffer, int startPos) {
    if (buffer.length == 0) {
      throw new IllegalArgumentException("initial buffer length must be greater than 0.");
    }
    this.buffer = buffer;
    startIndex = startPos;
    index = startIndex;
  }

  /**
   * For reuse-ability, this stream object can be re-initialized with another
   * given buffer, using 0 as default starting position.
   * 
   * @param buffer some space to which writing will be made.
   */
  public void reInit(byte[] buffer) {
    reInit(buffer, 0);
  }

  /**
   * writes a given byte(at the form of an int) to the buffer. If the buffer's
   * empty space is insufficient, the buffer is doubled.
   * 
   * @param value byte value to be written
   */
  @Override
  public void write(int value) throws IOException {
    if (index >= buffer.length) {
      grow(buffer.length << 1);
    }
    buffer[index++] = (byte) value;
  }

  /**
   * writes a given byte[], with offset and length to the buffer. If the
   * buffer's empty space is insufficient, the buffer is doubled until it
   * could contain all the data.
   * 
   * @param b
   *            byte buffer, containing the source data to be written
   * @param off
   *            index from which data from the buffer b should be written
   * @param len
   *            number of bytes that should be written
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // If there's not enough space for the data
    int targetLength = index + len;
    if (targetLength >= buffer.length) {
      // Calculating the new required length of the array, keeping the array
      // size a power of 2 if it was initialized like that.
      int newlen = buffer.length;
      while ((newlen <<= 1) < targetLength) {}
      grow(newlen);
    }

    // Now that we have enough spare space, we could copy the rest of the
    // data
    System.arraycopy(b, off, buffer, index, len);

    // Updating the index to next available index.
    index += len;
  }

  /**
   * Returns the byte array saved within the buffer AS IS.
   * 
   * @return the actual inner buffer - not a copy of it.
   */
  public byte[] toByteArray() {
    return buffer;
  }

  /**
   * Returns the number of relevant bytes. This objects makes sure the buffer
   * is at least the size of it's data. But it can also be twice as big. The
   * user would want to process the relevant bytes only. For that he would
   * need the count.
   * 
   * @return number of relevant bytes
   */
  public int length() {
    return index;
  }

  /**
   * Returns the start position data was written to. This is useful in case you
   * used {@link #reInit(byte[], int)} or
   * {@link #UnsafeByteArrayOutputStream(byte[], int)} and passed a start
   * position which is not 0.
   */
  public int getStartPos() {
    return startIndex;
  }

}
