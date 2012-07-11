package org.apache.lucene.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

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
 * This class, much like {@link ByteArrayInputStream} uses a given buffer as a
 * source of an InputStream. Unlike ByteArrayInputStream, this class does not
 * "waste" memory by creating a local copy of the given buffer, but rather uses
 * the given buffer as is. Hence the name Unsafe. While using this class one
 * should remember that the byte[] buffer memory is shared and might be changed
 * from outside.
 * 
 * For reuse-ability, a call for {@link #reInit(byte[])} can be called, and
 * initialize the stream with a new buffer.
 * 
 * @lucene.experimental
 */
public class UnsafeByteArrayInputStream extends InputStream {

  private byte[] buffer;
  private int markIndex;
  private int upperLimit;
  private int index;

  /**
   * Creates a new instance by not using any byte[] up front. If you use this
   * constructor, you MUST call either of the {@link #reInit(byte[]) reInit}
   * methods before you consume any byte from this instance.<br>
   * This constructor is for convenience purposes only, so that if one does not
   * have the byte[] at the moment of creation, one is not forced to pass a
   * <code>new byte[0]</code> or something. Obviously in that case, one will
   * call either {@link #reInit(byte[]) reInit} methods before using the class.
   */
  public UnsafeByteArrayInputStream() {
    markIndex = upperLimit = index = 0;
  }

  /**
   * Creates an UnsafeByteArrayInputStream which uses a given byte array as
   * the source of the stream. Default range is [0 , buffer.length)
   * 
   * @param buffer
   *            byte array used as the source of this stream
   */
  public UnsafeByteArrayInputStream(byte[] buffer) {
    reInit(buffer, 0, buffer.length);
  }

  /**
   * Creates an UnsafeByteArrayInputStream which uses a given byte array as
   * the source of the stream, at the specific range: [startPos, endPos)
   * 
   * @param buffer
   *            byte array used as the source of this stream
   * @param startPos
   *            first index (inclusive) to the data lying in the given buffer
   * @param endPos
   *            an index (exclusive) where the data ends. data @
   *            buffer[endPos] will never be read
   */
  public UnsafeByteArrayInputStream(byte[] buffer, int startPos, int endPos) {
    reInit(buffer, startPos, endPos);
  }

  @Override
  public void mark(int readlimit) {
    markIndex = index;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  /**
   * Initialize the stream with a given buffer, using the default limits of
   * [0, buffer.length)
   * 
   * @param buffer
   *            byte array used as the source of this stream
   */
  public void reInit(byte[] buffer) {
    reInit(buffer, 0, buffer.length);
  }

  /**
   * Initialize the stream with a given byte array as the source of the
   * stream, at the specific range: [startPos, endPos)
   * 
   * @param buffer
   *            byte array used as the source of this stream
   * @param startPos
   *            first index (inclusive) to the data lying in the given buffer
   * @param endPos
   *            an index (exclusive) where the data ends. data @
   *            buffer[endPos] will never be read
   */
  public void reInit(byte[] buffer, int startPos, int endPos) {
    this.buffer = buffer;
    markIndex = startPos;
    upperLimit = endPos;
    index = markIndex;
  }

  @Override
  public int available() throws IOException {
    return upperLimit - index;
  }

  /**
   * Read a byte. Data returned as an integer [0,255] If end of stream
   * reached, returns -1
   */
  @Override
  public int read() throws IOException {
    return index < upperLimit ? buffer[index++] & 0xff : -1;
  }

  /**
   * Resets the stream back to its original state. Basically - moving the
   * index back to start position.
   */
  @Override
  public void reset() throws IOException {
    index = markIndex;
  }

}
