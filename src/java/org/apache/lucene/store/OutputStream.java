package org.apache.lucene.store;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

/** Abstract class for output to a file in a Directory.  A random-access output
 * stream.  Used for all Lucene index output operations.
 * @see Directory
 * @see InputStream
 */
public abstract class OutputStream {
  static final int BUFFER_SIZE = 1024;

  private final byte[] buffer = new byte[BUFFER_SIZE];
  private long bufferStart = 0;			  // position in file of buffer
  private int bufferPosition = 0;		  // position in buffer

  /** Writes a single byte.
   * @see InputStream#readByte()
   */
  public final void writeByte(byte b) throws IOException {
    if (bufferPosition >= BUFFER_SIZE)
      flush();
    buffer[bufferPosition++] = b;
  }

  /** Writes an array of bytes.
   * @param b the bytes to write
   * @param length the number of bytes to write
   * @see InputStream#readBytes(byte[],int,int)
   */
  public final void writeBytes(byte[] b, int length) throws IOException {
    for (int i = 0; i < length; i++)
      writeByte(b[i]);
  }

  /** Writes an int as four bytes.
   * @see InputStream#readInt()
   */
  public final void writeInt(int i) throws IOException {
    writeByte((byte)(i >> 24));
    writeByte((byte)(i >> 16));
    writeByte((byte)(i >>  8));
    writeByte((byte) i);
  }

  /** Writes an int in a variable-length format.  Writes between one and
   * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   * @see InputStream#readVInt()
   */
  public final void writeVInt(int i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  /** Writes a long as eight bytes.
   * @see InputStream#readLong()
   */
  public final void writeLong(long i) throws IOException {
    writeInt((int) (i >> 32));
    writeInt((int) i);
  }

  /** Writes an long in a variable-length format.  Writes between one and five
   * bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   * @see InputStream#readVLong()
   */
  public final void writeVLong(long i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  /** Writes a string.
   * @see InputStream#readString()
   */
  public final void writeString(String s) throws IOException {
    int length = s.length();
    writeVInt(length);
    writeChars(s, 0, length);
  }

  /** Writes a sequence of UTF-8 encoded characters from a string.
   * @param s the source of the characters
   * @param start the first character in the sequence
   * @param length the number of characters in the sequence
   * @see InputStream#readChars(char[],int,int)
   */
  public final void writeChars(String s, int start, int length)
       throws IOException {
    final int end = start + length;
    for (int i = start; i < end; i++) {
      final int code = (int)s.charAt(i);
      if (code >= 0x01 && code <= 0x7F)
	writeByte((byte)code);
      else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) {
	writeByte((byte)(0xC0 | (code >> 6)));
	writeByte((byte)(0x80 | (code & 0x3F)));
      } else {
	writeByte((byte)(0xE0 | (code >>> 12)));
	writeByte((byte)(0x80 | ((code >> 6) & 0x3F)));
	writeByte((byte)(0x80 | (code & 0x3F)));
      }
    }
  }

  /** Forces any buffered output to be written. */
  protected final void flush() throws IOException {
    flushBuffer(buffer, bufferPosition);
    bufferStart += bufferPosition;
    bufferPosition = 0;
  }

  /** Expert: implements buffer write.  Writes bytes at the current position in
   * the output.
   * @param b the bytes to write
   * @param len the number of bytes to write
   */
  protected abstract void flushBuffer(byte[] b, int len) throws IOException;

  /** Closes this stream to further operations. */
  public void close() throws IOException {
    flush();
  }

  /** Returns the current position in this file, where the next write will
   * occur.
   * @see #seek(long)
   */
  public final long getFilePointer() throws IOException {
    return bufferStart + bufferPosition;
  }

  /** Sets current position in this file, where the next write will occur.
   * @see #getFilePointer()
   */
  public void seek(long pos) throws IOException {
    flush();
    bufferStart = pos;
  }

  /** The number of bytes in the file. */
  public abstract long length() throws IOException;


}
