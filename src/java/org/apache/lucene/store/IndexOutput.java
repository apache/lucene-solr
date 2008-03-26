package org.apache.lucene.store;

/**
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

import java.io.IOException;
import org.apache.lucene.util.UnicodeUtil;

/** Abstract base class for output to a file in a Directory.  A random-access
 * output stream.  Used for all Lucene index output operations.
 * @see Directory
 * @see IndexInput
 */
public abstract class IndexOutput {

  private UnicodeUtil.UTF8Result utf8Result = new UnicodeUtil.UTF8Result();

  /** Writes a single byte.
   * @see IndexInput#readByte()
   */
  public abstract void writeByte(byte b) throws IOException;

  /** Writes an array of bytes.
   * @param b the bytes to write
   * @param length the number of bytes to write
   * @see IndexInput#readBytes(byte[],int,int)
   */
  public void writeBytes(byte[] b, int length) throws IOException {
    writeBytes(b, 0, length);
  }

  /** Writes an array of bytes.
   * @param b the bytes to write
   * @param offset the offset in the byte array
   * @param length the number of bytes to write
   * @see IndexInput#readBytes(byte[],int,int)
   */
  public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

  /** Writes an int as four bytes.
   * @see IndexInput#readInt()
   */
  public void writeInt(int i) throws IOException {
    writeByte((byte)(i >> 24));
    writeByte((byte)(i >> 16));
    writeByte((byte)(i >>  8));
    writeByte((byte) i);
  }

  /** Writes an int in a variable-length format.  Writes between one and
   * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   * @see IndexInput#readVInt()
   */
  public void writeVInt(int i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  /** Writes a long as eight bytes.
   * @see IndexInput#readLong()
   */
  public void writeLong(long i) throws IOException {
    writeInt((int) (i >> 32));
    writeInt((int) i);
  }

  /** Writes an long in a variable-length format.  Writes between one and five
   * bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   * @see IndexInput#readVLong()
   */
  public void writeVLong(long i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByte((byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  /** Writes a string.
   * @see IndexInput#readString()
   */
  public void writeString(String s) throws IOException {
    UnicodeUtil.UTF16toUTF8(s, 0, s.length(), utf8Result);
    writeVInt(utf8Result.length);
    writeBytes(utf8Result.result, 0, utf8Result.length);
  }

  /** Writes a sub sequence of characters from s as the old
   *  format (modified UTF-8 encoded bytes).
   * @param s the source of the characters
   * @param start the first character in the sequence
   * @param length the number of characters in the sequence
   * @deprecated -- please pre-convert to utf8 bytes
   * instead or use {@link #writeString}
   */
  public void writeChars(String s, int start, int length)
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

  /** Writes a sub sequence of characters from char[] as
   *  the old format (modified UTF-8 encoded bytes).
   * @param s the source of the characters
   * @param start the first character in the sequence
   * @param length the number of characters in the sequence
   * @deprecated -- please pre-convert to utf8 bytes instead or use {@link #writeString}
   */
  public void writeChars(char[] s, int start, int length)
    throws IOException {
    final int end = start + length;
    for (int i = start; i < end; i++) {
      final int code = (int)s[i];
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

  private static int COPY_BUFFER_SIZE = 16384;
  private byte[] copyBuffer;

  /** Copy numBytes bytes from input to ourself. */
  public void copyBytes(IndexInput input, long numBytes) throws IOException {
    long left = numBytes;
    if (copyBuffer == null)
      copyBuffer = new byte[COPY_BUFFER_SIZE];
    while(left > 0) {
      final int toCopy;
      if (left > COPY_BUFFER_SIZE)
        toCopy = COPY_BUFFER_SIZE;
      else
        toCopy = (int) left;
      input.readBytes(copyBuffer, 0, toCopy);
      writeBytes(copyBuffer, 0, toCopy);
      left -= toCopy;
    }
  }

  /** Forces any buffered output to be written. */
  public abstract void flush() throws IOException;

  /** Closes this stream to further operations. */
  public abstract void close() throws IOException;

  /** Returns the current position in this file, where the next write will
   * occur.
   * @see #seek(long)
   */
  public abstract long getFilePointer();

  /** Sets current position in this file, where the next write will occur.
   * @see #getFilePointer()
   */
  public abstract void seek(long pos) throws IOException;

  /** The number of bytes in the file. */
  public abstract long length() throws IOException;

  /** Set the file length. By default, this method does
   * nothing (it's optional for a Directory to implement
   * it).  But, certain Directory implementations (for
   * example @see FSDirectory) can use this to inform the
   * underlying IO system to pre-allocate the file to the
   * specified size.  If the length is longer than the
   * current file length, the bytes added to the file are
   * undefined.  Otherwise the file is truncated.
   * @param length file length
   */
  public void setLength(long length) throws IOException {};
}
