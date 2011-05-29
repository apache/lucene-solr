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
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for performing read operations of Lucene's low-level
 * data types.
 */
public abstract class DataInput implements Cloneable {

  private boolean preUTF8Strings;                 // true if we are reading old (modified UTF8) string format

  /** Call this if readString should read characters stored
   *  in the old modified UTF8 format (length in java chars
   *  and java's modified UTF8 encoding).  This is used for
   *  indices written pre-2.4 See LUCENE-510 for details. */
  public void setModifiedUTF8StringsMode() {
    preUTF8Strings = true;
  }

  /** Reads and returns a single byte.
   * @see DataOutput#writeByte(byte)
   */
  public abstract byte readByte() throws IOException;

  /** Reads a specified number of bytes into an array at the specified offset.
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   * @see DataOutput#writeBytes(byte[],int)
   */
  public abstract void readBytes(byte[] b, int offset, int len)
    throws IOException;

  /** Reads a specified number of bytes into an array at the
   * specified offset with control over whether the read
   * should be buffered (callers who have their own buffer
   * should pass in "false" for useBuffer).  Currently only
   * {@link BufferedIndexInput} respects this parameter.
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   * @param useBuffer set to false if the caller will handle
   * buffering.
   * @see DataOutput#writeBytes(byte[],int)
   */
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer)
    throws IOException
  {
    // Default to ignoring useBuffer entirely
    readBytes(b, offset, len);
  }

  /** Reads two bytes and returns a short.
   * @see DataOutput#writeByte(byte)
   */
  public short readShort() throws IOException {
    return (short) (((readByte() & 0xFF) <<  8) |  (readByte() & 0xFF));
  }

  /** Reads four bytes and returns an int.
   * @see DataOutput#writeInt(int)
   */
  public int readInt() throws IOException {
    return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16)
         | ((readByte() & 0xFF) <<  8) |  (readByte() & 0xFF);
  }

  /** Reads an int stored in variable-length format.  Reads between one and
   * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   * @see DataOutput#writeVInt(int)
   */
  public int readVInt() throws IOException {
    /* This is the original code of this method,
     * but a Hotspot bug (see LUCENE-2975) corrupts the for-loop if
     * readByte() is inlined. So the loop was unwinded!
    byte b = readByte();
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte();
      i |= (b & 0x7F) << shift;
    }
    return i;
    */
    byte b = readByte();
    int i = b & 0x7F;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7F) << 7;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7F) << 14;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7F) << 21;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    assert (b & 0x80) == 0;
    return i | ((b & 0x7F) << 28);
  }

  /** Reads eight bytes and returns a long.
   * @see DataOutput#writeLong(long)
   */
  public long readLong() throws IOException {
    return (((long)readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
  }

  /** Reads a long stored in variable-length format.  Reads between one and
   * nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported. */
  public long readVLong() throws IOException {
    /* This is the original code of this method,
     * but a Hotspot bug (see LUCENE-2975) corrupts the for-loop if
     * readByte() is inlined. So the loop was unwinded!
    byte b = readByte();
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte();
      i |= (b & 0x7FL) << shift;
    }
    return i;
    */
    byte b = readByte();
    long i = b & 0x7FL;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 7;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 14;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 21;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 28;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 35;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 42;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 49;
    if ((b & 0x80) == 0) return i;
    b = readByte();
    assert (b & 0x80) == 0;
    return i | ((b & 0x7FL) << 56);
  }

  /** Reads a string.
   * @see DataOutput#writeString(String)
   */
  public String readString() throws IOException {
    if (preUTF8Strings)
      return readModifiedUTF8String();
    int length = readVInt();
    final byte[] bytes = new byte[length];
    readBytes(bytes, 0, length);
    return new String(bytes, 0, length, "UTF-8");
  }

  private String readModifiedUTF8String() throws IOException {
    int length = readVInt();
    final char[] chars = new char[length];
    readChars(chars, 0, length);
    return new String(chars, 0, length);
  }

  /** Reads Lucene's old "modified UTF-8" encoded
   *  characters into an array.
   * @param buffer the array to read characters into
   * @param start the offset in the array to start storing characters
   * @param length the number of characters to read
   * @see DataOutput#writeChars(String,int,int)
   * @deprecated -- please use readString or readBytes
   *                instead, and construct the string
   *                from those utf8 bytes
   */
  @Deprecated
  public void readChars(char[] buffer, int start, int length)
       throws IOException {
    final int end = start + length;
    for (int i = start; i < end; i++) {
      byte b = readByte();
      if ((b & 0x80) == 0)
	buffer[i] = (char)(b & 0x7F);
      else if ((b & 0xE0) != 0xE0) {
	buffer[i] = (char)(((b & 0x1F) << 6)
		 | (readByte() & 0x3F));
      } else {
	buffer[i] = (char)(((b & 0x0F) << 12)
		| ((readByte() & 0x3F) << 6)
	        |  (readByte() & 0x3F));
      }
    }
  }

  /** Returns a clone of this stream.
   *
   * <p>Clones of a stream access the same data, and are positioned at the same
   * point as the stream they were cloned from.
   *
   * <p>Expert: Subclasses must ensure that clones may be positioned at
   * different points in the input from each other and from the stream they
   * were cloned from.
   */
  @Override
  public Object clone() {
    DataInput clone = null;
    try {
      clone = (DataInput)super.clone();
    } catch (CloneNotSupportedException e) {}

    return clone;
  }

  public Map<String,String> readStringStringMap() throws IOException {
    final Map<String,String> map = new HashMap<String,String>();
    final int count = readInt();
    for(int i=0;i<count;i++) {
      final String key = readString();
      final String val = readString();
      map.put(key, val);
    }

    return map;
  }
}
