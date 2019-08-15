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

package org.apache.solr.common.util;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.noggit.CharArr;

/**A mutable byte[] backed Utf8CharSequence. This is quite similar to the BytesRef of Lucene
 * Do not alter the contents of the byte[] . it may be inconsistent with the cached String
 * This is designed for single-threaded use
 *
 */
public class ByteArrayUtf8CharSequence implements Utf8CharSequence {

  protected byte[] buf;
  protected int offset;
  protected int hashCode = Integer.MIN_VALUE;
  protected int length;
  protected volatile String utf16;
  public Function<ByteArrayUtf8CharSequence, String> stringProvider;

  public ByteArrayUtf8CharSequence(String utf16) {
    buf = new byte[Math.multiplyExact(utf16.length(), 3)];
    offset = 0;
    length = ByteUtils.UTF16toUTF8(utf16, 0, utf16.length(), buf, 0);
    if (buf.length > length) {
      byte[] copy = new byte[length];
      System.arraycopy(buf, 0, copy, 0, length);
      buf = copy;
    }
    assert isValid();
  }

  public byte[] getBuf() {
    return buf;
  }

  public int offset() {
    return offset;
  }

  public ByteArrayUtf8CharSequence(byte[] buf, int offset, int length) {
    this.buf = buf;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public byte byteAt(int idx) {
    if (idx >= length || idx < 0) throw new ArrayIndexOutOfBoundsException("idx must be >=0 and < " + length);
    return buf[offset + idx];
  }

  /**
   * this is for internal use to get a cached string value.
   * returns null if There is no cached String value
   */
  public String getStringOrNull() {
    return utf16;
  }

  @Override
  public int write(int start, byte[] buffer, int pos) {
    return _writeBytes(buf, offset, length, start, buffer, pos);
  }

  static int _writeBytes(byte[] src, int srcOffset, int srcLength, int start, byte[] buffer, int pos) {
    if (srcOffset == -1 || start >= srcLength) return -1;
    int writableBytes = Math.min(srcLength - start, buffer.length - pos);
    System.arraycopy(src, srcOffset + start, buffer, pos, writableBytes);
    return writableBytes;
  }

  @Override
  public int size() {
    return length;
  }

  private ByteArrayUtf8CharSequence(byte[] buf, int offset, int length, String utf16, int hashCode) {
    this.buf = buf;
    this.offset = offset;
    this.length = length;
    this.utf16 = utf16;
    this.hashCode = hashCode;
  }

  @Override
  public int hashCode() {
    if (hashCode == Integer.MIN_VALUE) {
      hashCode = MurmurHash2.hash32(buf, offset, length);
    }
    return hashCode;
  }

  @Override
  public int length() {
    return _getStr().length();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Utf8CharSequence) {
      if (size() != ((Utf8CharSequence) other).size()) return false;
      if (other instanceof ByteArrayUtf8CharSequence) {
        if (this.length != ((ByteArrayUtf8CharSequence) other).length) return false;
        ByteArrayUtf8CharSequence that = (ByteArrayUtf8CharSequence) other;
        return _equals(this.buf, this.offset, this.offset + this.length,
            that.buf, that.offset, that.offset + that.length);
      }
      return utf8Equals(this, (Utf8CharSequence) other);
    } else {
      return false;
    }
  }

  public static boolean utf8Equals(Utf8CharSequence utf8_1, Utf8CharSequence utf8_2) {
    if (utf8_1.size() != utf8_2.size()) return false;
    for (int i = 0; i < utf8_1.size(); i++) {
      if (utf8_1.byteAt(i) != utf8_2.byteAt(i)) return false;
    }
    return true;
  }


  @Override
  public char charAt(int index) {
    return _getStr().charAt(index);
  }

  private String _getStr() {
    String utf16 = this.utf16;
    if (utf16 == null) {
      if (stringProvider != null) {
        this.utf16 = utf16 = stringProvider.apply(this);
      } else {
        CharArr arr = new CharArr();
        ByteUtils.UTF8toUTF16(buf, offset, length, arr);
        this.utf16 = utf16 = arr.toString();
      }

    }
    return utf16;
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return new ByteArrayUtf8CharSequence(_getStr().subSequence(start, end).toString());
  }

  @Override
  public ByteArrayUtf8CharSequence clone() {
    return new ByteArrayUtf8CharSequence(buf, offset, length, utf16, hashCode);
  }

  public ByteArrayUtf8CharSequence deepCopy() {
    byte[] bytes = new byte[length];
    System.arraycopy(buf, offset, bytes, 0, length);
    return new ByteArrayUtf8CharSequence(bytes, 0, length, utf16, hashCode);
  }

  @SuppressWarnings("unchecked")
  public static Map.Entry convertCharSeq(Map.Entry e) {
    if (e.getKey() instanceof Utf8CharSequence || e.getValue() instanceof Utf8CharSequence) {
      return new AbstractMap.SimpleEntry(convertCharSeq(e.getKey()), convertCharSeq(e.getValue()));
    }
    return e;

  }

  @SuppressWarnings("unchecked")
  public static Collection convertCharSeq(Collection vals) {
    if (vals == null) return vals;
    boolean needsCopy = false;
    for (Object o : vals) {
      if (o instanceof Utf8CharSequence) {
        needsCopy = true;
        break;
      }
    }
    if (needsCopy) {
      Collection copy =  null;
      if (vals instanceof Set){
        copy = new HashSet(vals.size());
      } else {
        copy = new ArrayList(vals.size());
      }
      for (Object o : vals) copy.add(convertCharSeq(o));
      return copy;
    }
    return vals;
  }

  public static Object convertCharSeq(Object o) {
    if (o == null) return null;
    if (o instanceof Utf8CharSequence) return ((Utf8CharSequence) o).toString();
    if (o instanceof Collection) return convertCharSeq((Collection) o);
    return o;
  }


  // methods in Arrays are defined stupid: they cannot use Objects.checkFromToIndex
  // they throw IAE (vs IOOBE) in the case of fromIndex > toIndex.
  // so this method works just like checkFromToIndex, but with that stupidity added.
  private static void checkFromToIndex(int fromIndex, int toIndex, int length) {
    if (fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex " + fromIndex + " > toIndex " + toIndex);
    }
    if (fromIndex < 0 || toIndex > length) {
      throw new IndexOutOfBoundsException("Range [" + fromIndex + ", " + toIndex + ") out-of-bounds for length " + length);
    }
  }

  @Override
  public String toString() {
    return _getStr();
  }

  /**
   * Behaves like Java 9's Arrays.equals
   *
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-byte:A-int-int-byte:A-int-int-">Arrays.equals</a>
   */
  public static boolean _equals(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
      return false;
    }
    for (int i = 0; i < aLen; i++) {
      if (a[i + aFromIndex] != b[i + bFromIndex]) {
        return false;
      }
    }
    return true;
  }


  public ByteArrayUtf8CharSequence reset(byte[] bytes, int offset, int length, String str) {
    this.buf = bytes;
    this.offset = offset;
    this.length = length;
    this.utf16 = str;
    this.hashCode = Integer.MIN_VALUE;
    return this;
  }

  /**
   * Performs internal consistency checks.
   * Always returns true (or throws IllegalStateException)
   */
  public boolean isValid() {
    if (buf == null) {
      throw new IllegalStateException("bytes is null");
    }
    if (length < 0) {
      throw new IllegalStateException("length is negative: " + length);
    }
    if (length > buf.length) {
      throw new IllegalStateException("length is out of bounds: " + length + ",bytes.length=" + buf.length);
    }
    if (offset < 0) {
      throw new IllegalStateException("offset is negative: " + offset);
    }
    if (offset > buf.length) {
      throw new IllegalStateException("offset out of bounds: " + offset + ",bytes.length=" + buf.length);
    }
    if (offset + length < 0) {
      throw new IllegalStateException("offset+length is negative: offset=" + offset + ",length=" + length);
    }
    if (offset + length > buf.length) {
      throw new IllegalStateException("offset+length out of bounds: offset=" + offset + ",length=" + length + ",bytes.length=" + buf.length);
    }
    return true;
  }
}
