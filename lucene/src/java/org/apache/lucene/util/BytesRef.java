package org.apache.lucene.util;

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

import java.util.Comparator;
import java.io.UnsupportedEncodingException;

/** Represents byte[], as a slice (offset + length) into an
 *  existing byte[].
 *
 *  @lucene.experimental */
public final class BytesRef {

  public byte[] bytes;
  public int offset;
  public int length;

  public BytesRef() {
  }

  public BytesRef(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  public BytesRef(byte[] bytes) {
    this.bytes = bytes;
    this.offset = 0;
    this.length = bytes.length;
  }

  public BytesRef(int capacity) {
    this.bytes = new byte[capacity];
  }

  /**
   * @param text Initialize the byte[] from the UTF8 bytes
   * for the provided Sring.  This must be well-formed
   * unicode text, with no unpaired surrogates or U+FFFF.
   */
  public BytesRef(CharSequence text) {
    copy(text);
  }

  public BytesRef(BytesRef other) {
    copy(other);
  }

  /**
   * Copies the UTF8 bytes for this string.
   * 
   * @param text Must be well-formed unicode text, with no
   * unpaired surrogates or invalid UTF16 code units.
   */
  public void copy(CharSequence text) {
    // TODO: new byte[10] is waste of resources; it should
    // simply allocate text.length()*4 like UnicodeUtil.
    // Ideally, I would remove this here and add a
    // null-check in UnicodeUtil. (Uwe)
    if (bytes == null) {
      bytes = new byte[10];
    }
    UnicodeUtil.UTF16toUTF8(text, 0, text.length(), this);
  }

  public boolean bytesEquals(BytesRef other) {
    if (length == other.length) {
      int otherUpto = other.offset;
      final byte[] otherBytes = other.bytes;
      final int end = offset + length;
      for(int upto=offset;upto<end;upto++,otherUpto++) {
        if (bytes[upto] != otherBytes[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() {
    return new BytesRef(this);
  }

  private boolean sliceEquals(BytesRef other, int pos) {
    if (pos < 0 || length - pos < other.length) {
      return false;
    }
    int i = offset + pos;
    int j = other.offset;
    final int k = other.offset + other.length;
    
    while (j < k) {
      if (bytes[i++] != other.bytes[j++]) {
        return false;
      }
    }
    
    return true;
  }
  
  public boolean startsWith(BytesRef other) {
    return sliceEquals(other, 0);
  }

  public boolean endsWith(BytesRef other) {
    return sliceEquals(other, length - other.length);
  }
  
  /** Calculates the hash code as required by TermsHash during indexing.
   * <p>It is defined as:
   * <pre>
   *  int hash = 0;
   *  for (int i = offset; i &lt; offset + length; i++) {
   *    hash = 31*hash + bytes[i];
   *  }
   * </pre>
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 0;
    final int end = offset + length;
    for(int i=offset;i<end;i++) {
      result = prime * result + bytes[i];
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return this.bytesEquals((BytesRef) other);
  }

  /** Interprets stored bytes as UTF8 bytes, returning the
   *  resulting string */
  public String utf8ToString() {
    try {
      return new String(bytes, offset, length, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      // should not happen -- UTF8 is presumably supported
      // by all JREs
      throw new RuntimeException(uee);
    }
  }

  /** Returns hex encoded bytes, eg [0x6c 0x75 0x63 0x65 0x6e 0x65] */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    final int end = offset + length;
    for(int i=offset;i<end;i++) {
      if (i > offset) {
        sb.append(' ');
      }
      sb.append(Integer.toHexString(bytes[i]&0xff));
    }
    sb.append(']');
    return sb.toString();
  }

  public void copy(BytesRef other) {
    if (bytes == null) {
      bytes = new byte[other.length];
    } else {
      bytes = ArrayUtil.grow(bytes, other.length);
    }
    System.arraycopy(other.bytes, other.offset, bytes, 0, other.length);
    length = other.length;
    offset = 0;
  }

  public void grow(int newLength) {
    bytes = ArrayUtil.grow(bytes, newLength);
  }

  private final static Comparator<BytesRef> utf8SortedAsUTF16SortOrder = new UTF8SortedAsUTF16Comparator();

  public static Comparator<BytesRef> getUTF8SortedAsUTF16Comparator() {
    return utf8SortedAsUTF16SortOrder;
  }

  private static class UTF8SortedAsUTF16Comparator implements Comparator<BytesRef> {
    // Only singleton
    private UTF8SortedAsUTF16Comparator() {};

    public int compare(BytesRef a, BytesRef b) {

      final byte[] aBytes = a.bytes;
      int aUpto = a.offset;
      final byte[] bBytes = b.bytes;
      int bUpto = b.offset;
      
      final int aStop;
      if (a.length < b.length) {
        aStop = aUpto + a.length;
      } else {
        aStop = aUpto + b.length;
      }

      while(aUpto < aStop) {
        int aByte = aBytes[aUpto++] & 0xff;
        int bByte = bBytes[bUpto++] & 0xff;

        if (aByte != bByte) {

          // See http://icu-project.org/docs/papers/utf16_code_point_order.html#utf-8-in-utf-16-order

          // We know the terms are not equal, but, we may
          // have to carefully fixup the bytes at the
          // difference to match UTF16's sort order:
          if (aByte >= 0xee && bByte >= 0xee) {
            if ((aByte & 0xfe) == 0xee) {
              aByte += 0x10;
            }
            if ((bByte&0xfe) == 0xee) {
              bByte += 0x10;
            }
          }
          return aByte - bByte;
        }
      }

      // One is a prefix of the other, or, they are equal:
      return a.length - b.length;
    }

    public boolean equals(Object other) {
      return this == other;
    }
  }
}
