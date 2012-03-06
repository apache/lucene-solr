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

/** Represents byte[], as a slice (offset + length) into an
 *  existing byte[].  The {@link #bytes} member should never be null;
 *  use {@link #EMPTY_BYTES} if necessary.
 *
 *  @lucene.experimental */
public final class BytesRef implements Comparable<BytesRef>,Cloneable {
  /** An empty byte array for convenience */
  public static final byte[] EMPTY_BYTES = new byte[0]; 

  /** The contents of the BytesRef. Should never be {@code null}. */
  public byte[] bytes;

  /** Offset of first valid byte. */
  public int offset;

  /** Length of used bytes. */
  public int length;

  /** Create a BytesRef with {@link #EMPTY_BYTES} */
  public BytesRef() {
    this(EMPTY_BYTES);
  }

  /** This instance will directly reference bytes w/o making a copy.
   * bytes should not be null.
   */
  public BytesRef(byte[] bytes, int offset, int length) {
    assert bytes != null;
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /** This instance will directly reference bytes w/o making a copy.
   * bytes should not be null */
  public BytesRef(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  /** 
   * Create a BytesRef pointing to a new array of size <code>capacity</code>.
   * Offset and length will both be zero.
   */
  public BytesRef(int capacity) {
    this.bytes = new byte[capacity];
  }

  /**
   * Initialize the byte[] from the UTF8 bytes
   * for the provided String.  
   * 
   * @param text This must be well-formed
   * unicode text, with no unpaired surrogates.
   */
  public BytesRef(CharSequence text) {
    this();
    copyChars(text);
  }

  /**
   * Copies the UTF8 bytes for this string.
   * 
   * @param text Must be well-formed unicode text, with no
   * unpaired surrogates or invalid UTF16 code units.
   */
  // TODO broken if offset != 0
  public void copyChars(CharSequence text) {
    UnicodeUtil.UTF16toUTF8(text, 0, text.length(), this);
  }
  
  /**
   * Expert: compares the bytes against another BytesRef,
   * returning true if the bytes are equal.
   * 
   * @param other Another BytesRef, should not be null.
   * @lucene.internal
   */
  public boolean bytesEquals(BytesRef other) {
    assert other != null;
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
  public BytesRef clone() {
    return new BytesRef(bytes, offset, length);
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
    int hash = 0;
    final int end = offset + length;
    for(int i=offset;i<end;i++) {
      hash = 31 * hash + bytes[i];
    }
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof BytesRef) {
      return this.bytesEquals((BytesRef) other);
    }
    return false;
  }

  /** Interprets stored bytes as UTF8 bytes, returning the
   *  resulting string */
  public String utf8ToString() {
    final CharsRef ref = new CharsRef(length);
    UnicodeUtil.UTF8toUTF16(bytes, offset, length, ref);
    return ref.toString(); 
  }

  /** Returns hex encoded bytes, eg [0x6c 0x75 0x63 0x65 0x6e 0x65] */
  @Override
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

  /**
   * Copies the bytes from the given {@link BytesRef}
   * <p>
   * NOTE: if this would exceed the array size, this method creates a 
   * new reference array.
   */
  public void copyBytes(BytesRef other) {
    if (bytes.length < other.length) {
      bytes = new byte[other.length];
      offset = 0;
    }
    System.arraycopy(other.bytes, other.offset, bytes, offset, other.length);
    length = other.length;
  }

  /**
   * Appends the bytes from the given {@link BytesRef}
   * <p>
   * NOTE: if this would exceed the array size, this method creates a 
   * new reference array.
   */
  public void append(BytesRef other) {
    int newLen = length + other.length;
    if (bytes.length < newLen) {
      byte[] newBytes = new byte[newLen];
      System.arraycopy(bytes, offset, newBytes, 0, length);
      offset = 0;
      bytes = newBytes;
    }
    System.arraycopy(other.bytes, other.offset, bytes, length+offset, other.length);
    length = newLen;
  }

  // TODO: stupid if existing offset is non-zero.
  /** @lucene.internal */
  public void grow(int newLength) {
    bytes = ArrayUtil.grow(bytes, newLength);
  }

  /** Unsigned byte order comparison */
  public int compareTo(BytesRef other) {
    return utf8SortedAsUnicodeSortOrder.compare(this, other);
  }
  
  private final static Comparator<BytesRef> utf8SortedAsUnicodeSortOrder = new UTF8SortedAsUnicodeComparator();

  public static Comparator<BytesRef> getUTF8SortedAsUnicodeComparator() {
    return utf8SortedAsUnicodeSortOrder;
  }

  private static class UTF8SortedAsUnicodeComparator implements Comparator<BytesRef> {
    // Only singleton
    private UTF8SortedAsUnicodeComparator() {};

    public int compare(BytesRef a, BytesRef b) {
      final byte[] aBytes = a.bytes;
      int aUpto = a.offset;
      final byte[] bBytes = b.bytes;
      int bUpto = b.offset;
      
      final int aStop = aUpto + Math.min(a.length, b.length);
      while(aUpto < aStop) {
        int aByte = aBytes[aUpto++] & 0xff;
        int bByte = bBytes[bUpto++] & 0xff;

        int diff = aByte - bByte;
        if (diff != 0) {
          return diff;
        }
      }

      // One is a prefix of the other, or, they are equal:
      return a.length - b.length;
    }    
  }

  /** @deprecated */
  @Deprecated
  private final static Comparator<BytesRef> utf8SortedAsUTF16SortOrder = new UTF8SortedAsUTF16Comparator();

  /** @deprecated This comparator is only a transition mechanism */
  @Deprecated
  public static Comparator<BytesRef> getUTF8SortedAsUTF16Comparator() {
    return utf8SortedAsUTF16SortOrder;
  }

  /** @deprecated */
  @Deprecated
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
          
          // NOTE: instead of moving supplementary code points (0xee and 0xef) to the unused 0xfe and 0xff, 
          // we move them to the unused 0xfc and 0xfd [reserved for future 6-byte character sequences]
          // this reserves 0xff for preflex's term reordering (surrogate dance), and if unicode grows such
          // that 6-byte sequences are needed we have much bigger problems anyway.
          if (aByte >= 0xee && bByte >= 0xee) {
            if ((aByte & 0xfe) == 0xee) {
              aByte += 0xe;
            }
            if ((bByte&0xfe) == 0xee) {
              bByte += 0xe;
            }
          }
          return aByte - bByte;
        }
      }

      // One is a prefix of the other, or, they are equal:
      return a.length - b.length;
    }
  }
  
  /**
   * Creates a new BytesRef that points to a copy of the bytes from 
   * <code>other</code>
   * <p>
   * The returned BytesRef will have a length of other.length
   * and an offset of zero.
   */
  public static BytesRef deepCopyOf(BytesRef other) {
    BytesRef copy = new BytesRef();
    copy.copyBytes(other);
    return copy;
  }
}
