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
public final class BytesRef implements Comparable<BytesRef> {

  static final int HASH_PRIME = 31;
  public static final byte[] EMPTY_BYTES = new byte[0]; 

  /** The contents of the BytesRef. Should never be {@code null}. */
  public byte[] bytes;

  /** Offset of first valid byte. */
  public int offset;

  /** Length of used bytes. */
  public int length;

  public BytesRef() {
    bytes = EMPTY_BYTES;
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
    assert bytes != null;
    this.bytes = bytes;
    this.offset = 0;
    this.length = bytes.length;
  }

  public BytesRef(int capacity) {
    this.bytes = new byte[capacity];
  }

  /** Incoming IntsRef values must be Byte.MIN_VALUE -
   *  Byte.MAX_VALUE. */
  public BytesRef(IntsRef intsRef) {
    bytes = new byte[intsRef.length];
    for(int idx=0;idx<intsRef.length;idx++) {
      final int v = intsRef.ints[intsRef.offset + idx];
      assert v >= Byte.MIN_VALUE && v <= Byte.MAX_VALUE;
      bytes[idx] = (byte) v;
    }
    length = intsRef.length;
  }

  /**
   * @param text Initialize the byte[] from the UTF8 bytes
   * for the provided String.  This must be well-formed
   * unicode text, with no unpaired surrogates or U+FFFF.
   */
  public BytesRef(CharSequence text) {
    this();
    copy(text);
  }
  
  /**
   * @param text Initialize the byte[] from the UTF8 bytes
   * for the provided array.  This must be well-formed
   * unicode text, with no unpaired surrogates or U+FFFF.
   */
  public BytesRef(char text[], int offset, int length) {
    this(length * 4);
    copy(text, offset, length);
  }

  public BytesRef(BytesRef other) {
    this();
    copy(other);
  }

  /* // maybe?
  public BytesRef(BytesRef other, boolean shallow) {
    this();
    if (shallow) {
      offset = other.offset;
      length = other.length;
      bytes = other.bytes;
    } else {
      copy(other);
    }
  }
  */

  /**
   * Copies the UTF8 bytes for this string.
   * 
   * @param text Must be well-formed unicode text, with no
   * unpaired surrogates or invalid UTF16 code units.
   */
  public void copy(CharSequence text) {
    UnicodeUtil.UTF16toUTF8(text, 0, text.length(), this);
  }

  /**
   * Copies the UTF8 bytes for this string.
   * 
   * @param text Must be well-formed unicode text, with no
   * unpaired surrogates or invalid UTF16 code units.
   */
  public void copy(char text[], int offset, int length) {
    UnicodeUtil.UTF16toUTF8(text, offset, length, this);
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
    int result = 0;
    final int end = offset + length;
    for(int i=offset;i<end;i++) {
      result = HASH_PRIME * result + bytes[i];
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    return this.bytesEquals((BytesRef) other);
  }

  /** Interprets stored bytes as UTF8 bytes, returning the
   *  resulting string */
  public String utf8ToString() {
    final CharsRef ref = new CharsRef(length);
    UnicodeUtil.UTF8toUTF16(bytes, offset, length, ref);
    return ref.toString(); 
  }
  
  /** Interprets stored bytes as UTF8 bytes into the given {@link CharsRef} */
  public CharsRef utf8ToChars(CharsRef ref) {
    UnicodeUtil.UTF8toUTF16(bytes, offset, length, ref);
    return ref;
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
   * Copies the given {@link BytesRef}
   * <p>
   * NOTE: this method resets the offset to 0 and resizes the reference array
   * if needed.
   */
  public void copy(BytesRef other) {
    if (bytes.length < other.length) {
      bytes = new byte[other.length];
    }
    System.arraycopy(other.bytes, other.offset, bytes, 0, other.length);
    length = other.length;
    offset = 0;
  }

  /**
   * Copies the given long value and encodes it as 8 byte Big-Endian.
   * <p>
   * NOTE: this method resets the offset to 0, length to 8 and resizes the reference array
   * if needed.
   */
  public void copy(long value) {
    if (bytes.length < 8) {
      bytes = new byte[8];
    }
    copyInternal((int) (value >> 32), offset = 0);
    copyInternal((int) value, 4);
    length = 8;
  }
  
  /**
   * Copies the given int value and encodes it as 4 byte Big-Endian.
   * <p>
   * NOTE: this method resets the offset to 0, length to 4 and resizes the reference array
   * if needed.
   */
  public void copy(int value) {
    if (bytes.length < 4) {
      bytes = new byte[4];
    }
    copyInternal(value, offset = 0);
    length = 4;
  }

  /**
   * Copies the given short value and encodes it as a 2 byte Big-Endian.
   * <p>
   * NOTE: this method resets the offset to 0, length to 2 and resizes the reference array
   * if needed.
   */
  public void copy(short value) {
    if (bytes.length < 2) {
      bytes = new byte[2];
    }
    bytes[offset] = (byte) (value >> 8);
    bytes[offset + 1] = (byte) (value);

  }
  
  /**
   * Converts 2 consecutive bytes from the current offset to a short. Bytes are
   * interpreted as Big-Endian (most significant bit first)
   * <p>
   * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
   */
  public short asShort() {
    int pos = offset;
    return (short) (0xFFFF & ((bytes[pos++] & 0xFF) << 8) | (bytes[pos] & 0xFF));
  }

  /**
   * Converts 4 consecutive bytes from the current offset to an int. Bytes are
   * interpreted as Big-Endian (most significant bit first)
   * <p>
   * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
   */
  public int asInt() {
    return asIntInternal(offset);
  }

  /**
   * Converts 8 consecutive bytes from the current offset to a long. Bytes are
   * interpreted as Big-Endian (most significant bit first)
   * <p>
   * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
   */
  public long asLong() {
    return (((long) asIntInternal(offset) << 32) | asIntInternal(offset + 4) & 0xFFFFFFFFL);
  }

  private void copyInternal(int value, int startOffset) {
    bytes[startOffset] = (byte) (value >> 24);
    bytes[startOffset + 1] = (byte) (value >> 16);
    bytes[startOffset + 2] = (byte) (value >> 8);
    bytes[startOffset + 3] = (byte) (value);
  }

  private int asIntInternal(int pos) {
    return ((bytes[pos++] & 0xFF) << 24) | ((bytes[pos++] & 0xFF) << 16)
        | ((bytes[pos++] & 0xFF) << 8) | (bytes[pos] & 0xFF);
  }

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

  public void grow(int newLength) {
    bytes = ArrayUtil.grow(bytes, newLength);
  }

  /** Unsigned byte order comparison */
  public int compareTo(BytesRef other) {
    if (this == other) return 0;

    final byte[] aBytes = this.bytes;
    int aUpto = this.offset;
    final byte[] bBytes = other.bytes;
    int bUpto = other.offset;

    final int aStop = aUpto + Math.min(this.length, other.length);

    while(aUpto < aStop) {
      int aByte = aBytes[aUpto++] & 0xff;
      int bByte = bBytes[bUpto++] & 0xff;
      int diff = aByte - bByte;
      if (diff != 0) return diff;
    }

    // One is a prefix of the other, or, they are equal:
    return this.length - other.length;
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
      
      final int aStop;
      if (a.length < b.length) {
        aStop = aUpto + a.length;
      } else {
        aStop = aUpto + b.length;
      }

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
}
