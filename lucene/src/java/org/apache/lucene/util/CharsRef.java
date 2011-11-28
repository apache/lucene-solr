package org.apache.lucene.util;

import java.util.Comparator;

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

/**
 * Represents char[], as a slice (offset + length) into an existing char[].
 * The {@link #chars} member should never be null; use
 * {@link #EMPTY_CHARS} if necessary.
 * @lucene.internal
 */
public final class CharsRef implements Comparable<CharsRef>, CharSequence, Cloneable {
  public static final char[] EMPTY_CHARS = new char[0];
  public char[] chars;
  public int offset;
  public int length;

  /**
   * Creates a new {@link CharsRef} initialized an empty array zero-length
   */
  public CharsRef() {
    this(EMPTY_CHARS, 0, 0);
  }

  /**
   * Creates a new {@link CharsRef} initialized with an array of the given
   * capacity
   */
  public CharsRef(int capacity) {
    chars = new char[capacity];
  }

  /**
   * Creates a new {@link CharsRef} initialized with the given array, offset and
   * length
   */
  public CharsRef(char[] chars, int offset, int length) {
    assert chars != null;
    assert chars.length >= offset + length;
    this.chars = chars;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Creates a new {@link CharsRef} initialized with the given Strings character
   * array
   */
  public CharsRef(String string) {
    this.chars = string.toCharArray();
    this.offset = 0;
    this.length = chars.length;
  }

  @Override
  public CharsRef clone() {
    return new CharsRef(chars, offset, length);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 0;
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      result = prime * result + chars[i];
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof CharsRef) {
      return this.charsEquals((CharsRef) other);
    }
    return false;
  }

  public boolean charsEquals(CharsRef other) {
    if (length == other.length) {
      int otherUpto = other.offset;
      final char[] otherChars = other.chars;
      final int end = offset + length;
      for (int upto = offset; upto < end; upto++, otherUpto++) {
        if (chars[upto] != otherChars[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /** Signed int order comparison */
  public int compareTo(CharsRef other) {
    if (this == other)
      return 0;

    final char[] aChars = this.chars;
    int aUpto = this.offset;
    final char[] bChars = other.chars;
    int bUpto = other.offset;

    final int aStop = aUpto + Math.min(this.length, other.length);

    while (aUpto < aStop) {
      int aInt = aChars[aUpto++];
      int bInt = bChars[bUpto++];
      if (aInt > bInt) {
        return 1;
      } else if (aInt < bInt) {
        return -1;
      }
    }

    // One is a prefix of the other, or, they are equal:
    return this.length - other.length;
  }
  
  /**
   * Copies the given {@link CharsRef} referenced content into this instance
   * starting at offset 0.
   * 
   * @param other
   *          the {@link CharsRef} to copy
   */
  // TODO: why does this behave differently/not invoke copyChars(char[], int, int) ???
  public void copyChars(CharsRef other) {
    if (chars == null) {
      chars = new char[other.length];
    } else {
      chars = ArrayUtil.grow(chars, other.length);
    }
    System.arraycopy(other.chars, other.offset, chars, 0, other.length);
    length = other.length;
    offset = 0;
  }

  public void grow(int newLength) {
    if (chars.length < newLength) {
      chars = ArrayUtil.grow(chars, newLength);
    }
  }

  /**
   * Copies the given array into this CharsRef starting at offset 0
   */
  public void copyChars(char[] otherChars, int otherOffset, int otherLength) {
    grow(otherLength);
    System.arraycopy(otherChars, otherOffset, this.chars, 0,
        otherLength);
    this.offset = 0;
    this.length = otherLength;
  }

  /**
   * Appends the given array to this CharsRef
   */
  public void append(char[] otherChars, int otherOffset, int otherLength) {
    final int newLength = length + otherLength;
    grow(this.offset + newLength);
    System.arraycopy(otherChars, otherOffset, this.chars, this.offset+length,
        otherLength);
    this.length += otherLength;
  }

  @Override
  public String toString() {
    return new String(chars, offset, length);
  }

  public int length() {
    return length;
  }

  public char charAt(int index) {
    return chars[offset + index];
  }

  public CharSequence subSequence(int start, int end) {
    return new CharsRef(chars, offset + start, offset + end - 1);
  }
  
  /** @deprecated */
  @Deprecated
  private final static Comparator<CharsRef> utf16SortedAsUTF8SortOrder = new UTF16SortedAsUTF8Comparator();
  
  /** @deprecated This comparator is only a transition mechanism */
  @Deprecated
  public static Comparator<CharsRef> getUTF16SortedAsUTF8Comparator() {
    return utf16SortedAsUTF8SortOrder;
  }
  
  /** @deprecated */
  @Deprecated
  private static class UTF16SortedAsUTF8Comparator implements Comparator<CharsRef> {
    // Only singleton
    private UTF16SortedAsUTF8Comparator() {};

    public int compare(CharsRef a, CharsRef b) {
      if (a == b)
        return 0;

      final char[] aChars = a.chars;
      int aUpto = a.offset;
      final char[] bChars = b.chars;
      int bUpto = b.offset;

      final int aStop = aUpto + Math.min(a.length, b.length);

      while (aUpto < aStop) {
        char aChar = aChars[aUpto++];
        char bChar = bChars[bUpto++];
        if (aChar != bChar) {
          // http://icu-project.org/docs/papers/utf16_code_point_order.html
          
          /* aChar != bChar, fix up each one if they're both in or above the surrogate range, then compare them */
          if (aChar >= 0xd800 && bChar >= 0xd800) {
            if (aChar >= 0xe000) {
              aChar -= 0x800;
            } else {
              aChar += 0x2000;
            }
            
            if (bChar >= 0xe000) {
              bChar -= 0x800;
            } else {
              bChar += 0x2000;
            }
          }
          
          /* now aChar and bChar are in code point order */
          return (int)aChar - (int)bChar; /* int must be 32 bits wide */
        }
      }

      // One is a prefix of the other, or, they are equal:
      return a.length - b.length;
    }
  }
  
  /**
   * Creates a new CharsRef that points to a copy of the chars from 
   * <code>other</code>
   * <p>
   * The returned CharsRef will have a length of other.length
   * and an offset of zero.
   */
  public static CharsRef deepCopyOf(CharsRef other) {
    CharsRef clone = new CharsRef();
    clone.copyChars(other);
    return clone;
  }
}