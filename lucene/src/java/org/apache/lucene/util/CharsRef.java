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

/**
 * Represents char[], as a slice (offset + length) into an existing char[].
 * 
 * @lucene.internal
 */
public final class CharsRef implements Comparable<CharsRef>, CharSequence {
  private static final char[] EMPTY_ARRAY = new char[0];
  public char[] chars;
  public int offset;
  public int length;

  /**
   * Creates a new {@link CharsRef} initialized an empty array zero-length
   */
  public CharsRef() {
    this(EMPTY_ARRAY, 0, 0);
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

  /**
   * Creates a new {@link CharsRef} and copies the contents of the source into
   * the new instance.
   * @see #copy(CharsRef)
   */
  public CharsRef(CharsRef other) {
    copy(other);
  }

  @Override
  public Object clone() {
    return new CharsRef(this);
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
    if (this == other) {
      return true;
    }

    if (other instanceof CharsRef) {
      return charsEquals((CharsRef) other);
    }

    if (other instanceof CharSequence) {
      final CharSequence seq = (CharSequence) other;
      if (length == seq.length()) {
        int n = length;
        int i = offset;
        int j = 0;
        while (n-- != 0) {
          if (chars[i++] != seq.charAt(j++))
            return false;
        }
        return true;
      }
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
  public void copy(CharsRef other) {
    chars = ArrayUtil.grow(chars, other.length);
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
  public void copy(char[] otherChars, int otherOffset, int otherLength) {
    this.offset = 0;
    append(otherChars, otherOffset, otherLength);
  }

  /**
   * Appends the given array to this CharsRef starting at the current offset
   */
  public void append(char[] otherChars, int otherOffset, int otherLength) {
    grow(this.offset + otherLength);
    System.arraycopy(otherChars, otherOffset, this.chars, this.offset,
        otherLength);
    this.length = otherLength;
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
}