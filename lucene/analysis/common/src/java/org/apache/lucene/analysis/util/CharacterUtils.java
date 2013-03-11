package org.apache.lucene.analysis.util;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.util.Version;

/**
 * {@link CharacterUtils} provides a unified interface to Character-related
 * operations to implement backwards compatible character operations based on a
 * {@link Version} instance.
 * 
 * @lucene.internal
 */
public abstract class CharacterUtils {
  private static final Java4CharacterUtils JAVA_4 = new Java4CharacterUtils();
  private static final Java5CharacterUtils JAVA_5 = new Java5CharacterUtils();

  /**
   * Returns a {@link CharacterUtils} implementation according to the given
   * {@link Version} instance.
   * 
   * @param matchVersion
   *          a version instance
   * @return a {@link CharacterUtils} implementation according to the given
   *         {@link Version} instance.
   */
  public static CharacterUtils getInstance(final Version matchVersion) {
    return matchVersion.onOrAfter(Version.LUCENE_31) ? JAVA_5 : JAVA_4;
  }

  /**
   * Returns the code point at the given index of the char array.
   * Depending on the {@link Version} passed to
   * {@link CharacterUtils#getInstance(Version)} this method mimics the behavior
   * of {@link Character#codePointAt(char[], int)} as it would have been
   * available on a Java 1.4 JVM or on a later virtual machine version.
   * 
   * @param chars
   *          a character array
   * @param offset
   *          the offset to the char values in the chars array to be converted
   * 
   * @return the Unicode code point at the given index
   * @throws NullPointerException
   *           - if the array is null.
   * @throws IndexOutOfBoundsException
   *           - if the value offset is negative or not less than the length of
   *           the char array.
   */
  public abstract int codePointAt(final char[] chars, final int offset);

  /**
   * Returns the code point at the given index of the {@link CharSequence}.
   * Depending on the {@link Version} passed to
   * {@link CharacterUtils#getInstance(Version)} this method mimics the behavior
   * of {@link Character#codePointAt(char[], int)} as it would have been
   * available on a Java 1.4 JVM or on a later virtual machine version.
   * 
   * @param seq
   *          a character sequence
   * @param offset
   *          the offset to the char values in the chars array to be converted
   * 
   * @return the Unicode code point at the given index
   * @throws NullPointerException
   *           - if the sequence is null.
   * @throws IndexOutOfBoundsException
   *           - if the value offset is negative or not less than the length of
   *           the character sequence.
   */
  public abstract int codePointAt(final CharSequence seq, final int offset);
  
  /**
   * Returns the code point at the given index of the char array where only elements
   * with index less than the limit are used.
   * Depending on the {@link Version} passed to
   * {@link CharacterUtils#getInstance(Version)} this method mimics the behavior
   * of {@link Character#codePointAt(char[], int)} as it would have been
   * available on a Java 1.4 JVM or on a later virtual machine version.
   * 
   * @param chars
   *          a character array
   * @param offset
   *          the offset to the char values in the chars array to be converted
   * @param limit the index afer the last element that should be used to calculate
   *        codepoint.  
   * 
   * @return the Unicode code point at the given index
   * @throws NullPointerException
   *           - if the array is null.
   * @throws IndexOutOfBoundsException
   *           - if the value offset is negative or not less than the length of
   *           the char array.
   */
  public abstract int codePointAt(final char[] chars, final int offset, final int limit);
  
  /**
   * Creates a new {@link CharacterBuffer} and allocates a <code>char[]</code>
   * of the given bufferSize.
   * 
   * @param bufferSize
   *          the internal char buffer size, must be <code>&gt;= 2</code>
   * @return a new {@link CharacterBuffer} instance.
   */
  public static CharacterBuffer newCharacterBuffer(final int bufferSize) {
    if (bufferSize < 2) {
      throw new IllegalArgumentException("buffersize must be >= 2");
    }
    return new CharacterBuffer(new char[bufferSize], 0, 0);
  }
  
  
  /**
   * Converts each unicode codepoint to lowerCase via {@link Character#toLowerCase(int)} starting 
   * at the given offset.
   * @param buffer the char buffer to lowercase
   * @param offset the offset to start at
   * @param limit the max char in the buffer to lower case
   */
  public void toLowerCase(final char[] buffer, final int offset, final int limit) {
    assert buffer.length >= limit;
    assert offset <=0 && offset <= buffer.length;
    for (int i = offset; i < limit;) {
      i += Character.toChars(
              Character.toLowerCase(
                  codePointAt(buffer, i)), buffer, i);
     }
  }
  
  /**
   * Fills the {@link CharacterBuffer} with characters read from the given
   * reader {@link Reader}. This method tries to read as many characters into
   * the {@link CharacterBuffer} as possible, each call to fill will start
   * filling the buffer from offset <code>0</code> up to the length of the size
   * of the internal character array.
   * <p>
   * Depending on the {@link Version} passed to
   * {@link CharacterUtils#getInstance(Version)} this method implements
   * supplementary character awareness when filling the given buffer. For all
   * {@link Version} &gt; 3.0 {@link #fill(CharacterBuffer, Reader)} guarantees
   * that the given {@link CharacterBuffer} will never contain a high surrogate
   * character as the last element in the buffer unless it is the last available
   * character in the reader. In other words, high and low surrogate pairs will
   * always be preserved across buffer boarders.
   * </p>
   * 
   * @param buffer
   *          the buffer to fill.
   * @param reader
   *          the reader to read characters from.
   * @return <code>true</code> if and only if no more characters are available
   *         in the reader, otherwise <code>false</code>.
   * @throws IOException
   *           if the reader throws an {@link IOException}.
   */
  public abstract boolean fill(CharacterBuffer buffer, Reader reader) throws IOException;

  private static final class Java5CharacterUtils extends CharacterUtils {
    Java5CharacterUtils() {
    }

    @Override
    public int codePointAt(final char[] chars, final int offset) {
      return Character.codePointAt(chars, offset);
    }

    @Override
    public int codePointAt(final CharSequence seq, final int offset) {
      return Character.codePointAt(seq, offset);
    }

    @Override
    public int codePointAt(final char[] chars, final int offset, final int limit) {
     return Character.codePointAt(chars, offset, limit);
    }

    @Override
    public boolean fill(final CharacterBuffer buffer, final Reader reader) throws IOException {
      final char[] charBuffer = buffer.buffer;
      buffer.offset = 0;
      final int offset;

      // Install the previously saved ending high surrogate:
      if (buffer.lastTrailingHighSurrogate != 0) {
        charBuffer[0] = buffer.lastTrailingHighSurrogate;
        offset = 1;
      } else {
        offset = 0;
      }

      final int read = reader.read(charBuffer,
                                   offset,
                                   charBuffer.length - offset);
      if (read == -1) {
        buffer.length = offset;
        buffer.lastTrailingHighSurrogate = 0;
        return offset != 0;
      }
      assert read > 0;
      buffer.length = read + offset;

      // If we read only a single char, and that char was a
      // high surrogate, read again:
      if (buffer.length == 1
          && Character.isHighSurrogate(charBuffer[buffer.length - 1])) {
        final int read2 = reader.read(charBuffer,
                                      1,
                                      charBuffer.length - 1);
        if (read2 == -1) {
          // NOTE: mal-formed input (ended on a high
          // surrogate)!  Consumer must deal with it...
          return true;
        }
        assert read2 > 0;

        buffer.length += read2;
      }

      if (buffer.length > 1
          && Character.isHighSurrogate(charBuffer[buffer.length - 1])) {
        buffer.lastTrailingHighSurrogate = charBuffer[--buffer.length];
      } else {
        buffer.lastTrailingHighSurrogate = 0;
      }

      return true;
    }
  }

  private static final class Java4CharacterUtils extends CharacterUtils {
    Java4CharacterUtils() {
    }

    @Override
    public int codePointAt(final char[] chars, final int offset) {
      return chars[offset];
    }

    @Override
    public int codePointAt(final CharSequence seq, final int offset) {
      return seq.charAt(offset);
    }

    @Override
    public int codePointAt(final char[] chars, final int offset, final int limit) {
      if(offset >= limit)
        throw new IndexOutOfBoundsException("offset must be less than limit");
      return chars[offset];
    }

    @Override
    public boolean fill(final CharacterBuffer buffer, final Reader reader) throws IOException {
      buffer.offset = 0;
      final int read = reader.read(buffer.buffer);
      if(read == -1)
        return false;
      buffer.length = read;
      return true;
    }

  }
  
  /**
   * A simple IO buffer to use with
   * {@link CharacterUtils#fill(CharacterBuffer, Reader)}.
   */
  public static final class CharacterBuffer {
    
    private final char[] buffer;
    private int offset;
    private int length;
    // NOTE: not private so outer class can access without
    // $access methods:
    char lastTrailingHighSurrogate;
    
    CharacterBuffer(char[] buffer, int offset, int length) {
      this.buffer = buffer;
      this.offset = offset;
      this.length = length;
    }
    
    /**
     * Returns the internal buffer
     * 
     * @return the buffer
     */
    public char[] getBuffer() {
      return buffer;
    }
    
    /**
     * Returns the data offset in the internal buffer.
     * 
     * @return the offset
     */
    public int getOffset() {
      return offset;
    }
    
    /**
     * Return the length of the data in the internal buffer starting at
     * {@link #getOffset()}
     * 
     * @return the length
     */
    public int getLength() {
      return length;
    }
    
    /**
     * Resets the CharacterBuffer. All internals are reset to its default
     * values.
     */
    public void reset() {
      offset = 0;
      length = 0;
      lastTrailingHighSurrogate = 0;
    }
  }

}
