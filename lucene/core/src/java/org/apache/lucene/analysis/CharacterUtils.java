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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.Reader;

/**
 * Utility class to write tokenizers or token filters.
 *
 * @lucene.internal
 */
public final class CharacterUtils {

  private CharacterUtils() {} // no instantiation

  /**
   * Creates a new {@link CharacterBuffer} and allocates a <code>char[]</code> of the given
   * bufferSize.
   *
   * @param bufferSize the internal char buffer size, must be <code>&gt;= 2</code>
   * @return a new {@link CharacterBuffer} instance.
   */
  public static CharacterBuffer newCharacterBuffer(final int bufferSize) {
    if (bufferSize < 2) {
      throw new IllegalArgumentException("buffersize must be >= 2");
    }
    return new CharacterBuffer(new char[bufferSize], 0, 0);
  }

  /**
   * Converts each unicode codepoint to lowerCase via {@link Character#toLowerCase(int)} starting at
   * the given offset.
   *
   * @param buffer the char buffer to lowercase
   * @param offset the offset to start at
   * @param limit the max char in the buffer to lower case
   */
  public static void toLowerCase(final char[] buffer, final int offset, final int limit) {
    assert buffer.length >= limit;
    assert 0 <= offset && offset <= buffer.length;
    for (int i = offset; i < limit; ) {
      i +=
          Character.toChars(
              Character.toLowerCase(Character.codePointAt(buffer, i, limit)), buffer, i);
    }
  }

  /**
   * Converts each unicode codepoint to UpperCase via {@link Character#toUpperCase(int)} starting at
   * the given offset.
   *
   * @param buffer the char buffer to UPPERCASE
   * @param offset the offset to start at
   * @param limit the max char in the buffer to lower case
   */
  public static void toUpperCase(final char[] buffer, final int offset, final int limit) {
    assert buffer.length >= limit;
    assert 0 <= offset && offset <= buffer.length;
    for (int i = offset; i < limit; ) {
      i +=
          Character.toChars(
              Character.toUpperCase(Character.codePointAt(buffer, i, limit)), buffer, i);
    }
  }

  /**
   * Converts a sequence of Java characters to a sequence of unicode code points.
   *
   * @return the number of code points written to the destination buffer
   */
  public static int toCodePoints(char[] src, int srcOff, int srcLen, int[] dest, int destOff) {
    if (srcLen < 0) {
      throw new IllegalArgumentException("srcLen must be >= 0");
    }
    int codePointCount = 0;
    for (int i = 0; i < srcLen; ) {
      final int cp = Character.codePointAt(src, srcOff + i, srcOff + srcLen);
      final int charCount = Character.charCount(cp);
      dest[destOff + codePointCount++] = cp;
      i += charCount;
    }
    return codePointCount;
  }

  /**
   * Converts a sequence of unicode code points to a sequence of Java characters.
   *
   * @return the number of chars written to the destination buffer
   */
  public static int toChars(int[] src, int srcOff, int srcLen, char[] dest, int destOff) {
    if (srcLen < 0) {
      throw new IllegalArgumentException("srcLen must be >= 0");
    }
    int written = 0;
    for (int i = 0; i < srcLen; ++i) {
      written += Character.toChars(src[srcOff + i], dest, destOff + written);
    }
    return written;
  }

  /**
   * Fills the {@link CharacterBuffer} with characters read from the given reader {@link Reader}.
   * This method tries to read <code>numChars</code> characters into the {@link CharacterBuffer},
   * each call to fill will start filling the buffer from offset <code>0</code> up to <code>numChars
   * </code>. In case code points can span across 2 java characters, this method may only fill
   * <code>numChars - 1</code> characters in order not to split in the middle of a surrogate pair,
   * even if there are remaining characters in the {@link Reader}.
   *
   * <p>This method guarantees that the given {@link CharacterBuffer} will never contain a high
   * surrogate character as the last element in the buffer unless it is the last available character
   * in the reader. In other words, high and low surrogate pairs will always be preserved across
   * buffer boarders.
   *
   * <p>A return value of <code>false</code> means that this method call exhausted the reader, but
   * there may be some bytes which have been read, which can be verified by checking whether <code>
   * buffer.getLength() &gt; 0</code>.
   *
   * @param buffer the buffer to fill.
   * @param reader the reader to read characters from.
   * @param numChars the number of chars to read
   * @return <code>false</code> if and only if reader.read returned -1 while trying to fill the
   *     buffer
   * @throws IOException if the reader throws an {@link IOException}.
   */
  public static boolean fill(CharacterBuffer buffer, Reader reader, int numChars)
      throws IOException {
    assert buffer.buffer.length >= 2;
    if (numChars < 2 || numChars > buffer.buffer.length) {
      throw new IllegalArgumentException("numChars must be >= 2 and <= the buffer size");
    }
    final char[] charBuffer = buffer.buffer;
    buffer.offset = 0;
    final int offset;

    // Install the previously saved ending high surrogate:
    if (buffer.lastTrailingHighSurrogate != 0) {
      charBuffer[0] = buffer.lastTrailingHighSurrogate;
      buffer.lastTrailingHighSurrogate = 0;
      offset = 1;
    } else {
      offset = 0;
    }

    final int read = readFully(reader, charBuffer, offset, numChars - offset);

    buffer.length = offset + read;
    final boolean result = buffer.length == numChars;
    if (buffer.length < numChars) {
      // We failed to fill the buffer. Even if the last char is a high
      // surrogate, there is nothing we can do
      return result;
    }

    if (Character.isHighSurrogate(charBuffer[buffer.length - 1])) {
      buffer.lastTrailingHighSurrogate = charBuffer[--buffer.length];
    }
    return result;
  }

  /** Convenience method which calls <code>fill(buffer, reader, buffer.buffer.length)</code>. */
  public static boolean fill(CharacterBuffer buffer, Reader reader) throws IOException {
    return fill(buffer, reader, buffer.buffer.length);
  }

  static int readFully(Reader reader, char[] dest, int offset, int len) throws IOException {
    int read = 0;
    while (read < len) {
      final int r = reader.read(dest, offset + read, len - read);
      if (r == -1) {
        break;
      }
      read += r;
    }
    return read;
  }

  /** A simple IO buffer to use with {@link CharacterUtils#fill(CharacterBuffer, Reader)}. */
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
     * Return the length of the data in the internal buffer starting at {@link #getOffset()}
     *
     * @return the length
     */
    public int getLength() {
      return length;
    }

    /** Resets the CharacterBuffer. All internals are reset to its default values. */
    public void reset() {
      offset = 0;
      length = 0;
      lastTrailingHighSurrogate = 0;
    }
  }
}
