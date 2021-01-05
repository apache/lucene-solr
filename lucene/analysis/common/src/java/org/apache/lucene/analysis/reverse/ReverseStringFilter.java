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
package org.apache.lucene.analysis.reverse;

import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Reverse token string, for example "country" =&gt; "yrtnuoc".
 *
 * <p>If <code>marker</code> is supplied, then tokens will be also prepended by that character. For
 * example, with a marker of &#x5C;u0001, "country" =&gt; "&#x5C;u0001yrtnuoc". This is useful when
 * implementing efficient leading wildcards search.
 */
public final class ReverseStringFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final char marker;
  private static final char NOMARKER = '\uFFFF';

  /** Example marker character: U+0001 (START OF HEADING) */
  public static final char START_OF_HEADING_MARKER = '\u0001';

  /** Example marker character: U+001F (INFORMATION SEPARATOR ONE) */
  public static final char INFORMATION_SEPARATOR_MARKER = '\u001F';

  /** Example marker character: U+EC00 (PRIVATE USE AREA: EC00) */
  public static final char PUA_EC00_MARKER = '\uEC00';

  /** Example marker character: U+200F (RIGHT-TO-LEFT MARK) */
  public static final char RTL_DIRECTION_MARKER = '\u200F';

  /**
   * Create a new ReverseStringFilter that reverses all tokens in the supplied {@link TokenStream}.
   *
   * <p>The reversed tokens will not be marked.
   *
   * @param in {@link TokenStream} to filter
   */
  public ReverseStringFilter(TokenStream in) {
    this(in, NOMARKER);
  }

  /**
   * Create a new ReverseStringFilter that reverses and marks all tokens in the supplied {@link
   * TokenStream}.
   *
   * <p>The reversed tokens will be prepended (marked) by the <code>marker</code> character.
   *
   * @param in {@link TokenStream} to filter
   * @param marker A character used to mark reversed tokens
   */
  public ReverseStringFilter(TokenStream in, char marker) {
    super(in);
    this.marker = marker;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      int len = termAtt.length();
      if (marker != NOMARKER) {
        len++;
        termAtt.resizeBuffer(len);
        termAtt.buffer()[len - 1] = marker;
      }
      reverse(termAtt.buffer(), 0, len);
      termAtt.setLength(len);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Reverses the given input string
   *
   * @param input the string to reverse
   * @return the given input string in reversed order
   */
  public static String reverse(final String input) {
    final char[] charInput = input.toCharArray();
    reverse(charInput, 0, charInput.length);
    return new String(charInput);
  }

  /**
   * Reverses the given input buffer in-place
   *
   * @param buffer the input char array to reverse
   */
  public static void reverse(final char[] buffer) {
    reverse(buffer, 0, buffer.length);
  }

  /**
   * Partially reverses the given input buffer in-place from offset 0 up to the given length.
   *
   * @param buffer the input char array to reverse
   * @param len the length in the buffer up to where the buffer should be reversed
   */
  public static void reverse(final char[] buffer, final int len) {
    reverse(buffer, 0, len);
  }

  /**
   * Partially reverses the given input buffer in-place from the given offset up to the given
   * length.
   *
   * @param buffer the input char array to reverse
   * @param start the offset from where to reverse the buffer
   * @param len the length in the buffer up to where the buffer should be reversed
   */
  public static void reverse(final char[] buffer, final int start, final int len) {
    /* modified version of Apache Harmony AbstractStringBuilder reverse0() */
    if (len < 2) return;
    int end = (start + len) - 1;
    char frontHigh = buffer[start];
    char endLow = buffer[end];
    boolean allowFrontSur = true, allowEndSur = true;
    final int mid = start + (len >> 1);
    for (int i = start; i < mid; ++i, --end) {
      final char frontLow = buffer[i + 1];
      final char endHigh = buffer[end - 1];
      final boolean surAtFront = allowFrontSur && Character.isSurrogatePair(frontHigh, frontLow);
      if (surAtFront && (len < 3)) {
        // nothing to do since surAtFront is allowed and 1 char left
        return;
      }
      final boolean surAtEnd = allowEndSur && Character.isSurrogatePair(endHigh, endLow);
      allowFrontSur = allowEndSur = true;
      if (surAtFront == surAtEnd) {
        if (surAtFront) {
          // both surrogates
          buffer[end] = frontLow;
          buffer[--end] = frontHigh;
          buffer[i] = endHigh;
          buffer[++i] = endLow;
          frontHigh = buffer[i + 1];
          endLow = buffer[end - 1];
        } else {
          // neither surrogates
          buffer[end] = frontHigh;
          buffer[i] = endLow;
          frontHigh = frontLow;
          endLow = endHigh;
        }
      } else {
        if (surAtFront) {
          // surrogate only at the front
          buffer[end] = frontLow;
          buffer[i] = endLow;
          endLow = endHigh;
          allowFrontSur = false;
        } else {
          // surrogate only at the end
          buffer[end] = frontHigh;
          buffer[i] = endHigh;
          frontHigh = frontLow;
          allowEndSur = false;
        }
      }
    }
    if ((len & 0x01) == 1 && !(allowFrontSur && allowEndSur)) {
      // only if odd length
      buffer[end] = allowFrontSur ? endLow : frontHigh;
    }
  }
}
