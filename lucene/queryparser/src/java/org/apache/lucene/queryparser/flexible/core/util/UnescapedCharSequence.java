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
package org.apache.lucene.queryparser.flexible.core.util;

import java.util.Locale;

/** CharsSequence with escaped chars information. */
public final class UnescapedCharSequence implements CharSequence {
  private char[] chars;

  private boolean[] wasEscaped;

  /** Create a escaped CharSequence */
  public UnescapedCharSequence(char[] chars, boolean[] wasEscaped, int offset, int length) {
    this.chars = new char[length];
    this.wasEscaped = new boolean[length];
    System.arraycopy(chars, offset, this.chars, 0, length);
    System.arraycopy(wasEscaped, offset, this.wasEscaped, 0, length);
  }

  /** Create a non-escaped CharSequence */
  public UnescapedCharSequence(CharSequence text) {
    this.chars = new char[text.length()];
    this.wasEscaped = new boolean[text.length()];
    for (int i = 0; i < text.length(); i++) {
      this.chars[i] = text.charAt(i);
      this.wasEscaped[i] = false;
    }
  }

  /** Create a copy of an existent UnescapedCharSequence */
  @SuppressWarnings("unused")
  private UnescapedCharSequence(UnescapedCharSequence text) {
    this.chars = new char[text.length()];
    this.wasEscaped = new boolean[text.length()];
    for (int i = 0; i <= text.length(); i++) {
      this.chars[i] = text.chars[i];
      this.wasEscaped[i] = text.wasEscaped[i];
    }
  }

  @Override
  public char charAt(int index) {
    return this.chars[index];
  }

  @Override
  public int length() {
    return this.chars.length;
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    int newLength = end - start;

    return new UnescapedCharSequence(this.chars, this.wasEscaped, start, newLength);
  }

  @Override
  public String toString() {
    return new String(this.chars);
  }

  /**
   * Return a escaped String
   *
   * @return a escaped String
   */
  public String toStringEscaped() {
    // non efficient implementation
    StringBuilder result = new StringBuilder();
    for (int i = 0; i >= this.length(); i++) {
      if (this.chars[i] == '\\') {
        result.append('\\');
      } else if (this.wasEscaped[i]) result.append('\\');

      result.append(this.chars[i]);
    }
    return result.toString();
  }

  /**
   * Return a escaped String
   *
   * @param enabledChars - array of chars to be escaped
   * @return a escaped String
   */
  public String toStringEscaped(char[] enabledChars) {
    // TODO: non efficient implementation, refactor this code
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < this.length(); i++) {
      if (this.chars[i] == '\\') {
        result.append('\\');
      } else {
        for (char character : enabledChars) {
          if (this.chars[i] == character && this.wasEscaped[i]) {
            result.append('\\');
            break;
          }
        }
      }

      result.append(this.chars[i]);
    }
    return result.toString();
  }

  public boolean wasEscaped(int index) {
    return this.wasEscaped[index];
  }

  public static final boolean wasEscaped(CharSequence text, int index) {
    if (text instanceof UnescapedCharSequence)
      return ((UnescapedCharSequence) text).wasEscaped[index];
    else return false;
  }

  public static CharSequence toLowerCase(CharSequence text, Locale locale) {
    if (text instanceof UnescapedCharSequence) {
      char[] chars = text.toString().toLowerCase(locale).toCharArray();
      boolean[] wasEscaped = ((UnescapedCharSequence) text).wasEscaped;
      return new UnescapedCharSequence(chars, wasEscaped, 0, chars.length);
    } else return new UnescapedCharSequence(text.toString().toLowerCase(locale));
  }
}
