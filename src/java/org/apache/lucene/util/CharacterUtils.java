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
 * {@link CharacterUtils} provides a unified interface to Character-related
 * operations to implement backwards compatible character operations based on a
 * {@link Version} instance.
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
  public static CharacterUtils getInstance(Version matchVersion) {
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
  public abstract int codePointAt(char[] chars, int offset);

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
  public abstract int codePointAt(CharSequence seq, int offset);

  private static final class Java5CharacterUtils extends CharacterUtils {
    Java5CharacterUtils() {
    };

    @Override
    public final int codePointAt(char[] chars, int offset) {
      return Character.codePointAt(chars, offset);
    }

    @Override
    public int codePointAt(CharSequence seq, int offset) {
      return Character.codePointAt(seq, offset);
    }
  }

  private static final class Java4CharacterUtils extends CharacterUtils {
    Java4CharacterUtils() {
    };

    @Override
    public final int codePointAt(char[] chars, int offset) {
      return chars[offset];
    }

    @Override
    public int codePointAt(CharSequence seq, int offset) {
      return seq.charAt(offset);
    }
  }

}
