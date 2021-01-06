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
package org.apache.lucene.analysis.cn.smart;

import static java.lang.Character.isSurrogate;

import org.apache.lucene.analysis.cn.smart.hhmm.SegTokenFilter; // for javadoc

/**
 * SmartChineseAnalyzer utility constants and methods
 *
 * @lucene.experimental
 */
public class Utility {

  public static final char[] STRING_CHAR_ARRAY = "未##串".toCharArray();

  public static final char[] NUMBER_CHAR_ARRAY = "未##数".toCharArray();

  public static final char[] START_CHAR_ARRAY = "始##始".toCharArray();

  public static final char[] END_CHAR_ARRAY = "末##末".toCharArray();

  /** Delimiters will be filtered to this character by {@link SegTokenFilter} */
  public static final char[] COMMON_DELIMITER = new char[] {','};

  /**
   * Space-like characters that need to be skipped: such as space, tab, newline, carriage return.
   */
  public static final String SPACES = " 　\t\r\n";

  /** Maximum bigram frequency (used in the smoothing function). */
  public static final int MAX_FREQUENCE = 2079997 + 80000;

  /**
   * compare two arrays starting at the specified offsets.
   *
   * @param larray left array
   * @param lstartIndex start offset into larray
   * @param rarray right array
   * @param rstartIndex start offset into rarray
   * @return 0 if the arrays are equal，1 if larray &gt; rarray, -1 if larray &lt; rarray
   */
  public static int compareArray(char[] larray, int lstartIndex, char[] rarray, int rstartIndex) {

    if (larray == null) {
      if (rarray == null || rstartIndex >= rarray.length) return 0;
      else return -1;
    } else {
      // larray != null
      if (rarray == null) {
        if (lstartIndex >= larray.length) return 0;
        else return 1;
      }
    }

    int li = lstartIndex, ri = rstartIndex;
    while (li < larray.length && ri < rarray.length && larray[li] == rarray[ri]) {
      li++;
      ri++;
    }
    if (li == larray.length) {
      if (ri == rarray.length) {
        // Both arrays are equivalent, return 0.
        return 0;
      } else {
        // larray < rarray because larray has ended first.
        return -1;
      }
    } else {
      // differing lengths
      if (ri == rarray.length) {
        // larray > rarray because rarray has ended first.
        return 1;
      } else {
        // determine by comparison
        if (larray[li] > rarray[ri]) return 1;
        else return -1;
      }
    }
  }

  /**
   * Compare two arrays, starting at the specified offsets, but treating shortArray as a prefix to
   * longArray. As long as shortArray is a prefix of longArray, return 0. Otherwise, behave as
   * {@link Utility#compareArray(char[], int, char[], int)}
   *
   * @param shortArray prefix array
   * @param shortIndex offset into shortArray
   * @param longArray long array (word)
   * @param longIndex offset into longArray
   * @return 0 if shortArray is a prefix of longArray, otherwise act as {@link
   *     Utility#compareArray(char[], int, char[], int)}
   */
  public static int compareArrayByPrefix(
      char[] shortArray, int shortIndex, char[] longArray, int longIndex) {

    // a null prefix is a prefix of longArray
    if (shortArray == null) return 0;
    else if (longArray == null) return (shortIndex < shortArray.length) ? 1 : 0;

    int si = shortIndex, li = longIndex;
    while (si < shortArray.length && li < longArray.length && shortArray[si] == longArray[li]) {
      si++;
      li++;
    }
    if (si == shortArray.length) {
      // shortArray is a prefix of longArray
      return 0;
    } else {
      // shortArray > longArray because longArray ended first.
      if (li == longArray.length) return 1;
      else
        // determine by comparison
        return (shortArray[si] > longArray[li]) ? 1 : -1;
    }
  }

  /**
   * Return the internal {@link CharType} constant of a given character.
   *
   * @param ch input character
   * @return constant from {@link CharType} describing the character type.
   * @see CharType
   */
  public static int getCharType(char ch) {
    if (isSurrogate(ch)) return CharType.SURROGATE;
    // Most (but not all!) of these are Han Ideographic Characters
    if (ch >= 0x4E00 && ch <= 0x9FA5) return CharType.HANZI;
    if ((ch >= 0x0041 && ch <= 0x005A) || (ch >= 0x0061 && ch <= 0x007A)) return CharType.LETTER;
    if (ch >= 0x0030 && ch <= 0x0039) return CharType.DIGIT;
    if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '　')
      return CharType.SPACE_LIKE;
    // Punctuation Marks
    if ((ch >= 0x0021 && ch <= 0x00BB)
        || (ch >= 0x2010 && ch <= 0x2642)
        || (ch >= 0x3001 && ch <= 0x301E)) return CharType.DELIMITER;

    // Full-Width range
    if ((ch >= 0xFF21 && ch <= 0xFF3A) || (ch >= 0xFF41 && ch <= 0xFF5A))
      return CharType.FULLWIDTH_LETTER;
    if (ch >= 0xFF10 && ch <= 0xFF19) return CharType.FULLWIDTH_DIGIT;
    if (ch >= 0xFE30 && ch <= 0xFF63) return CharType.DELIMITER;
    return CharType.OTHER;
  }
}
