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
package org.apache.lucene.analysis.hunspell;

enum WordCase {
  /** e.g. WORD */
  UPPER,

  /** e.g. Word */
  TITLE,

  /** e.g. word */
  LOWER,

  /** e.g. WoRd or wOrd */
  MIXED,

  /** e.g "-" or "/" or "42" */
  NEUTRAL;

  static WordCase caseOf(char[] word, int length) {
    CharCase startCase = charCase(word[0]);

    boolean seenUpper = false;
    boolean seenLower = false;
    for (int i = 1; i < length; i++) {
      CharCase cc = charCase(word[i]);
      seenUpper = seenUpper || cc == CharCase.UPPER;
      seenLower = seenLower || cc == CharCase.LOWER;
      if (seenUpper && seenLower) break;
    }

    return get(startCase, seenUpper, seenLower);
  }

  static WordCase caseOf(CharSequence word) {
    return caseOf(word, word.length());
  }

  static WordCase caseOf(CharSequence word, int length) {
    CharCase startCase = charCase(word.charAt(0));

    boolean seenUpper = false;
    boolean seenLower = false;
    for (int i = 1; i < length; i++) {
      CharCase cc = charCase(word.charAt(i));
      seenUpper = seenUpper || cc == CharCase.UPPER;
      seenLower = seenLower || cc == CharCase.LOWER;
      if (seenUpper && seenLower) break;
    }

    return get(startCase, seenUpper, seenLower);
  }

  private static WordCase get(CharCase startCase, boolean seenUpper, boolean seenLower) {
    if (seenLower && seenUpper) return MIXED;
    switch (startCase) {
      case LOWER:
        return seenUpper ? MIXED : LOWER;
      case UPPER:
        return !seenLower ? UPPER : TITLE;
      case NEUTRAL:
      default:
        return seenLower ? LOWER : seenUpper ? UPPER : NEUTRAL;
    }
  }

  private static CharCase charCase(char c) {
    if (Character.isUpperCase(c)) {
      return CharCase.UPPER;
    }
    if (Character.isLowerCase(c) && Character.toUpperCase(c) != c) {
      return CharCase.LOWER;
    }
    return CharCase.NEUTRAL;
  }

  private enum CharCase {
    UPPER,
    LOWER,
    NEUTRAL
  }
}
