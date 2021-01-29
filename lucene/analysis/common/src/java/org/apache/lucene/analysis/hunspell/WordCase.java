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
  UPPER,
  TITLE,
  LOWER,
  MIXED;

  static WordCase caseOf(char[] word, int length) {
    boolean startsWithLower = Character.isLowerCase(word[0]);

    boolean seenUpper = false;
    boolean seenLower = false;
    for (int i = 1; i < length; i++) {
      CharCase cc = charCase(word[i]);
      seenUpper = seenUpper || cc == CharCase.UPPER;
      seenLower = seenLower || cc == CharCase.LOWER;
      if (seenUpper && seenLower) break;
    }

    return get(startsWithLower, seenUpper, seenLower);
  }

  static WordCase caseOf(CharSequence word, int length) {
    boolean startsWithLower = Character.isLowerCase(word.charAt(0));

    boolean seenUpper = false;
    boolean seenLower = false;
    for (int i = 1; i < length; i++) {
      CharCase cc = charCase(word.charAt(i));
      seenUpper = seenUpper || cc == CharCase.UPPER;
      seenLower = seenLower || cc == CharCase.LOWER;
      if (seenUpper && seenLower) break;
    }

    return get(startsWithLower, seenUpper, seenLower);
  }

  private static WordCase get(boolean startsWithLower, boolean seenUpper, boolean seenLower) {
    if (!startsWithLower) {
      return !seenLower ? UPPER : !seenUpper ? TITLE : MIXED;
    }
    return seenUpper ? MIXED : LOWER;
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
