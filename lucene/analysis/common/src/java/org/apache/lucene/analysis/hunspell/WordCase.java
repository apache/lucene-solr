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
    boolean capitalized = Character.isUpperCase(word[0]);

    boolean seenUpper = false;
    boolean seenLower = false;
    for (int i = 1; i < length; i++) {
      char ch = word[i];
      seenUpper = seenUpper || Character.isUpperCase(ch);
      seenLower = seenLower || Character.isLowerCase(ch);
      if (seenUpper && seenLower) break;
    }

    return get(capitalized, seenUpper, seenLower);
  }

  static WordCase caseOf(CharSequence word, int length) {
    boolean capitalized = Character.isUpperCase(word.charAt(0));

    boolean seenUpper = false;
    boolean seenLower = false;
    for (int i = 1; i < length; i++) {
      char ch = word.charAt(i);
      seenUpper = seenUpper || Character.isUpperCase(ch);
      seenLower = seenLower || Character.isLowerCase(ch);
      if (seenUpper && seenLower) break;
    }

    return get(capitalized, seenUpper, seenLower);
  }

  private static WordCase get(boolean capitalized, boolean seenUpper, boolean seenLower) {
    if (capitalized) {
      return !seenLower ? UPPER : !seenUpper ? TITLE : MIXED;
    }
    return seenUpper ? MIXED : LOWER;
  }
}
