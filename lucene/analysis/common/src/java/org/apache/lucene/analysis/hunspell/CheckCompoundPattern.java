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

import org.apache.lucene.util.CharsRef;

class CheckCompoundPattern {
  private final String endChars;
  private final String beginChars;
  private final String replacement;
  private final char[] endFlags;
  private final char[] beginFlags;
  private final Dictionary dictionary;

  CheckCompoundPattern(
      String unparsed, Dictionary.FlagParsingStrategy strategy, Dictionary dictionary) {
    this.dictionary = dictionary;
    String[] parts = unparsed.split("\\s+");
    if (parts.length < 3) {
      throw new IllegalArgumentException("Invalid pattern: " + unparsed);
    }

    int flagSep = parts[1].indexOf("/");
    endChars = flagSep < 0 ? parts[1] : parts[1].substring(0, flagSep);
    endFlags = flagSep < 0 ? new char[0] : strategy.parseFlags(parts[1].substring(flagSep + 1));

    flagSep = parts[2].indexOf("/");
    beginChars = flagSep < 0 ? parts[2] : parts[2].substring(0, flagSep);
    beginFlags = flagSep < 0 ? new char[0] : strategy.parseFlags(parts[2].substring(flagSep + 1));

    replacement = parts.length == 3 ? null : parts[3];
  }

  @Override
  public String toString() {
    return endChars + " " + beginChars + (replacement == null ? "" : " -> " + replacement);
  }

  boolean prohibitsCompounding(CharsRef word, int breakPos, Root<?> rootBefore, Root<?> rootAfter) {
    if (isNonAffixedPattern(endChars)) {
      if (!charsMatch(word, breakPos - rootBefore.word.length(), rootBefore.word)) {
        return false;
      }
    } else if (!charsMatch(word, breakPos - endChars.length(), endChars)) {
      return false;
    }

    if (isNonAffixedPattern(beginChars)) {
      if (!charsMatch(word, breakPos, rootAfter.word)) {
        return false;
      }
    } else if (!charsMatch(word, breakPos, beginChars)) {
      return false;
    }

    if (endFlags.length > 0 && !hasAllFlags(rootBefore, endFlags)) {
      return false;
    }
    //noinspection RedundantIfStatement
    if (beginFlags.length > 0 && !hasAllFlags(rootAfter, beginFlags)) {
      return false;
    }

    return true;
  }

  private static boolean isNonAffixedPattern(String pattern) {
    return pattern.length() == 1 && pattern.charAt(0) == '0';
  }

  private boolean hasAllFlags(Root<?> root, char[] flags) {
    for (char flag : flags) {
      if (!dictionary.hasFlag(root.entryId, flag)) {
        return false;
      }
    }
    return true;
  }

  CharsRef expandReplacement(CharsRef word, int breakPos) {
    if (replacement != null && charsMatch(word, breakPos, replacement)) {
      return new CharsRef(
          new String(word.chars, 0, word.offset + breakPos)
              + endChars
              + beginChars
              + word.subSequence(breakPos + replacement.length(), word.length));
    }
    return null;
  }

  int endLength() {
    return endChars.length();
  }

  private static boolean charsMatch(CharsRef word, int offset, CharSequence pattern) {
    int len = pattern.length();
    if (word.length - offset < len || offset < 0 || offset > word.length) {
      return false;
    }

    for (int i = 0; i < len; i++) {
      if (word.chars[word.offset + offset + i] != pattern.charAt(i)) {
        return false;
      }
    }
    return true;
  }
}
