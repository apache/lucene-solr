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
import org.apache.lucene.util.IntsRef;

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

  boolean prohibitsCompounding(
      CharsRef word, int breakPos, CharsRef stemBefore, CharsRef stemAfter) {
    if (isNonAffixedPattern(endChars)) {
      if (!charsMatch(word, breakPos - stemBefore.length, stemBefore)) {
        return false;
      }
    } else if (!charsMatch(word, breakPos - endChars.length(), endChars)) {
      return false;
    }

    if (isNonAffixedPattern(beginChars)) {
      if (!charsMatch(word, breakPos, stemAfter)) {
        return false;
      }
    } else if (!charsMatch(word, breakPos, beginChars)) {
      return false;
    }

    if (endFlags.length > 0 && !stemHasFlags(stemBefore, endFlags)) {
      return false;
    }
    //noinspection RedundantIfStatement
    if (beginFlags.length > 0 && !stemHasFlags(stemAfter, beginFlags)) {
      return false;
    }

    return true;
  }

  private static boolean isNonAffixedPattern(String pattern) {
    return pattern.length() == 1 && pattern.charAt(0) == '0';
  }

  private boolean stemHasFlags(CharsRef stem, char[] flags) {
    IntsRef forms = dictionary.lookupWord(stem.chars, stem.offset, stem.length);
    return forms != null && hasAllFlags(flags, forms);
  }

  private boolean hasAllFlags(char[] flags, IntsRef forms) {
    for (char flag : flags) {
      if (!dictionary.hasFlag(forms, flag)) {
        return false;
      }
    }
    return true;
  }

  CharsRef expandReplacement(CharsRef word, int breakPos) {
    if (replacement != null && charsMatch(word, breakPos, replacement)) {
      return new CharsRef(
          word.subSequence(0, breakPos)
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
