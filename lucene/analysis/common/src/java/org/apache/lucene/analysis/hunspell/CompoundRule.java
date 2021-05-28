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

import java.util.List;
import org.apache.lucene.util.IntsRef;

class CompoundRule {
  private final char[] data;
  private final Dictionary dictionary;

  CompoundRule(String rule, Dictionary dictionary) {
    this.dictionary = dictionary;
    StringBuilder parsedFlags = new StringBuilder();
    int pos = 0;
    while (pos < rule.length()) {
      int lParen = rule.indexOf("(", pos);
      if (lParen < 0) {
        parsedFlags.append(dictionary.flagParsingStrategy.parseFlags(rule.substring(pos)));
        break;
      }

      parsedFlags.append(dictionary.flagParsingStrategy.parseFlags(rule.substring(pos, lParen)));
      int rParen = rule.indexOf(')', lParen + 1);
      if (rParen < 0) {
        throw new IllegalArgumentException("Unmatched parentheses: " + rule);
      }

      parsedFlags.append(
          dictionary.flagParsingStrategy.parseFlags(rule.substring(lParen + 1, rParen)));
      pos = rParen + 1;
      if (pos < rule.length() && (rule.charAt(pos) == '?' || rule.charAt(pos) == '*')) {
        parsedFlags.append(rule.charAt(pos++));
      }
    }
    data = parsedFlags.toString().toCharArray();
  }

  boolean mayMatch(List<IntsRef> words) {
    return match(words, 0, 0, false);
  }

  boolean fullyMatches(List<IntsRef> words) {
    return match(words, 0, 0, true);
  }

  private boolean match(List<IntsRef> words, int patternIndex, int wordIndex, boolean fully) {
    if (patternIndex >= data.length) {
      return wordIndex >= words.size();
    }
    if (wordIndex >= words.size() && !fully) {
      return true;
    }

    char flag = data[patternIndex];
    if (patternIndex < data.length - 1 && data[patternIndex + 1] == '*') {
      int startWI = wordIndex;
      while (wordIndex < words.size() && dictionary.hasFlag(words.get(wordIndex), flag)) {
        wordIndex++;
      }

      while (wordIndex >= startWI) {
        if (match(words, patternIndex + 2, wordIndex, fully)) {
          return true;
        }

        wordIndex--;
      }
      return false;
    }

    boolean currentWordMatches =
        wordIndex < words.size() && dictionary.hasFlag(words.get(wordIndex), flag);

    if (patternIndex < data.length - 1 && data[patternIndex + 1] == '?') {
      if (currentWordMatches && match(words, patternIndex + 2, wordIndex + 1, fully)) {
        return true;
      }
      return match(words, patternIndex + 2, wordIndex, fully);
    }

    return currentWordMatches && match(words, patternIndex + 1, wordIndex + 1, fully);
  }

  @Override
  public String toString() {
    return new String(data);
  }
}
