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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

/** A class that modifies the given misspelled word in various ways to get correct suggestions */
class ModifyingSuggester {
  private static final int MAX_CHAR_DISTANCE = 4;
  private final LinkedHashSet<String> result;
  private final char[] tryChars;
  private final Hunspell speller;

  ModifyingSuggester(Hunspell speller, LinkedHashSet<String> result) {
    this.speller = speller;
    tryChars = speller.dictionary.tryChars.toCharArray();
    this.result = result;
  }

  /** @return whether any of the added suggestions are considered "good" */
  boolean suggest(String word, WordCase wordCase) {
    String low = wordCase != WordCase.LOWER ? speller.dictionary.toLowerCase(word) : word;
    if (wordCase == WordCase.UPPER || wordCase == WordCase.MIXED) {
      trySuggestion(low);
    }

    boolean hasGoodSuggestions = tryVariationsOf(word);

    if (wordCase == WordCase.TITLE) {
      hasGoodSuggestions |= tryVariationsOf(low);
    } else if (wordCase == WordCase.UPPER) {
      hasGoodSuggestions |= tryVariationsOf(low);
      hasGoodSuggestions |= tryVariationsOf(speller.dictionary.toTitleCase(word));
    } else if (wordCase == WordCase.MIXED) {
      int dot = word.indexOf('.');
      if (dot > 0
          && dot < word.length() - 1
          && WordCase.caseOf(word.substring(dot + 1)) == WordCase.TITLE) {
        result.add(word.substring(0, dot + 1) + " " + word.substring(dot + 1));
      }

      boolean capitalized = Character.isUpperCase(word.charAt(0));
      if (capitalized) {
        hasGoodSuggestions |=
            tryVariationsOf(speller.dictionary.caseFold(word.charAt(0)) + word.substring(1));
      }

      hasGoodSuggestions |= tryVariationsOf(low);

      if (capitalized) {
        hasGoodSuggestions |= tryVariationsOf(speller.dictionary.toTitleCase(low));
      }

      List<String> adjusted = new ArrayList<>();
      for (String candidate : result) {
        String s = capitalizeAfterSpace(word, candidate);
        adjusted.add(s.equals(candidate) ? adjusted.size() : 0, s);
      }

      result.clear();
      result.addAll(adjusted);
    }
    return hasGoodSuggestions;
  }

  // aNew -> "a New" (instead of "a new")
  private String capitalizeAfterSpace(String misspelled, String candidate) {
    int space = candidate.indexOf(' ');
    int tail = candidate.length() - space - 1;
    if (space > 0
        && !misspelled.regionMatches(misspelled.length() - tail, candidate, space + 1, tail)) {
      return candidate.substring(0, space + 1)
          + Character.toUpperCase(candidate.charAt(space + 1))
          + candidate.substring(space + 2);
    }
    return candidate;
  }

  private boolean tryVariationsOf(String word) {
    boolean hasGoodSuggestions = trySuggestion(word.toUpperCase(Locale.ROOT));
    hasGoodSuggestions |= tryRep(word);

    if (!speller.dictionary.mapTable.isEmpty()) {
      enumerateMapReplacements(word, "", 0);
    }

    trySwappingChars(word);
    tryLongSwap(word);
    tryNeighborKeys(word);
    tryRemovingChar(word);
    tryAddingChar(word);
    tryMovingChar(word);
    tryReplacingChar(word);
    tryTwoDuplicateChars(word);

    List<String> goodSplit = checkDictionaryForSplitSuggestions(word);
    if (!goodSplit.isEmpty()) {
      List<String> copy = new ArrayList<>(result);
      result.clear();
      result.addAll(goodSplit);
      if (hasGoodSuggestions) {
        result.addAll(copy);
      }
      hasGoodSuggestions = true;
    }

    if (!hasGoodSuggestions && speller.dictionary.enableSplitSuggestions) {
      trySplitting(word);
    }
    return hasGoodSuggestions;
  }

  private boolean tryRep(String word) {
    int before = result.size();
    for (RepEntry entry : speller.dictionary.repTable) {
      for (String candidate : entry.substitute(word)) {
        candidate = candidate.trim();
        if (trySuggestion(candidate)) {
          continue;
        }

        if (candidate.contains(" ")
            && Arrays.stream(candidate.split(" ")).allMatch(this::checkSimpleWord)) {
          result.add(candidate);
        }
      }
    }
    return result.size() > before;
  }

  private void enumerateMapReplacements(String word, String accumulated, int offset) {
    if (offset == word.length()) {
      trySuggestion(accumulated);
      return;
    }

    for (List<String> entries : speller.dictionary.mapTable) {
      for (String entry : entries) {
        if (word.regionMatches(offset, entry, 0, entry.length())) {
          for (String replacement : entries) {
            if (!entry.equals(replacement)) {
              enumerateMapReplacements(word, accumulated + replacement, offset + entry.length());
            }
          }
        }
      }
    }

    enumerateMapReplacements(word, accumulated + word.charAt(offset), offset + 1);
  }

  private boolean checkSimpleWord(String part) {
    return Boolean.TRUE.equals(speller.checkSimpleWord(part.toCharArray(), part.length(), null));
  }

  private void trySwappingChars(String word) {
    int length = word.length();
    for (int i = 0; i < length - 1; i++) {
      char c1 = word.charAt(i);
      char c2 = word.charAt(i + 1);
      trySuggestion(word.substring(0, i) + c2 + c1 + word.substring(i + 2));
    }

    if (length == 4 || length == 5) {
      tryDoubleSwapForShortWords(word, length);
    }
  }

  // ahev -> have, owudl -> would
  private void tryDoubleSwapForShortWords(String word, int length) {
    char[] candidate = word.toCharArray();
    candidate[0] = word.charAt(1);
    candidate[1] = word.charAt(0);
    candidate[length - 1] = word.charAt(length - 2);
    candidate[length - 2] = word.charAt(length - 1);
    trySuggestion(new String(candidate));

    if (candidate.length == 5) {
      candidate[0] = word.charAt(0);
      candidate[1] = word.charAt(2);
      candidate[2] = word.charAt(1);
      trySuggestion(new String(candidate));
    }
  }

  private void tryNeighborKeys(String word) {
    for (int i = 0; i < word.length(); i++) {
      char c = word.charAt(i);
      char up = Character.toUpperCase(c);
      if (up != c) {
        trySuggestion(word.substring(0, i) + up + word.substring(i + 1));
      }

      // check neighbor characters in keyboard string
      for (String group : speller.dictionary.neighborKeyGroups) {
        if (group.indexOf(c) >= 0) {
          for (int j = 0; j < group.length(); j++) {
            if (group.charAt(j) != c) {
              trySuggestion(word.substring(0, i) + group.charAt(j) + word.substring(i + 1));
            }
          }
        }
      }
    }
  }

  private void tryLongSwap(String word) {
    for (int i = 0; i < word.length(); i++) {
      for (int j = i + 2; j < word.length() && j <= i + MAX_CHAR_DISTANCE; j++) {
        char c1 = word.charAt(i);
        char c2 = word.charAt(j);
        String prefix = word.substring(0, i);
        String suffix = word.substring(j + 1);
        trySuggestion(prefix + c2 + word.substring(i + 1, j) + c1 + suffix);
      }
    }
  }

  private void tryRemovingChar(String word) {
    if (word.length() == 1) return;

    for (int i = 0; i < word.length(); i++) {
      trySuggestion(word.substring(0, i) + word.substring(i + 1));
    }
  }

  private void tryAddingChar(String word) {
    for (int i = 0; i <= word.length(); i++) {
      String prefix = word.substring(0, i);
      String suffix = word.substring(i);
      for (char toInsert : tryChars) {
        trySuggestion(prefix + toInsert + suffix);
      }
    }
  }

  private void tryMovingChar(String word) {
    for (int i = 0; i < word.length(); i++) {
      String prefix = word.substring(0, i);
      for (int j = i + 2; j < word.length() && j <= i + MAX_CHAR_DISTANCE; j++) {
        trySuggestion(prefix + word.substring(i + 1, j) + word.charAt(i) + word.substring(j));
        trySuggestion(prefix + word.charAt(j) + word.substring(i, j) + word.substring(j + 1));
      }
      if (i < word.length() - 1) {
        trySuggestion(prefix + word.substring(i + 1) + word.charAt(i));
      }
    }
  }

  private void tryReplacingChar(String word) {
    for (int i = 0; i < word.length(); i++) {
      String prefix = word.substring(0, i);
      String suffix = word.substring(i + 1);
      for (char toInsert : tryChars) {
        if (toInsert != word.charAt(i)) {
          trySuggestion(prefix + toInsert + suffix);
        }
      }
    }
  }

  // perhaps we doubled two characters
  // (for example vacation -> vacacation)
  private void tryTwoDuplicateChars(String word) {
    int dupLen = 0;
    for (int i = 2; i < word.length(); i++) {
      if (word.charAt(i) == word.charAt(i - 2)) {
        dupLen++;
        if (dupLen == 3 || dupLen == 2 && i >= 4) {
          trySuggestion(word.substring(0, i - 1) + word.substring(i + 1));
          dupLen = 0;
        }
      } else {
        dupLen = 0;
      }
    }
  }

  private List<String> checkDictionaryForSplitSuggestions(String word) {
    List<String> result = new ArrayList<>();
    for (int i = 1; i < word.length() - 1; i++) {
      String w1 = word.substring(0, i);
      String w2 = word.substring(i);
      String spaced = w1 + " " + w2;
      if (speller.checkWord(spaced)) {
        result.add(spaced);
      }
      if (shouldSplitByDash()) {
        String dashed = w1 + "-" + w2;
        if (speller.checkWord(dashed)) {
          result.add(dashed);
        }
      }
    }
    return result;
  }

  private void trySplitting(String word) {
    for (int i = 1; i < word.length(); i++) {
      String w1 = word.substring(0, i);
      String w2 = word.substring(i);
      if (checkSimpleWord(w1) && checkSimpleWord(w2)) {
        result.add(w1 + " " + w2);
        if (w1.length() > 1 && w2.length() > 1 && shouldSplitByDash()) {
          result.add(w1 + "-" + w2);
        }
      }
    }
  }

  private boolean shouldSplitByDash() {
    return speller.dictionary.tryChars.contains("-") || speller.dictionary.tryChars.contains("a");
  }

  private boolean trySuggestion(String candidate) {
    return speller.checkWord(candidate) && result.add(candidate);
  }
}
