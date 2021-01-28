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

import static org.apache.lucene.analysis.hunspell.Dictionary.FLAG_UNSET;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_BEGIN;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_END;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_MIDDLE;
import static org.apache.lucene.analysis.hunspell.WordContext.SIMPLE_WORD;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;

/**
 * A spell checker based on Hunspell dictionaries. The objects of this class are not thread-safe
 * (but a single underlying Dictionary can be shared by multiple spell-checkers in different
 * threads). Not all Hunspell features are supported yet.
 */
public class SpellChecker {
  private final Dictionary dictionary;
  private final BytesRef scratch = new BytesRef();
  private final Stemmer stemmer;

  public SpellChecker(Dictionary dictionary) {
    this.dictionary = dictionary;
    stemmer = new Stemmer(dictionary);
  }

  /** @return whether the given word's spelling is considered correct according to Hunspell rules */
  public boolean spell(String word) {
    if (word.isEmpty()) return true;

    if (dictionary.needsInputCleaning) {
      word = dictionary.cleanInput(word, new StringBuilder()).toString();
    }

    if (word.endsWith(".")) {
      return spellWithTrailingDots(word);
    }

    return spellClean(word);
  }

  private boolean spellClean(String word) {
    if (isNumber(word)) {
      return true;
    }

    char[] wordChars = word.toCharArray();
    if (dictionary.isForbiddenWord(wordChars, wordChars.length, scratch)) {
      return false;
    }

    if (checkWord(wordChars, wordChars.length, null)) {
      return true;
    }

    WordCase wc = stemmer.caseOf(wordChars, wordChars.length);
    if ((wc == WordCase.UPPER || wc == WordCase.TITLE) && checkCaseVariants(wordChars, wc)) {
      return true;
    }

    if (dictionary.breaks.isNotEmpty() && !hasTooManyBreakOccurrences(word)) {
      return tryBreaks(word);
    }

    return false;
  }

  private boolean spellWithTrailingDots(String word) {
    int length = word.length() - 1;
    while (length > 0 && word.charAt(length - 1) == '.') {
      length--;
    }
    return spellClean(word.substring(0, length)) || spellClean(word.substring(0, length + 1));
  }

  private boolean checkCaseVariants(char[] wordChars, WordCase wordCase) {
    char[] caseVariant = wordChars;
    if (wordCase == WordCase.UPPER) {
      caseVariant = stemmer.caseFoldTitle(caseVariant, wordChars.length);
      if (checkWord(caseVariant, wordChars.length, wordCase)) {
        return true;
      }
      char[] aposCase = Stemmer.capitalizeAfterApostrophe(caseVariant, wordChars.length);
      if (aposCase != null && checkWord(aposCase, aposCase.length, wordCase)) {
        return true;
      }
      for (char[] variation : stemmer.sharpSVariations(caseVariant, wordChars.length)) {
        if (checkWord(variation, variation.length, null)) {
          return true;
        }
      }
    }
    char[] lower = stemmer.caseFoldLower(caseVariant, wordChars.length);
    if (checkWord(lower, wordChars.length, wordCase)) {
      return true;
    }
    if (wordCase == WordCase.UPPER) {
      for (char[] variation : stemmer.sharpSVariations(lower, wordChars.length)) {
        if (checkWord(variation, variation.length, null)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean checkWord(char[] wordChars, int length, WordCase originalCase) {
    if (dictionary.isForbiddenWord(wordChars, length, scratch)) {
      return false;
    }

    if (!stemmer.doStem(wordChars, 0, length, originalCase, SIMPLE_WORD).isEmpty()) {
      return true;
    }

    if (dictionary.compoundRules != null
        && checkCompoundRules(wordChars, 0, length, new ArrayList<>())) {
      return true;
    }

    if (dictionary.compoundBegin != FLAG_UNSET || dictionary.compoundFlag != FLAG_UNSET) {
      return checkCompounds(wordChars, 0, length, originalCase, 0);
    }

    return false;
  }

  private boolean checkCompounds(
      char[] chars, int offset, int length, WordCase originalCase, int depth) {
    if (depth > dictionary.compoundMax - 2) return false;

    int limit = length - dictionary.compoundMin + 1;
    for (int breakPos = dictionary.compoundMin; breakPos < limit; breakPos++) {
      WordContext context = depth == 0 ? COMPOUND_BEGIN : COMPOUND_MIDDLE;
      int breakOffset = offset + breakPos;
      if (mayBreakIntoCompounds(chars, offset, length, breakOffset)) {
        List<CharsRef> stems = stemmer.doStem(chars, offset, breakPos, originalCase, context);
        if (stems.isEmpty()
            && dictionary.simplifiedTriple
            && chars[breakOffset - 1] == chars[breakOffset]) {
          stems = stemmer.doStem(chars, offset, breakPos + 1, originalCase, context);
        }
        if (stems.isEmpty()) continue;

        int remainingLength = length - breakPos;
        List<CharsRef> lastStems =
            stemmer.doStem(chars, breakOffset, remainingLength, originalCase, COMPOUND_END);
        if (!lastStems.isEmpty()
            && !(dictionary.checkCompoundDup && intersectIgnoreCase(stems, lastStems))) {
          return true;
        }

        if (checkCompounds(chars, breakOffset, remainingLength, originalCase, depth + 1)) {
          return true;
        }
      }
    }

    return false;
  }

  private boolean intersectIgnoreCase(List<CharsRef> stems1, List<CharsRef> stems2) {
    return stems1.stream().anyMatch(s1 -> stems2.stream().anyMatch(s2 -> equalsIgnoreCase(s1, s2)));
  }

  private boolean equalsIgnoreCase(CharsRef cr1, CharsRef cr2) {
    return cr1.toString().equalsIgnoreCase(cr2.toString());
  }

  private boolean mayBreakIntoCompounds(char[] chars, int offset, int length, int breakPos) {
    if (dictionary.checkCompoundCase) {
      if (Character.isUpperCase(chars[breakPos - 1]) || Character.isUpperCase(chars[breakPos])) {
        return false;
      }
    }
    if (dictionary.checkCompoundTriple && chars[breakPos - 1] == chars[breakPos]) {
      //noinspection RedundantIfStatement
      if (breakPos > offset + 1 && chars[breakPos - 2] == chars[breakPos - 1]
          || breakPos < length - 1 && chars[breakPos] == chars[breakPos + 1]) {
        return false;
      }
    }
    return true;
  }

  private boolean checkCompoundRules(
      char[] wordChars, int offset, int length, List<IntsRef> words) {
    if (words.size() >= 100) return false;

    int limit = length - dictionary.compoundMin + 1;
    for (int breakPos = dictionary.compoundMin; breakPos < limit; breakPos++) {
      IntsRef forms = dictionary.lookupWord(wordChars, offset, breakPos);
      if (forms != null) {
        words.add(forms);

        if (dictionary.compoundRules != null
            && dictionary.compoundRules.stream().anyMatch(r -> r.mayMatch(words, scratch))) {
          if (checkLastCompoundPart(wordChars, offset + breakPos, length - breakPos, words)) {
            return true;
          }

          if (checkCompoundRules(wordChars, offset + breakPos, length - breakPos, words)) {
            return true;
          }
        }

        words.remove(words.size() - 1);
      }
    }

    return false;
  }

  private boolean checkLastCompoundPart(
      char[] wordChars, int start, int length, List<IntsRef> words) {
    IntsRef forms = dictionary.lookupWord(wordChars, start, length);
    if (forms == null) return false;

    words.add(forms);
    boolean result =
        dictionary.compoundRules.stream().anyMatch(r -> r.fullyMatches(words, scratch));
    words.remove(words.size() - 1);
    return result;
  }

  private static boolean isNumber(String s) {
    int i = 0;
    while (i < s.length()) {
      char c = s.charAt(i);
      if (isDigit(c)) {
        i++;
      } else if (c == '.' || c == ',' || c == '-') {
        if (i == 0 || i >= s.length() - 1 || !isDigit(s.charAt(i + 1))) {
          return false;
        }
        i += 2;
      } else {
        return false;
      }
    }
    return true;
  }

  private static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  private boolean tryBreaks(String word) {
    for (String br : dictionary.breaks.starting) {
      if (word.length() > br.length() && word.startsWith(br)) {
        if (spell(word.substring(br.length()))) {
          return true;
        }
      }
    }

    for (String br : dictionary.breaks.ending) {
      if (word.length() > br.length() && word.endsWith(br)) {
        if (spell(word.substring(0, word.length() - br.length()))) {
          return true;
        }
      }
    }

    for (String br : dictionary.breaks.middle) {
      int pos = word.indexOf(br);
      if (canBeBrokenAt(word, br, pos)) {
        return true;
      }

      // try to break at the second occurrence
      // to recognize dictionary words with a word break
      if (pos > 0 && canBeBrokenAt(word, br, word.indexOf(br, pos + 1))) {
        return true;
      }
    }
    return false;
  }

  private boolean hasTooManyBreakOccurrences(String word) {
    int occurrences = 0;
    for (String br : dictionary.breaks.middle) {
      int pos = 0;
      while ((pos = word.indexOf(br, pos)) >= 0) {
        if (++occurrences >= 10) return true;
        pos += br.length();
      }
    }
    return false;
  }

  private boolean canBeBrokenAt(String word, String breakStr, int breakPos) {
    return breakPos > 0
        && breakPos < word.length() - breakStr.length()
        && spell(word.substring(0, breakPos))
        && spell(word.substring(breakPos + breakStr.length()));
  }
}
