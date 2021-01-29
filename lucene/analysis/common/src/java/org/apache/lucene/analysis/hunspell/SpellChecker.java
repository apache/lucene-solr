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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;

/**
 * A spell checker based on Hunspell dictionaries. The objects of this class are not thread-safe
 * (but a single underlying Dictionary can be shared by multiple spell-checkers in different
 * threads). Not all Hunspell features are supported yet.
 */
public class SpellChecker {
  final Dictionary dictionary;
  final Stemmer stemmer;
  private final BytesRef scratch = new BytesRef();

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

    if (dictionary.isDotICaseChangeDisallowed(wordChars)) {
      return false;
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

  boolean checkWord(String word) {
    return checkWord(word.toCharArray(), word.length(), null);
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
      return checkCompounds(new CharsRef(wordChars, 0, length), originalCase, 0, __ -> true);
    }

    return false;
  }

  private boolean checkCompounds(
      CharsRef word, WordCase originalCase, int depth, Predicate<List<CharsRef>> checkPatterns) {
    if (depth > dictionary.compoundMax - 2) return false;

    int limit = word.length - dictionary.compoundMin + 1;
    for (int breakPos = dictionary.compoundMin; breakPos < limit; breakPos++) {
      WordContext context = depth == 0 ? COMPOUND_BEGIN : COMPOUND_MIDDLE;
      int breakOffset = word.offset + breakPos;
      if (mayBreakIntoCompounds(word.chars, word.offset, word.length, breakOffset)) {
        List<CharsRef> stems =
            stemmer.doStem(word.chars, word.offset, breakPos, originalCase, context);
        if (stems.isEmpty()
            && dictionary.simplifiedTriple
            && word.chars[breakOffset - 1] == word.chars[breakOffset]) {
          stems = stemmer.doStem(word.chars, word.offset, breakPos + 1, originalCase, context);
        }
        if (!stems.isEmpty() && checkPatterns.test(stems)) {
          Predicate<List<CharsRef>> nextCheck = checkNextPatterns(word, breakPos, stems);
          if (checkCompoundsAfter(word, breakPos, originalCase, depth, stems, nextCheck)) {
            return true;
          }
        }
      }

      if (checkCompoundPatternReplacements(word, breakPos, originalCase, depth)) {
        return true;
      }
    }

    return false;
  }

  private boolean checkCompoundPatternReplacements(
      CharsRef word, int pos, WordCase originalCase, int depth) {
    for (CheckCompoundPattern pattern : dictionary.checkCompoundPatterns) {
      CharsRef expanded = pattern.expandReplacement(word, pos);
      if (expanded != null) {
        WordContext context = depth == 0 ? COMPOUND_BEGIN : COMPOUND_MIDDLE;
        int breakPos = pos + pattern.endLength();
        List<CharsRef> stems =
            stemmer.doStem(expanded.chars, expanded.offset, breakPos, originalCase, context);
        if (!stems.isEmpty()) {
          Predicate<List<CharsRef>> nextCheck =
              next -> pattern.prohibitsCompounding(expanded, breakPos, stems, next);
          if (checkCompoundsAfter(expanded, breakPos, originalCase, depth, stems, nextCheck)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private Predicate<List<CharsRef>> checkNextPatterns(
      CharsRef word, int breakPos, List<CharsRef> stems) {
    return nextStems ->
        dictionary.checkCompoundPatterns.stream()
            .noneMatch(p -> p.prohibitsCompounding(word, breakPos, stems, nextStems));
  }

  private boolean checkCompoundsAfter(
      CharsRef word,
      int breakPos,
      WordCase originalCase,
      int depth,
      List<CharsRef> prevStems,
      Predicate<List<CharsRef>> checkPatterns) {
    int remainingLength = word.length - breakPos;
    int breakOffset = word.offset + breakPos;
    List<CharsRef> tailStems =
        stemmer.doStem(word.chars, breakOffset, remainingLength, originalCase, COMPOUND_END);
    if (!tailStems.isEmpty()
        && !(dictionary.checkCompoundDup && intersectIgnoreCase(prevStems, tailStems))
        && !hasForceUCaseProblem(word.chars, breakOffset, remainingLength, originalCase)
        && checkPatterns.test(tailStems)) {
      return true;
    }

    CharsRef tail = new CharsRef(word.chars, breakOffset, remainingLength);
    return checkCompounds(tail, originalCase, depth + 1, checkPatterns);
  }

  private boolean hasForceUCaseProblem(
      char[] chars, int offset, int length, WordCase originalCase) {
    if (dictionary.forceUCase == FLAG_UNSET) return false;
    if (originalCase == WordCase.TITLE || originalCase == WordCase.UPPER) return false;

    IntsRef forms = dictionary.lookupWord(chars, offset, length);
    return forms != null && dictionary.hasFlag(forms, dictionary.forceUCase, scratch);
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

  public List<String> suggest(String word) {
    if (word.length() >= 100) return Collections.emptyList();

    if (dictionary.needsInputCleaning) {
      word = dictionary.cleanInput(word, new StringBuilder()).toString();
    }

    ModifyingSuggester modifier = new ModifyingSuggester(this);
    Set<String> result = modifier.suggest(word);

    if (word.contains("-") && result.stream().noneMatch(s -> s.contains("-"))) {
      result.addAll(modifyChunksBetweenDashes(word));
    }

    return new ArrayList<>(result);
  }

  private List<String> modifyChunksBetweenDashes(String word) {
    List<String> result = new ArrayList<>();
    int chunkStart = 0;
    while (chunkStart < word.length()) {
      int chunkEnd = word.indexOf('-', chunkStart);
      if (chunkEnd < 0) {
        chunkEnd = word.length();
      }

      if (chunkEnd > chunkStart) {
        String chunk = word.substring(chunkStart, chunkEnd);
        if (!spell(chunk)) {
          for (String chunkSug : suggest(chunk)) {
            result.add(word.substring(0, chunkStart) + chunkSug + word.substring(chunkEnd));
          }
        }
      }

      chunkStart = chunkEnd + 1;
    }
    return result;
  }
}
