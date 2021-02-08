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
    if (dictionary.isForbiddenWord(wordChars, wordChars.length)) {
      return false;
    }

    if (checkWord(wordChars, wordChars.length, null)) {
      return true;
    }

    WordCase wc = stemmer.caseOf(wordChars, wordChars.length);
    if ((wc == WordCase.UPPER || wc == WordCase.TITLE)) {
      Stemmer.CaseVariationProcessor variationProcessor =
          (variant, varLength, originalCase) -> !checkWord(variant, varLength, originalCase);
      if (!stemmer.varyCase(wordChars, wordChars.length, wc, variationProcessor)) {
        return true;
      }
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

  boolean checkWord(String word) {
    return checkWord(word.toCharArray(), word.length(), null);
  }

  Boolean checkSimpleWord(char[] wordChars, int length, WordCase originalCase) {
    if (dictionary.isForbiddenWord(wordChars, length)) {
      return false;
    }

    if (findStem(wordChars, 0, length, originalCase, SIMPLE_WORD) != null) {
      return true;
    }

    return null;
  }

  private boolean checkWord(char[] wordChars, int length, WordCase originalCase) {
    Boolean simpleResult = checkSimpleWord(wordChars, length, originalCase);
    if (simpleResult != null) {
      return simpleResult;
    }

    if (dictionary.compoundRules != null
        && checkCompoundRules(wordChars, 0, length, new ArrayList<>())) {
      return true;
    }

    if (dictionary.compoundBegin != FLAG_UNSET || dictionary.compoundFlag != FLAG_UNSET) {
      return checkCompounds(new CharsRef(wordChars, 0, length), originalCase, null);
    }

    return false;
  }

  private CharsRef findStem(
      char[] wordChars, int offset, int length, WordCase originalCase, WordContext context) {
    CharsRef[] result = {null};
    stemmer.doStem(
        wordChars,
        offset,
        length,
        originalCase,
        context,
        (stem, forms, formID) -> {
          result[0] = stem;
          return false;
        });
    return result[0];
  }

  private boolean checkCompounds(CharsRef word, WordCase originalCase, CompoundPart prev) {
    if (prev != null && prev.index > dictionary.compoundMax - 2) return false;

    int limit = word.length - dictionary.compoundMin + 1;
    for (int breakPos = dictionary.compoundMin; breakPos < limit; breakPos++) {
      WordContext context = prev == null ? COMPOUND_BEGIN : COMPOUND_MIDDLE;
      int breakOffset = word.offset + breakPos;
      if (mayBreakIntoCompounds(word.chars, word.offset, word.length, breakOffset)) {
        CharsRef stem = findStem(word.chars, word.offset, breakPos, originalCase, context);
        if (stem == null
            && dictionary.simplifiedTriple
            && word.chars[breakOffset - 1] == word.chars[breakOffset]) {
          stem = findStem(word.chars, word.offset, breakPos + 1, originalCase, context);
        }
        if (stem != null && (prev == null || prev.mayCompound(stem, breakPos, originalCase))) {
          CompoundPart part = new CompoundPart(prev, word, breakPos, stem, null);
          if (checkCompoundsAfter(originalCase, part)) {
            return true;
          }
        }
      }

      if (checkCompoundPatternReplacements(word, breakPos, originalCase, prev)) {
        return true;
      }
    }

    return false;
  }

  private boolean checkCompoundPatternReplacements(
      CharsRef word, int pos, WordCase originalCase, CompoundPart prev) {
    for (CheckCompoundPattern pattern : dictionary.checkCompoundPatterns) {
      CharsRef expanded = pattern.expandReplacement(word, pos);
      if (expanded != null) {
        WordContext context = prev == null ? COMPOUND_BEGIN : COMPOUND_MIDDLE;
        int breakPos = pos + pattern.endLength();
        CharsRef stem = findStem(expanded.chars, expanded.offset, breakPos, originalCase, context);
        if (stem != null) {
          CompoundPart part = new CompoundPart(prev, expanded, breakPos, stem, pattern);
          if (checkCompoundsAfter(originalCase, part)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private boolean checkCompoundsAfter(WordCase originalCase, CompoundPart prev) {
    CharsRef word = prev.tail;
    int breakPos = prev.length;
    int remainingLength = word.length - breakPos;
    int breakOffset = word.offset + breakPos;
    CharsRef tailStem =
        findStem(word.chars, breakOffset, remainingLength, originalCase, COMPOUND_END);
    if (tailStem != null
        && !(dictionary.checkCompoundDup && equalsIgnoreCase(prev.stem, tailStem))
        && !hasForceUCaseProblem(word.chars, breakOffset, remainingLength, originalCase)
        && prev.mayCompound(tailStem, remainingLength, originalCase)) {
      return true;
    }

    CharsRef tail = new CharsRef(word.chars, breakOffset, remainingLength);
    return checkCompounds(tail, originalCase, prev);
  }

  private boolean hasForceUCaseProblem(
      char[] chars, int offset, int length, WordCase originalCase) {
    if (dictionary.forceUCase == FLAG_UNSET) return false;
    if (originalCase == WordCase.TITLE || originalCase == WordCase.UPPER) return false;

    IntsRef forms = dictionary.lookupWord(chars, offset, length);
    return forms != null && dictionary.hasFlag(forms, dictionary.forceUCase);
  }

  private boolean equalsIgnoreCase(CharsRef cr1, CharsRef cr2) {
    return cr1.toString().equalsIgnoreCase(cr2.toString());
  }

  private class CompoundPart {
    final CompoundPart prev;
    final int index, length;
    final CharsRef tail, stem;
    final CheckCompoundPattern enablingPattern;

    CompoundPart(
        CompoundPart prev, CharsRef tail, int length, CharsRef stem, CheckCompoundPattern enabler) {
      this.prev = prev;
      this.tail = tail;
      this.length = length;
      this.stem = stem;
      index = prev == null ? 1 : prev.index + 1;
      enablingPattern = enabler;
    }

    @Override
    public String toString() {
      return (prev == null ? "" : prev + "+") + tail.subSequence(0, length);
    }

    boolean mayCompound(CharsRef nextStem, int nextPartLength, WordCase originalCase) {
      boolean patternsOk =
          enablingPattern != null
              ? enablingPattern.prohibitsCompounding(tail, length, stem, nextStem)
              : dictionary.checkCompoundPatterns.stream()
                  .noneMatch(p -> p.prohibitsCompounding(tail, length, stem, nextStem));
      if (!patternsOk) {
        return false;
      }

      //noinspection RedundantIfStatement
      if (dictionary.checkCompoundRep
          && isMisspelledSimpleWord(length + nextPartLength, originalCase)) {
        return false;
      }
      return true;
    }

    private boolean isMisspelledSimpleWord(int length, WordCase originalCase) {
      String word = new String(tail.chars, tail.offset, length);
      for (RepEntry entry : dictionary.repTable) {
        if (entry.isMiddle()) {
          for (String sug : entry.substitute(word)) {
            if (findStem(sug.toCharArray(), 0, sug.length(), originalCase, SIMPLE_WORD) != null) {
              return true;
            }
          }
        }
      }
      return false;
    }
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
            && dictionary.compoundRules.stream().anyMatch(r -> r.mayMatch(words))) {
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
    boolean result = dictionary.compoundRules.stream().anyMatch(r -> r.fullyMatches(words));
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
            String replaced = word.substring(0, chunkStart) + chunkSug + word.substring(chunkEnd);
            if (!dictionary.isForbiddenWord(replaced.toCharArray(), replaced.length())) {
              result.add(replaced);
            }
          }
        }
      }

      chunkStart = chunkEnd + 1;
    }
    return result;
  }
}
