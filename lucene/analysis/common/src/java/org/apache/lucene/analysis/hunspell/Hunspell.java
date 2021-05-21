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
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.*;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_BEGIN;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_END;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_MIDDLE;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_RULE_END;
import static org.apache.lucene.analysis.hunspell.WordContext.SIMPLE_WORD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;

/**
 * A spell checker based on Hunspell dictionaries. This class can be used in place of native
 * Hunspell for many languages for spell-checking and suggesting purposes. Note that not all
 * languages are supported yet. For example:
 *
 * <ul>
 *   <li>Hungarian (as it doesn't only rely on dictionaries, but has some logic directly in the
 *       source code
 *   <li>Languages with Unicode characters outside of the Basic Multilingual Plane
 *   <li>PHONE affix file option for suggestions
 * </ul>
 *
 * <p>The objects of this class are thread-safe.
 */
public class Hunspell {
  static final long SUGGEST_TIME_LIMIT = 250;

  final Dictionary dictionary;
  final Stemmer stemmer;
  private final TimeoutPolicy policy;
  final Runnable checkCanceled;

  public Hunspell(Dictionary dictionary) {
    this(dictionary, RETURN_PARTIAL_RESULT, () -> {});
  }

  /**
   * @param policy a strategy determining what to do when API calls take too much time
   * @param checkCanceled an object that's periodically called, allowing to interrupt spell-checking
   *     or suggestion generation by throwing an exception
   */
  public Hunspell(Dictionary dictionary, TimeoutPolicy policy, Runnable checkCanceled) {
    this.dictionary = dictionary;
    this.policy = policy;
    this.checkCanceled = checkCanceled;
    stemmer = new Stemmer(dictionary);
  }

  /** @return whether the given word's spelling is considered correct according to Hunspell rules */
  public boolean spell(String word) {
    checkCanceled.run();
    if (word.isEmpty()) return true;

    if (dictionary.needsInputCleaning(word)) {
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
    Boolean simpleResult = checkSimpleWord(wordChars, wordChars.length, null);
    if (simpleResult != null) {
      return simpleResult;
    }

    if (checkCompounds(wordChars, wordChars.length, null)) {
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
    Root<CharsRef> entry = findStem(wordChars, 0, length, originalCase, SIMPLE_WORD);
    if (entry != null) {
      return !dictionary.hasFlag(entry.entryId, dictionary.forbiddenword);
    }

    return null;
  }

  private boolean checkWord(char[] wordChars, int length, WordCase originalCase) {
    Boolean simpleResult = checkSimpleWord(wordChars, length, originalCase);
    if (simpleResult != null) {
      return simpleResult;
    }

    return checkCompounds(wordChars, length, originalCase);
  }

  private boolean checkCompounds(char[] wordChars, int length, WordCase originalCase) {
    if (dictionary.compoundRules != null
        && checkCompoundRules(wordChars, 0, length, new ArrayList<>())) {
      return true;
    }

    if (dictionary.compoundBegin != FLAG_UNSET || dictionary.compoundFlag != FLAG_UNSET) {
      return checkCompounds(new CharsRef(wordChars, 0, length), originalCase, null);
    }

    return false;
  }

  private Root<CharsRef> findStem(
      char[] wordChars, int offset, int length, WordCase originalCase, WordContext context) {
    checkCanceled.run();
    boolean checkCase = context != COMPOUND_MIDDLE && context != COMPOUND_END;
    @SuppressWarnings({"rawtypes", "unchecked"})
    Root<CharsRef>[] result = new Root[1];
    stemmer.doStem(
        wordChars,
        offset,
        length,
        context,
        (stem, formID, morphDataId) -> {
          if (checkCase && !acceptCase(originalCase, formID, stem)) {
            return dictionary.hasFlag(formID, Dictionary.HIDDEN_FLAG);
          }
          if (acceptsStem(formID)) {
            result[0] = new Root<>(stem, formID);
          }
          return false;
        });
    return result[0];
  }

  private boolean acceptCase(WordCase originalCase, int entryId, CharsRef root) {
    boolean keepCase = dictionary.hasFlag(entryId, dictionary.keepcase);
    if (originalCase != null) {
      if (keepCase
          && dictionary.checkSharpS
          && originalCase == WordCase.TITLE
          && containsSharpS(root.chars, root.offset, root.length)) {
        return true;
      }
      return !keepCase;
    }
    return !dictionary.hasFlag(entryId, Dictionary.HIDDEN_FLAG);
  }

  private boolean containsSharpS(char[] word, int offset, int length) {
    for (int i = 0; i < length; i++) {
      if (word[i + offset] == 'ß') {
        return true;
      }
    }
    return false;
  }

  boolean acceptsStem(int formID) {
    return true;
  }

  private boolean checkCompounds(CharsRef word, WordCase originalCase, CompoundPart prev) {
    if (prev != null && prev.index > dictionary.compoundMax - 2) return false;
    if (prev == null && word.offset != 0) {
      // we check the word's beginning for FORCEUCASE and expect to find it at 0
      throw new IllegalArgumentException();
    }

    int limit = word.length - dictionary.compoundMin + 1;
    for (int breakPos = dictionary.compoundMin; breakPos < limit; breakPos++) {
      WordContext context = prev == null ? COMPOUND_BEGIN : COMPOUND_MIDDLE;
      int breakOffset = word.offset + breakPos;
      if (mayBreakIntoCompounds(word.chars, word.offset, word.length, breakOffset)) {
        Root<CharsRef> stem = findStem(word.chars, word.offset, breakPos, originalCase, context);
        if (stem == null
            && dictionary.simplifiedTriple
            && word.chars[breakOffset - 1] == word.chars[breakOffset]) {
          stem = findStem(word.chars, word.offset, breakPos + 1, originalCase, context);
        }
        if (stem != null
            && !dictionary.hasFlag(stem.entryId, dictionary.forbiddenword)
            && (prev == null || prev.mayCompound(stem, breakPos, originalCase))) {
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
        Root<CharsRef> stem =
            findStem(expanded.chars, expanded.offset, breakPos, originalCase, context);
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
    Root<CharsRef> lastRoot =
        findStem(word.chars, breakOffset, remainingLength, originalCase, COMPOUND_END);
    if (lastRoot != null
        && !dictionary.hasFlag(lastRoot.entryId, dictionary.forbiddenword)
        && !(dictionary.checkCompoundDup && prev.root.equals(lastRoot))
        && !hasForceUCaseProblem(lastRoot, originalCase, word.chars)
        && prev.mayCompound(lastRoot, remainingLength, originalCase)) {
      return true;
    }

    CharsRef tail = new CharsRef(word.chars, breakOffset, remainingLength);
    return checkCompounds(tail, originalCase, prev);
  }

  private boolean hasForceUCaseProblem(Root<?> root, WordCase originalCase, char[] wordChars) {
    if (originalCase == WordCase.TITLE || originalCase == WordCase.UPPER) return false;
    if (originalCase == null && Character.isUpperCase(wordChars[0])) return false;
    return dictionary.hasFlag(root.entryId, dictionary.forceUCase);
  }

  /**
   * Find all roots that could result in the given word after case conversion and adding affixes.
   * This corresponds to the original {@code hunspell -s} (stemming) functionality.
   *
   * <p>Some affix rules are relaxed in this stemming process: e.g. explicitly forbidden words are
   * still returned. Some of the returned roots may be synthetic and not directly occur in the *.dic
   * file (but differ from some existing entries in case). No roots are returned for compound words.
   *
   * <p>The returned roots may be used to retrieve morphological data via {@link
   * Dictionary#lookupEntries}.
   */
  public List<String> getRoots(String word) {
    return stemmer.stem(word).stream()
        .map(CharsRef::toString)
        .distinct()
        .collect(Collectors.toList());
  }

  private class CompoundPart {
    final CompoundPart prev;
    final int index, length;
    final CharsRef tail;
    final Root<CharsRef> root;
    final CheckCompoundPattern enablingPattern;

    CompoundPart(
        CompoundPart prev,
        CharsRef tail,
        int length,
        Root<CharsRef> root,
        CheckCompoundPattern enabler) {
      this.prev = prev;
      this.tail = tail;
      this.length = length;
      this.root = root;
      index = prev == null ? 1 : prev.index + 1;
      enablingPattern = enabler;
    }

    @Override
    public String toString() {
      return (prev == null ? "" : prev + "+") + tail.subSequence(0, length);
    }

    boolean mayCompound(Root<CharsRef> nextRoot, int nextPartLength, WordCase originalCase) {
      boolean patternsOk =
          enablingPattern != null
              ? enablingPattern.prohibitsCompounding(tail, length, root, nextRoot)
              : dictionary.checkCompoundPatterns.stream()
                  .noneMatch(p -> p.prohibitsCompounding(tail, length, root, nextRoot));
      if (!patternsOk) {
        return false;
      }

      if (dictionary.checkCompoundRep
          && isMisspelledSimpleWord(length + nextPartLength, originalCase)) {
        return false;
      }

      char[] spaceSeparated = new char[length + nextPartLength + 1];
      System.arraycopy(tail.chars, tail.offset, spaceSeparated, 0, length);
      System.arraycopy(
          tail.chars, tail.offset + length, spaceSeparated, length + 1, nextPartLength);
      spaceSeparated[length] = ' ';
      return !Boolean.TRUE.equals(checkSimpleWord(spaceSeparated, spaceSeparated.length, null));
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
      char a = chars[breakPos - 1];
      char b = chars[breakPos];
      if ((Character.isUpperCase(a) || Character.isUpperCase(b)) && a != '-' && b != '-') {
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
      checkCanceled.run();
      IntsRef forms = dictionary.lookupWord(wordChars, offset, breakPos);
      if (forms != null) {
        words.add(forms);

        if (dictionary.compoundRules.stream().anyMatch(r -> r.mayMatch(words))) {
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
    IntsRef ref = new IntsRef(new int[1], 0, 1);
    words.add(ref);

    Stemmer.RootProcessor stopOnMatching =
        (stem, formID, morphDataId) -> {
          ref.ints[0] = formID;
          return dictionary.compoundRules.stream().noneMatch(r -> r.fullyMatches(words));
        };
    boolean found = !stemmer.doStem(wordChars, start, length, COMPOUND_RULE_END, stopOnMatching);
    words.remove(words.size() - 1);
    return found;
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

  /**
   * @return suggestions for the given misspelled word
   * @throws SuggestionTimeoutException if the computation takes too long and {@link
   *     TimeoutPolicy#THROW_EXCEPTION} was specified in the constructor
   */
  public List<String> suggest(String word) throws SuggestionTimeoutException {
    return suggest(word, SUGGEST_TIME_LIMIT);
  }

  /**
   * @param word the misspelled word to calculate suggestions for
   * @param timeLimitMs the duration limit in milliseconds, after which the associated {@link
   *     TimeoutPolicy}'s effects (exception or partial result) may kick in
   * @throws SuggestionTimeoutException if the computation takes too long and {@link
   *     TimeoutPolicy#THROW_EXCEPTION} was specified in the constructor
   */
  public List<String> suggest(String word, long timeLimitMs) throws SuggestionTimeoutException {
    checkCanceled.run();
    if (word.length() >= 100) return Collections.emptyList();

    if (dictionary.needsInputCleaning(word)) {
      word = dictionary.cleanInput(word, new StringBuilder()).toString();
    }

    WordCase wordCase = WordCase.caseOf(word);
    if (dictionary.forceUCase != FLAG_UNSET && wordCase == WordCase.LOWER) {
      String title = dictionary.toTitleCase(word);
      if (spell(title)) {
        return Collections.singletonList(title);
      }
    }

    LinkedHashSet<String> suggestions = new LinkedHashSet<>();
    Runnable checkCanceled =
        policy == NO_TIMEOUT
            ? this.checkCanceled
            : checkTimeLimit(word, wordCase, suggestions, timeLimitMs);
    try {
      doSuggest(word, wordCase, suggestions, checkCanceled);
    } catch (SuggestionTimeoutException e) {
      if (policy == RETURN_PARTIAL_RESULT) {
        return postprocess(word, wordCase, suggestions);
      }
      throw e;
    }

    return postprocess(word, wordCase, suggestions);
  }

  private void doSuggest(
      String word, WordCase wordCase, LinkedHashSet<String> suggestions, Runnable checkCanceled) {
    Hunspell suggestionSpeller =
        new Hunspell(dictionary, policy, checkCanceled) {
          @Override
          boolean acceptsStem(int formID) {
            return !dictionary.hasFlag(formID, dictionary.noSuggest)
                && !dictionary.hasFlag(formID, dictionary.subStandard);
          }
        };
    ModifyingSuggester modifier = new ModifyingSuggester(suggestionSpeller, suggestions);
    boolean hasGoodSuggestions = modifier.suggest(word, wordCase);

    if (!hasGoodSuggestions && dictionary.maxNGramSuggestions > 0) {
      suggestions.addAll(
          new GeneratingSuggester(suggestionSpeller)
              .suggest(dictionary.toLowerCase(word), wordCase, suggestions));
    }

    if (word.contains("-") && suggestions.stream().noneMatch(s -> s.contains("-"))) {
      suggestions.addAll(modifyChunksBetweenDashes(word));
    }
  }

  private Runnable checkTimeLimit(
      String word, WordCase wordCase, Set<String> suggestions, long timeLimitMs) {
    return new Runnable() {
      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeLimitMs);
      int invocationCounter = 100;

      @Override
      public void run() {
        checkCanceled.run();
        if (--invocationCounter <= 0) {
          if (System.nanoTime() - deadline > 0) {
            stop();
          }
          invocationCounter = 100;
        }
      }

      private void stop() {
        List<String> partialResult =
            policy == RETURN_PARTIAL_RESULT ? null : postprocess(word, wordCase, suggestions);
        String message = "Time limit of " + timeLimitMs + "ms exceeded for " + word;
        throw new SuggestionTimeoutException(message, partialResult);
      }
    };
  }

  private List<String> postprocess(String word, WordCase wordCase, Collection<String> suggestions) {
    Set<String> result = new LinkedHashSet<>();
    for (String candidate : suggestions) {
      result.add(adjustSuggestionCase(candidate, wordCase, word));
      if (wordCase == WordCase.UPPER && dictionary.checkSharpS && candidate.contains("ß")) {
        result.add(candidate);
      }
    }
    return result.stream().map(this::cleanOutput).collect(Collectors.toList());
  }

  private String adjustSuggestionCase(String candidate, WordCase originalCase, String original) {
    if (originalCase == WordCase.UPPER) {
      String upper = candidate.toUpperCase(Locale.ROOT);
      if (upper.contains(" ") || spell(upper)) {
        return upper;
      }
    }
    if (Character.isUpperCase(original.charAt(0))) {
      String title = Character.toUpperCase(candidate.charAt(0)) + candidate.substring(1);
      if (title.contains(" ") || spell(title)) {
        return title;
      }
    }
    return candidate;
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
            if (spell(replaced)) {
              result.add(replaced);
            }
          }
        }
      }

      chunkStart = chunkEnd + 1;
    }
    return result;
  }

  private String cleanOutput(String s) {
    if (dictionary.oconv == null) return s;

    StringBuilder sb = new StringBuilder(s);
    dictionary.oconv.applyMappings(sb);
    return sb.toString();
  }
}
