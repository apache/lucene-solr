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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.fst.FST;

/**
 * Stemmer uses the affix rules declared in the Dictionary to generate one or more stems for a word.
 * It conforms to the algorithm in the original hunspell algorithm, including recursive suffix
 * stripping.
 */
final class Stemmer {
  private final Dictionary dictionary;
  private final StringBuilder segment = new StringBuilder();

  // used for normalization
  private final StringBuilder scratchSegment = new StringBuilder();
  private char[] scratchBuffer = new char[32];

  // it's '1' if we have no stem exceptions, otherwise every other form
  // is really an ID pointing to the exception table
  private final int formStep;

  /**
   * Constructs a new Stemmer which will use the provided Dictionary to create its stems.
   *
   * @param dictionary Dictionary that will be used to create the stems
   */
  public Stemmer(Dictionary dictionary) {
    this.dictionary = dictionary;
    prefixReader = dictionary.prefixes == null ? null : dictionary.prefixes.getBytesReader();
    suffixReader = dictionary.suffixes == null ? null : dictionary.suffixes.getBytesReader();
    for (int level = 0; level < 3; level++) {
      if (dictionary.prefixes != null) {
        prefixArcs[level] = new FST.Arc<>();
      }
      if (dictionary.suffixes != null) {
        suffixArcs[level] = new FST.Arc<>();
      }
    }
    formStep = dictionary.formStep();
  }

  /**
   * Find the stem(s) of the provided word.
   *
   * @param word Word to find the stems for
   * @return List of stems for the word
   */
  public List<CharsRef> stem(String word) {
    return stem(word.toCharArray(), word.length());
  }

  /**
   * Find the stem(s) of the provided word
   *
   * @param word Word to find the stems for
   * @return List of stems for the word
   */
  public List<CharsRef> stem(char[] word, int length) {

    if (dictionary.needsInputCleaning) {
      scratchSegment.setLength(0);
      scratchSegment.append(word, 0, length);
      CharSequence cleaned = dictionary.cleanInput(scratchSegment, segment);
      scratchBuffer = ArrayUtil.grow(scratchBuffer, cleaned.length());
      length = segment.length();
      segment.getChars(0, length, scratchBuffer, 0);
      word = scratchBuffer;
    }

    if (dictionary.isForbiddenWord(word, length)) {
      return Collections.emptyList();
    }

    List<CharsRef> list = new ArrayList<>();
    RootProcessor processor =
        (stem, forms, formID) -> {
          list.add(newStem(stem, forms, formID));
          return true;
        };

    if (!doStem(word, 0, length, null, WordContext.SIMPLE_WORD, processor)) {
      return list;
    }

    WordCase wordCase = caseOf(word, length);
    if (wordCase == WordCase.UPPER || wordCase == WordCase.TITLE) {
      addCaseVariations(word, length, wordCase, processor);
    }
    return list;
  }

  private void addCaseVariations(
      char[] word, int length, WordCase wordCase, RootProcessor processor) {
    if (wordCase == WordCase.UPPER) {
      caseFoldTitle(word, length);
      char[] aposCase = capitalizeAfterApostrophe(titleBuffer, length);
      if (aposCase != null) {
        if (!doStem(aposCase, 0, length, wordCase, WordContext.SIMPLE_WORD, processor)) {
          return;
        }
      }
      if (!doStem(titleBuffer, 0, length, wordCase, WordContext.SIMPLE_WORD, processor)) {
        return;
      }
      for (char[] variation : sharpSVariations(titleBuffer, length)) {
        if (!doStem(variation, 0, variation.length, null, WordContext.SIMPLE_WORD, processor)) {
          return;
        }
      }
    }

    if (dictionary.isDotICaseChangeDisallowed(word)) {
      return;
    }

    caseFoldLower(wordCase == WordCase.UPPER ? titleBuffer : word, length);
    if (!doStem(lowerBuffer, 0, length, wordCase, WordContext.SIMPLE_WORD, processor)) {
      return;
    }
    if (wordCase == WordCase.UPPER) {
      for (char[] variation : sharpSVariations(lowerBuffer, length)) {
        if (!doStem(variation, 0, variation.length, null, WordContext.SIMPLE_WORD, processor)) {
          return;
        }
      }
    }
  }

  // temporary buffers for case variants
  private char[] lowerBuffer = new char[8];
  private char[] titleBuffer = new char[8];

  /** returns EXACT_CASE,TITLE_CASE, or UPPER_CASE type for the word */
  WordCase caseOf(char[] word, int length) {
    if (dictionary.ignoreCase || length == 0 || Character.isLowerCase(word[0])) {
      return WordCase.MIXED;
    }

    return WordCase.caseOf(word, length);
  }

  /** folds titlecase variant of word to titleBuffer */
  char[] caseFoldTitle(char[] word, int length) {
    titleBuffer = ArrayUtil.grow(titleBuffer, length);
    System.arraycopy(word, 0, titleBuffer, 0, length);
    for (int i = 1; i < length; i++) {
      titleBuffer[i] = dictionary.caseFold(titleBuffer[i]);
    }
    return titleBuffer;
  }

  /** folds lowercase variant of word (title cased) to lowerBuffer */
  char[] caseFoldLower(char[] word, int length) {
    lowerBuffer = ArrayUtil.grow(lowerBuffer, length);
    System.arraycopy(word, 0, lowerBuffer, 0, length);
    lowerBuffer[0] = dictionary.caseFold(lowerBuffer[0]);
    return lowerBuffer;
  }

  // Special prefix handling for Catalan, French, Italian:
  // prefixes separated by apostrophe (SANT'ELIA -> Sant'+Elia).
  static char[] capitalizeAfterApostrophe(char[] word, int length) {
    for (int i = 1; i < length - 1; i++) {
      if (word[i] == '\'') {
        char next = word[i + 1];
        char upper = Character.toUpperCase(next);
        if (upper != next) {
          char[] copy = ArrayUtil.copyOfSubArray(word, 0, length);
          copy[i + 1] = Character.toUpperCase(upper);
          return copy;
        }
      }
    }
    return null;
  }

  List<char[]> sharpSVariations(char[] word, int length) {
    if (!dictionary.checkSharpS) return Collections.emptyList();

    Stream<String> result =
        new Object() {
          int findSS(int start) {
            for (int i = start; i < length - 1; i++) {
              if (word[i] == 's' && word[i + 1] == 's') {
                return i;
              }
            }
            return -1;
          }

          Stream<String> replaceSS(int start, int depth) {
            if (depth > 5) { // cut off too large enumeration
              return Stream.of(new String(word, start, length - start));
            }

            int ss = findSS(start);
            if (ss < 0) {
              return null;
            } else {
              String prefix = new String(word, start, ss - start);
              Stream<String> tails = replaceSS(ss + 2, depth + 1);
              if (tails == null) {
                tails = Stream.of(new String(word, ss + 2, length - ss - 2));
              }
              return tails.flatMap(s -> Stream.of(prefix + "ss" + s, prefix + "ß" + s));
            }
          }
        }.replaceSS(0, 0);
    if (result == null) return Collections.emptyList();

    String src = new String(word, 0, length);
    return result.filter(s -> !s.equals(src)).map(String::toCharArray).collect(Collectors.toList());
  }

  boolean doStem(
      char[] word,
      int offset,
      int length,
      WordCase originalCase,
      WordContext context,
      RootProcessor processor) {
    IntsRef forms = dictionary.lookupWord(word, offset, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i += formStep) {
        int entryId = forms.ints[forms.offset + i];
        if (!acceptCase(originalCase, entryId, word, offset, length)) {
          continue;
        }
        // we can't add this form, it's a pseudostem requiring an affix
        if (dictionary.hasFlag(entryId, dictionary.needaffix)) {
          continue;
        }
        // we can't add this form, it only belongs inside a compound word
        if (!context.isCompound() && dictionary.hasFlag(entryId, dictionary.onlyincompound)) {
          continue;
        }
        if (context.isCompound()) {
          if (context != WordContext.COMPOUND_END
              && dictionary.hasFlag(entryId, dictionary.compoundForbid)) {
            return false;
          }
          if (!dictionary.hasFlag(entryId, dictionary.compoundFlag)
              && !dictionary.hasFlag(entryId, context.requiredFlag(dictionary))) {
            continue;
          }
        }
        if (!processor.processRoot(new CharsRef(word, offset, length), forms, i)) {
          return false;
        }
      }
    }
    try {
      return stem(
          word,
          offset,
          length,
          context,
          -1,
          Dictionary.FLAG_UNSET,
          -1,
          0,
          true,
          true,
          false,
          false,
          originalCase,
          processor);
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
  }

  private boolean acceptCase(
      WordCase originalCase, int entryId, char[] word, int offset, int length) {
    boolean keepCase = dictionary.hasFlag(entryId, dictionary.keepcase);
    if (originalCase != null) {
      if (keepCase
          && dictionary.checkSharpS
          && originalCase == WordCase.TITLE
          && containsSharpS(word, offset, length)) {
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

  /**
   * Find the unique stem(s) of the provided word
   *
   * @param word Word to find the stems for
   * @return List of stems for the word
   */
  public List<CharsRef> uniqueStems(char[] word, int length) {
    List<CharsRef> stems = stem(word, length);
    if (stems.size() < 2) {
      return stems;
    }
    CharArraySet terms = new CharArraySet(8, dictionary.ignoreCase);
    List<CharsRef> deduped = new ArrayList<>();
    for (CharsRef s : stems) {
      if (!terms.contains(s)) {
        deduped.add(s);
        terms.add(s);
      }
    }
    return deduped;
  }

  interface RootProcessor {
    /** @return whether the processing should be continued */
    boolean processRoot(CharsRef stem, IntsRef forms, int formID);
  }

  private CharsRef newStem(CharsRef stem, IntsRef forms, int formID) {
    final String exception;
    if (dictionary.hasStemExceptions) {
      int exceptionID = forms.ints[forms.offset + formID + 1];
      if (exceptionID > 0) {
        exception = dictionary.getStemException(exceptionID);
      } else {
        exception = null;
      }
    } else {
      exception = null;
    }

    if (dictionary.needsOutputCleaning) {
      scratchSegment.setLength(0);
      if (exception != null) {
        scratchSegment.append(exception);
      } else {
        scratchSegment.append(stem.chars, stem.offset, stem.length);
      }
      try {
        Dictionary.applyMappings(dictionary.oconv, scratchSegment);
      } catch (IOException bogus) {
        throw new RuntimeException(bogus);
      }
      char[] cleaned = new char[scratchSegment.length()];
      scratchSegment.getChars(0, cleaned.length, cleaned, 0);
      return new CharsRef(cleaned, 0, cleaned.length);
    } else {
      if (exception != null) {
        return new CharsRef(exception);
      } else {
        return stem;
      }
    }
  }

  // some state for traversing FSTs
  private final FST.BytesReader prefixReader;
  private final FST.BytesReader suffixReader;

  @SuppressWarnings({"unchecked", "rawtypes"})
  private final FST.Arc<IntsRef>[] prefixArcs = new FST.Arc[3];

  @SuppressWarnings({"unchecked", "rawtypes"})
  private final FST.Arc<IntsRef>[] suffixArcs = new FST.Arc[3];

  /**
   * Generates a list of stems for the provided word
   *
   * @param word Word to generate the stems for
   * @param previous previous affix that was removed (so we dont remove same one twice)
   * @param prevFlag Flag from a previous stemming step that need to be cross-checked with any
   *     affixes in this recursive step
   * @param prefixId ID of the most inner removed prefix, so that when removing a suffix, it's also
   *     checked against the word
   * @param recursionDepth current recursiondepth
   * @param doPrefix true if we should remove prefixes
   * @param doSuffix true if we should remove suffixes
   * @param previousWasPrefix true if the previous removal was a prefix: if we are removing a
   *     suffix, and it has no continuation requirements, it's ok. but two prefixes
   *     (COMPLEXPREFIXES) or two suffixes must have continuation requirements to recurse.
   * @param circumfix true if the previous prefix removal was signed as a circumfix this means inner
   *     most suffix must also contain circumfix flag.
   * @param originalCase if non-null, represents original word case to disallow case variations of
   *     word with KEEPCASE flags
   * @return whether the processing should be continued
   */
  private boolean stem(
      char[] word,
      int offset,
      int length,
      WordContext context,
      int previous,
      char prevFlag,
      int prefixId,
      int recursionDepth,
      boolean doPrefix,
      boolean doSuffix,
      boolean previousWasPrefix,
      boolean circumfix,
      WordCase originalCase,
      RootProcessor processor)
      throws IOException {
    if (doPrefix && dictionary.prefixes != null) {
      FST<IntsRef> fst = dictionary.prefixes;
      FST.Arc<IntsRef> arc = prefixArcs[recursionDepth];
      fst.getFirstArc(arc);
      IntsRef NO_OUTPUT = fst.outputs.getNoOutput();
      IntsRef output = NO_OUTPUT;
      int limit = dictionary.fullStrip ? length + 1 : length;
      for (int i = 0; i < limit; i++) {
        if (i > 0) {
          char ch = word[offset + i - 1];
          if (fst.findTargetArc(ch, arc, arc, prefixReader) == null) {
            break;
          } else if (arc.output() != NO_OUTPUT) {
            output = fst.outputs.add(output, arc.output());
          }
        }
        if (!arc.isFinal()) {
          continue;
        }
        IntsRef prefixes = fst.outputs.add(output, arc.nextFinalOutput());

        for (int j = 0; j < prefixes.length; j++) {
          int prefix = prefixes.ints[prefixes.offset + j];
          if (prefix == previous) {
            continue;
          }

          if (isAffixCompatible(prefix, prevFlag, recursionDepth, true, false, context)) {
            char[] strippedWord = stripAffix(word, offset, length, i, prefix, true);
            if (strippedWord == null) {
              continue;
            }

            boolean pureAffix = strippedWord == word;
            if (!applyAffix(
                strippedWord,
                pureAffix ? offset + i : 0,
                pureAffix ? length - i : strippedWord.length,
                context,
                prefix,
                previous,
                -1,
                recursionDepth,
                true,
                circumfix,
                originalCase,
                processor)) {
              return false;
            }
          }
        }
      }
    }

    if (doSuffix && dictionary.suffixes != null) {
      FST<IntsRef> fst = dictionary.suffixes;
      FST.Arc<IntsRef> arc = suffixArcs[recursionDepth];
      fst.getFirstArc(arc);
      IntsRef NO_OUTPUT = fst.outputs.getNoOutput();
      IntsRef output = NO_OUTPUT;
      int limit = dictionary.fullStrip ? 0 : 1;
      for (int i = length; i >= limit; i--) {
        if (i < length) {
          char ch = word[offset + i];
          if (fst.findTargetArc(ch, arc, arc, suffixReader) == null) {
            break;
          } else if (arc.output() != NO_OUTPUT) {
            output = fst.outputs.add(output, arc.output());
          }
        }
        if (!arc.isFinal()) {
          continue;
        }
        IntsRef suffixes = fst.outputs.add(output, arc.nextFinalOutput());

        for (int j = 0; j < suffixes.length; j++) {
          int suffix = suffixes.ints[suffixes.offset + j];
          if (suffix == previous) {
            continue;
          }

          if (isAffixCompatible(
              suffix, prevFlag, recursionDepth, false, previousWasPrefix, context)) {
            char[] strippedWord = stripAffix(word, offset, length, length - i, suffix, false);
            if (strippedWord == null) {
              continue;
            }

            boolean pureAffix = strippedWord == word;
            if (!applyAffix(
                strippedWord,
                pureAffix ? offset : 0,
                pureAffix ? i : strippedWord.length,
                context,
                suffix,
                previous,
                prefixId,
                recursionDepth,
                false,
                circumfix,
                originalCase,
                processor)) {
              return false;
            }
          }
        }
      }
    }

    return true;
  }

  /**
   * @return null if affix conditions isn't met; a reference to the same char[] if the affix has no
   *     strip data and can thus be simply removed, or a new char[] containing the word affix
   *     removal
   */
  private char[] stripAffix(
      char[] word, int offset, int length, int affixLen, int affix, boolean isPrefix) {
    int deAffixedLen = length - affixLen;

    int stripOrd = dictionary.affixData(affix, Dictionary.AFFIX_STRIP_ORD);
    int stripStart = dictionary.stripOffsets[stripOrd];
    int stripEnd = dictionary.stripOffsets[stripOrd + 1];
    int stripLen = stripEnd - stripStart;

    char[] stripData = dictionary.stripData;
    boolean condition =
        isPrefix
            ? checkCondition(
                affix, stripData, stripStart, stripLen, word, offset + affixLen, deAffixedLen)
            : checkCondition(affix, word, offset, deAffixedLen, stripData, stripStart, stripLen);
    if (!condition) {
      return null;
    }

    if (stripLen == 0) return word;

    char[] strippedWord = new char[stripLen + deAffixedLen];
    System.arraycopy(
        word,
        offset + (isPrefix ? affixLen : 0),
        strippedWord,
        isPrefix ? stripLen : 0,
        deAffixedLen);
    System.arraycopy(stripData, stripStart, strippedWord, isPrefix ? 0 : deAffixedLen, stripLen);
    return strippedWord;
  }

  private boolean isAffixCompatible(
      int affix,
      char prevFlag,
      int recursionDepth,
      boolean isPrefix,
      boolean previousWasPrefix,
      WordContext context) {
    int append = dictionary.affixData(affix, Dictionary.AFFIX_APPEND);

    if (context.isCompound()) {
      if (!isPrefix && dictionary.hasFlag(append, dictionary.compoundForbid)) {
        return false;
      }
      WordContext allowed = isPrefix ? WordContext.COMPOUND_BEGIN : WordContext.COMPOUND_END;
      if (context != allowed && !dictionary.hasFlag(append, dictionary.compoundPermit)) {
        return false;
      }
      if (context == WordContext.COMPOUND_END
          && !isPrefix
          && !previousWasPrefix
          && dictionary.hasFlag(append, dictionary.onlyincompound)) {
        return false;
      }
    }

    if (recursionDepth == 0) {
      // check if affix is allowed in a non-compound word
      return context.isCompound() || !dictionary.hasFlag(append, dictionary.onlyincompound);
    }

    if (isCrossProduct(affix)) {
      // cross check incoming continuation class (flag of previous affix) against list.
      if (context.isCompound() || !dictionary.hasFlag(append, dictionary.onlyincompound)) {
        return previousWasPrefix || dictionary.hasFlag(append, prevFlag);
      }
    }

    return false;
  }

  /** checks condition of the concatenation of two strings */
  // note: this is pretty stupid, we really should subtract strip from the condition up front and
  // just check the stem
  // but this is a little bit more complicated.
  private boolean checkCondition(
      int affix, char[] c1, int c1off, int c1len, char[] c2, int c2off, int c2len) {
    int condition = dictionary.affixData(affix, Dictionary.AFFIX_CONDITION) >>> 1;
    if (condition != 0) {
      CharacterRunAutomaton pattern = dictionary.patterns.get(condition);
      int state = 0;
      for (int i = c1off; i < c1off + c1len; i++) {
        state = pattern.step(state, c1[i]);
        if (state == -1) {
          return false;
        }
      }
      for (int i = c2off; i < c2off + c2len; i++) {
        state = pattern.step(state, c2[i]);
        if (state == -1) {
          return false;
        }
      }
      return pattern.isAccept(state);
    }
    return true;
  }

  /**
   * Applies the affix rule to the given word, producing a list of stems if any are found
   *
   * @param strippedWord Char array containing the word with the affix removed and the strip added
   * @param offset where the word actually starts in the array
   * @param length the length of the stripped word
   * @param affix HunspellAffix representing the affix rule itself
   * @param prefixId when we already stripped a prefix, we can't simply recurse and check the
   *     suffix, unless both are compatible so we must check dictionary form against both to add it
   *     as a stem!
   * @param recursionDepth current recursion depth
   * @param prefix true if we are removing a prefix (false if it's a suffix)
   * @return whether the processing should be continued
   */
  private boolean applyAffix(
      char[] strippedWord,
      int offset,
      int length,
      WordContext context,
      int affix,
      int previousAffix,
      int prefixId,
      int recursionDepth,
      boolean prefix,
      boolean circumfix,
      WordCase originalCase,
      RootProcessor processor)
      throws IOException {
    char flag = dictionary.affixData(affix, Dictionary.AFFIX_FLAG);

    boolean skipLookup = needsAnotherAffix(affix, previousAffix, !prefix);
    IntsRef forms = skipLookup ? null : dictionary.lookupWord(strippedWord, offset, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i += formStep) {
        int entryId = forms.ints[forms.offset + i];
        if (dictionary.hasFlag(entryId, flag) || isFlagAppendedByAffix(prefixId, flag)) {
          // confusing: in this one exception, we already chained the first prefix against the
          // second,
          // so it doesnt need to be checked against the word
          boolean chainedPrefix = dictionary.complexPrefixes && recursionDepth == 1 && prefix;
          if (!chainedPrefix && prefixId >= 0) {
            char prefixFlag = dictionary.affixData(prefixId, Dictionary.AFFIX_FLAG);
            if (!dictionary.hasFlag(entryId, prefixFlag)
                && !isFlagAppendedByAffix(affix, prefixFlag)) {
              continue;
            }
          }

          // if circumfix was previously set by a prefix, we must check this suffix,
          // to ensure it has it, and vice versa
          if (dictionary.circumfix != Dictionary.FLAG_UNSET) {
            boolean suffixCircumfix = isFlagAppendedByAffix(affix, dictionary.circumfix);
            if (circumfix != suffixCircumfix) {
              continue;
            }
          }

          // we are looking for a case variant, but this word does not allow it
          if (!acceptCase(originalCase, entryId, strippedWord, offset, length)) {
            continue;
          }
          if (!context.isCompound() && dictionary.hasFlag(entryId, dictionary.onlyincompound)) {
            continue;
          }
          if (context.isCompound()) {
            char cFlag = context.requiredFlag(dictionary);
            if (!dictionary.hasFlag(entryId, cFlag)
                && !isFlagAppendedByAffix(affix, cFlag)
                && !dictionary.hasFlag(entryId, dictionary.compoundFlag)
                && !isFlagAppendedByAffix(affix, dictionary.compoundFlag)) {
              continue;
            }
          }
          if (!processor.processRoot(new CharsRef(strippedWord, offset, length), forms, i)) {
            return false;
          }
        }
      }
    }

    // if a circumfix flag is defined in the dictionary, and we are a prefix, we need to check if we
    // have that flag
    if (dictionary.circumfix != Dictionary.FLAG_UNSET && !circumfix && prefix) {
      circumfix = isFlagAppendedByAffix(affix, dictionary.circumfix);
    }

    if (isCrossProduct(affix) && recursionDepth <= 1) {
      boolean doPrefix;
      if (recursionDepth == 0) {
        if (prefix) {
          prefixId = affix;
          doPrefix = dictionary.complexPrefixes && dictionary.twoStageAffix;
          // we took away the first prefix.
          // COMPLEXPREFIXES = true:  combine with a second prefix and another suffix
          // COMPLEXPREFIXES = false: combine with a suffix
        } else if (!dictionary.complexPrefixes && dictionary.twoStageAffix) {
          doPrefix = false;
          // we took away a suffix.
          // COMPLEXPREFIXES = true: we don't recurse! only one suffix allowed
          // COMPLEXPREFIXES = false: combine with another suffix
        } else {
          return true;
        }
      } else {
        doPrefix = false;
        if (prefix && dictionary.complexPrefixes) {
          prefixId = affix;
          // we took away the second prefix: go look for another suffix
        } else if (prefix || dictionary.complexPrefixes || !dictionary.twoStageAffix) {
          return true;
        }
        // we took away a prefix, then a suffix: go look for another suffix
      }

      return stem(
          strippedWord,
          offset,
          length,
          context,
          affix,
          flag,
          prefixId,
          recursionDepth + 1,
          doPrefix,
          true,
          prefix,
          circumfix,
          originalCase,
          processor);
    }

    return true;
  }

  private boolean needsAnotherAffix(int affix, int previousAffix, boolean isSuffix) {
    if (isFlagAppendedByAffix(affix, dictionary.needaffix)) {
      return !isSuffix
          || previousAffix < 0
          || isFlagAppendedByAffix(previousAffix, dictionary.needaffix);
    }
    return false;
  }

  private boolean isFlagAppendedByAffix(int affixId, char flag) {
    if (affixId < 0 || flag == Dictionary.FLAG_UNSET) return false;
    int appendId = dictionary.affixData(affixId, Dictionary.AFFIX_APPEND);
    return dictionary.hasFlag(appendId, flag);
  }

  private boolean isCrossProduct(int affix) {
    return (dictionary.affixData(affix, Dictionary.AFFIX_CONDITION) & 1) == 1;
  }
}
