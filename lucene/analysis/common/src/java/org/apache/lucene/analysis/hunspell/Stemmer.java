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
import java.util.List;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
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
  private final BytesRef scratch = new BytesRef();
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

    WordCase wordCase = caseOf(word, length);
    List<CharsRef> list = doStem(word, 0, length, false, WordContext.SIMPLE_WORD);
    if (wordCase == WordCase.UPPER) {
      caseFoldTitle(word, length);
      char[] aposCase = capitalizeAfterApostrophe(titleBuffer, length);
      if (aposCase != null) {
        list.addAll(doStem(aposCase, 0, length, true, WordContext.SIMPLE_WORD));
      }
      list.addAll(doStem(titleBuffer, 0, length, true, WordContext.SIMPLE_WORD));
    }
    if (wordCase == WordCase.UPPER || wordCase == WordCase.TITLE) {
      caseFoldLower(wordCase == WordCase.UPPER ? titleBuffer : word, length);
      list.addAll(doStem(lowerBuffer, 0, length, true, WordContext.SIMPLE_WORD));
    }
    return list;
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

  List<CharsRef> doStem(
      char[] word, int offset, int length, boolean caseVariant, WordContext context) {
    List<CharsRef> stems = new ArrayList<>();
    IntsRef forms = dictionary.lookupWord(word, offset, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i += formStep) {
        char[] wordFlags = dictionary.decodeFlags(forms.ints[forms.offset + i], scratch);
        if (!acceptCase(caseVariant, wordFlags)) {
          continue;
        }
        // we can't add this form, it's a pseudostem requiring an affix
        if (Dictionary.hasFlag(wordFlags, dictionary.needaffix)) {
          continue;
        }
        // we can't add this form, it only belongs inside a compound word
        if (!context.isCompound() && Dictionary.hasFlag(wordFlags, dictionary.onlyincompound)) {
          continue;
        }
        if (context.isCompound()
            && !Dictionary.hasFlag(wordFlags, context.requiredFlag(dictionary))) {
          continue;
        }
        stems.add(newStem(word, offset, length, forms, i));
      }
    }
    try {
      stems.addAll(
          stem(
              word,
              offset,
              length,
              context,
              -1,
              (char) 0,
              -1,
              0,
              true,
              true,
              false,
              false,
              caseVariant));
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
    return stems;
  }

  private boolean acceptCase(boolean caseVariant, char[] wordFlags) {
    return caseVariant
        ? !Dictionary.hasFlag(wordFlags, dictionary.keepcase)
        : !Dictionary.hasHiddenFlag(wordFlags);
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

  private CharsRef newStem(char[] buffer, int offset, int length, IntsRef forms, int formID) {
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
        scratchSegment.append(buffer, offset, length);
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
        return new CharsRef(buffer, offset, length);
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
   * @param caseVariant true if we are searching for a case variant. if the word has KEEPCASE flag
   *     it cannot succeed.
   * @return List of stems, or empty list if no stems are found
   */
  private List<CharsRef> stem(
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
      boolean caseVariant)
      throws IOException {

    // TODO: allow this stuff to be reused by tokenfilter
    List<CharsRef> stems = new ArrayList<>();

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
            stems.addAll(
                applyAffix(
                    strippedWord,
                    pureAffix ? offset + i : 0,
                    pureAffix ? length - i : strippedWord.length,
                    context,
                    prefix,
                    -1,
                    recursionDepth,
                    true,
                    circumfix,
                    caseVariant));
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
            stems.addAll(
                applyAffix(
                    strippedWord,
                    pureAffix ? offset : 0,
                    pureAffix ? i : strippedWord.length,
                    context,
                    suffix,
                    prefixId,
                    recursionDepth,
                    false,
                    circumfix,
                    caseVariant));
          }
        }
      }
    }

    return stems;
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

    if (context.isCompound() && dictionary.compoundPermit > 0) {
      WordContext allowed = isPrefix ? WordContext.COMPOUND_BEGIN : WordContext.COMPOUND_END;
      if (context != allowed && !dictionary.hasFlag(append, dictionary.compoundPermit, scratch)) {
        return false;
      }
    }

    if (recursionDepth == 0) {
      // check if affix is allowed in a non-compound word
      return context.isCompound()
          || !dictionary.hasFlag(append, dictionary.onlyincompound, scratch);
    }

    if (isCrossProduct(affix)) {
      // cross check incoming continuation class (flag of previous affix) against list.
      char[] appendFlags = dictionary.decodeFlags(append, scratch);
      if (context.isCompound() || !Dictionary.hasFlag(appendFlags, dictionary.onlyincompound)) {
        return previousWasPrefix || Dictionary.hasFlag(appendFlags, prevFlag);
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
   * @return List of stems for the word, or an empty list if none are found
   */
  private List<CharsRef> applyAffix(
      char[] strippedWord,
      int offset,
      int length,
      WordContext context,
      int affix,
      int prefixId,
      int recursionDepth,
      boolean prefix,
      boolean circumfix,
      boolean caseVariant)
      throws IOException {
    char flag = dictionary.affixData(affix, Dictionary.AFFIX_FLAG);

    List<CharsRef> stems = new ArrayList<>();

    IntsRef forms = dictionary.lookupWord(strippedWord, offset, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i += formStep) {
        char[] wordFlags = dictionary.decodeFlags(forms.ints[forms.offset + i], scratch);
        if (Dictionary.hasFlag(wordFlags, flag) || isFlagAppendedByAffix(prefixId, flag)) {
          // confusing: in this one exception, we already chained the first prefix against the
          // second,
          // so it doesnt need to be checked against the word
          boolean chainedPrefix = dictionary.complexPrefixes && recursionDepth == 1 && prefix;
          if (!chainedPrefix && prefixId >= 0) {
            char prefixFlag = dictionary.affixData(prefixId, Dictionary.AFFIX_FLAG);
            if (!Dictionary.hasFlag(wordFlags, prefixFlag)
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
          if (!acceptCase(caseVariant, wordFlags)) {
            continue;
          }
          if (!context.isCompound() && Dictionary.hasFlag(wordFlags, dictionary.onlyincompound)) {
            continue;
          }
          if (context.isCompound()) {
            char cFlag = context.requiredFlag(dictionary);
            if (!Dictionary.hasFlag(wordFlags, cFlag) && !isFlagAppendedByAffix(affix, cFlag)) {
              continue;
            }
          }
          stems.add(newStem(strippedWord, offset, length, forms, i));
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
          return stems;
        }
      } else {
        doPrefix = false;
        if (prefix && dictionary.complexPrefixes) {
          prefixId = affix;
          // we took away the second prefix: go look for another suffix
        } else if (prefix || dictionary.complexPrefixes || !dictionary.twoStageAffix) {
          return stems;
        }
        // we took away a prefix, then a suffix: go look for another suffix
      }

      stems.addAll(
          stem(
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
              caseVariant));
    }

    return stems;
  }

  private boolean isFlagAppendedByAffix(int affixId, char flag) {
    if (affixId < 0 || flag == Dictionary.FLAG_UNSET) return false;
    int appendId = dictionary.affixData(affixId, Dictionary.AFFIX_APPEND);
    return dictionary.hasFlag(appendId, flag, scratch);
  }

  private boolean isCrossProduct(int affix) {
    return (dictionary.affixData(affix, Dictionary.AFFIX_CONDITION) & 1) == 1;
  }
}
