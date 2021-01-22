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
import org.apache.lucene.util.fst.Outputs;

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
    for (int level = 0; level < 3; level++) {
      if (dictionary.prefixes != null) {
        prefixArcs[level] = new FST.Arc<>();
        prefixReaders[level] = dictionary.prefixes.getBytesReader();
      }
      if (dictionary.suffixes != null) {
        suffixArcs[level] = new FST.Arc<>();
        suffixReaders[level] = dictionary.suffixes.getBytesReader();
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
    List<CharsRef> list = doStem(word, length, false);
    if (wordCase == WordCase.UPPER) {
      caseFoldTitle(word, length);
      char[] aposCase = capitalizeAfterApostrophe(titleBuffer, length);
      if (aposCase != null) {
        list.addAll(doStem(aposCase, length, true));
      }
      list.addAll(doStem(titleBuffer, length, true));
    }
    if (wordCase == WordCase.UPPER || wordCase == WordCase.TITLE) {
      caseFoldLower(wordCase == WordCase.UPPER ? titleBuffer : word, length);
      list.addAll(doStem(lowerBuffer, length, true));
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
  char[] capitalizeAfterApostrophe(char[] word, int length) {
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

  List<CharsRef> doStem(char[] word, int length, boolean caseVariant) {
    List<CharsRef> stems = new ArrayList<>();
    IntsRef forms = dictionary.lookupWord(word, 0, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i += formStep) {
        char[] wordFlags = dictionary.decodeFlags(forms.ints[forms.offset + i], scratch);
        if (!acceptCase(caseVariant, wordFlags)) {
          continue;
        }
        // we can't add this form, it's a pseudostem requiring an affix
        if (dictionary.needaffix != -1
            && Dictionary.hasFlag(wordFlags, (char) dictionary.needaffix)) {
          continue;
        }
        // we can't add this form, it only belongs inside a compound word
        if (dictionary.onlyincompound != -1
            && Dictionary.hasFlag(wordFlags, (char) dictionary.onlyincompound)) {
          continue;
        }
        stems.add(newStem(word, length, forms, i));
      }
    }
    try {
      stems.addAll(stem(word, length, -1, -1, -1, 0, true, true, false, false, caseVariant));
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
    return stems;
  }

  private boolean acceptCase(boolean caseVariant, char[] wordFlags) {
    return caseVariant
        ? dictionary.keepcase == -1 || !Dictionary.hasFlag(wordFlags, (char) dictionary.keepcase)
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

  private CharsRef newStem(char[] buffer, int length, IntsRef forms, int formID) {
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
        scratchSegment.append(buffer, 0, length);
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
        return new CharsRef(buffer, 0, length);
      }
    }
  }

  // some state for traversing FSTs
  private final FST.BytesReader[] prefixReaders = new FST.BytesReader[3];

  @SuppressWarnings({"unchecked", "rawtypes"})
  private final FST.Arc<IntsRef>[] prefixArcs = new FST.Arc[3];

  private final FST.BytesReader[] suffixReaders = new FST.BytesReader[3];

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
      int length,
      int previous,
      int prevFlag,
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
      Outputs<IntsRef> outputs = fst.outputs;
      FST.BytesReader bytesReader = prefixReaders[recursionDepth];
      FST.Arc<IntsRef> arc = prefixArcs[recursionDepth];
      fst.getFirstArc(arc);
      IntsRef NO_OUTPUT = outputs.getNoOutput();
      IntsRef output = NO_OUTPUT;
      int limit = dictionary.fullStrip ? length + 1 : length;
      for (int i = 0; i < limit; i++) {
        if (i > 0) {
          int ch = word[i - 1];
          if (fst.findTargetArc(ch, arc, arc, bytesReader) == null) {
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

          if (isAffixCompatible(prefix, prevFlag, recursionDepth, false)) {
            int deAffixedLength = length - i;

            int stripOrd = dictionary.affixData(prefix, Dictionary.AFFIX_STRIP_ORD);
            int stripStart = dictionary.stripOffsets[stripOrd];
            int stripEnd = dictionary.stripOffsets[stripOrd + 1];
            int stripLength = stripEnd - stripStart;

            if (!checkCondition(
                prefix, dictionary.stripData, stripStart, stripLength, word, i, deAffixedLength)) {
              continue;
            }

            char[] strippedWord = new char[stripLength + deAffixedLength];
            System.arraycopy(dictionary.stripData, stripStart, strippedWord, 0, stripLength);
            System.arraycopy(word, i, strippedWord, stripLength, deAffixedLength);

            List<CharsRef> stemList =
                applyAffix(
                    strippedWord,
                    strippedWord.length,
                    prefix,
                    -1,
                    recursionDepth,
                    true,
                    circumfix,
                    caseVariant);

            stems.addAll(stemList);
          }
        }
      }
    }

    if (doSuffix && dictionary.suffixes != null) {
      FST<IntsRef> fst = dictionary.suffixes;
      Outputs<IntsRef> outputs = fst.outputs;
      FST.BytesReader bytesReader = suffixReaders[recursionDepth];
      FST.Arc<IntsRef> arc = suffixArcs[recursionDepth];
      fst.getFirstArc(arc);
      IntsRef NO_OUTPUT = outputs.getNoOutput();
      IntsRef output = NO_OUTPUT;
      int limit = dictionary.fullStrip ? 0 : 1;
      for (int i = length; i >= limit; i--) {
        if (i < length) {
          int ch = word[i];
          if (fst.findTargetArc(ch, arc, arc, bytesReader) == null) {
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

          if (isAffixCompatible(suffix, prevFlag, recursionDepth, previousWasPrefix)) {
            int appendLength = length - i;
            int deAffixedLength = length - appendLength;

            int stripOrd = dictionary.affixData(suffix, Dictionary.AFFIX_STRIP_ORD);
            int stripStart = dictionary.stripOffsets[stripOrd];
            int stripEnd = dictionary.stripOffsets[stripOrd + 1];
            int stripLength = stripEnd - stripStart;

            if (!checkCondition(
                suffix, word, 0, deAffixedLength, dictionary.stripData, stripStart, stripLength)) {
              continue;
            }

            char[] strippedWord = new char[stripLength + deAffixedLength];
            System.arraycopy(word, 0, strippedWord, 0, deAffixedLength);
            System.arraycopy(
                dictionary.stripData, stripStart, strippedWord, deAffixedLength, stripLength);

            List<CharsRef> stemList =
                applyAffix(
                    strippedWord,
                    strippedWord.length,
                    suffix,
                    prefixId,
                    recursionDepth,
                    false,
                    circumfix,
                    caseVariant);

            stems.addAll(stemList);
          }
        }
      }
    }

    return stems;
  }

  private boolean isAffixCompatible(
      int affix, int prevFlag, int recursionDepth, boolean previousWasPrefix) {
    int append = dictionary.affixData(affix, Dictionary.AFFIX_APPEND);

    if (recursionDepth == 0) {
      if (dictionary.onlyincompound == -1) {
        return true;
      }

      // check if affix is allowed in a non-compound word
      return !dictionary.hasFlag(append, (char) dictionary.onlyincompound, scratch);
    }

    if (isCrossProduct(affix)) {
      // cross check incoming continuation class (flag of previous affix) against list.
      char[] appendFlags = dictionary.decodeFlags(append, scratch);
      assert prevFlag >= 0;
      boolean allowed =
          dictionary.onlyincompound == -1
              || !Dictionary.hasFlag(appendFlags, (char) dictionary.onlyincompound);
      if (allowed) {
        return previousWasPrefix || Dictionary.hasFlag(appendFlags, (char) prevFlag);
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
   * @param strippedWord Word the affix has been removed and the strip added
   * @param length valid length of stripped word
   * @param affix HunspellAffix representing the affix rule itself
   * @param prefixId when we already stripped a prefix, we cant simply recurse and check the suffix,
   *     unless both are compatible so we must check dictionary form against both to add it as a
   *     stem!
   * @param recursionDepth current recursion depth
   * @param prefix true if we are removing a prefix (false if it's a suffix)
   * @return List of stems for the word, or an empty list if none are found
   */
  private List<CharsRef> applyAffix(
      char[] strippedWord,
      int length,
      int affix,
      int prefixId,
      int recursionDepth,
      boolean prefix,
      boolean circumfix,
      boolean caseVariant)
      throws IOException {
    char flag = dictionary.affixData(affix, Dictionary.AFFIX_FLAG);

    List<CharsRef> stems = new ArrayList<>();

    IntsRef forms = dictionary.lookupWord(strippedWord, 0, length);
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
          if (dictionary.circumfix != -1) {
            boolean suffixCircumfix = isFlagAppendedByAffix(affix, (char) dictionary.circumfix);
            if (circumfix != suffixCircumfix) {
              continue;
            }
          }

          // we are looking for a case variant, but this word does not allow it
          if (!acceptCase(caseVariant, wordFlags)) {
            continue;
          }
          // we aren't decompounding (yet)
          if (dictionary.onlyincompound != -1
              && Dictionary.hasFlag(wordFlags, (char) dictionary.onlyincompound)) {
            continue;
          }
          stems.add(newStem(strippedWord, length, forms, i));
        }
      }
    }

    // if a circumfix flag is defined in the dictionary, and we are a prefix, we need to check if we
    // have that flag
    if (dictionary.circumfix != -1 && !circumfix && prefix) {
      circumfix = isFlagAppendedByAffix(affix, (char) dictionary.circumfix);
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
              length,
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
    if (affixId < 0) return false;
    int appendId = dictionary.affixData(affixId, Dictionary.AFFIX_APPEND);
    return dictionary.hasFlag(appendId, flag, scratch);
  }

  private boolean isCrossProduct(int affix) {
    return (dictionary.affixData(affix, Dictionary.AFFIX_CONDITION) & 1) == 1;
  }
}
