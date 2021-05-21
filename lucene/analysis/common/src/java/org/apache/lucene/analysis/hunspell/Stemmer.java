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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;

/**
 * Stemmer uses the affix rules declared in the Dictionary to generate one or more stems for a word.
 * It conforms to the algorithm in the original hunspell algorithm, including recursive suffix
 * stripping.
 */
final class Stemmer {
  private final Dictionary dictionary;

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

    if (dictionary.mayNeedInputCleaning()) {
      CharsRef scratchSegment = new CharsRef(word, 0, length);
      if (dictionary.needsInputCleaning(scratchSegment)) {
        StringBuilder segment = new StringBuilder();
        dictionary.cleanInput(scratchSegment, segment);
        char[] scratchBuffer = new char[segment.length()];
        length = segment.length();
        segment.getChars(0, length, scratchBuffer, 0);
        word = scratchBuffer;
      }
    }

    List<CharsRef> list = new ArrayList<>();
    if (length == 0) {
      return list;
    }

    RootProcessor processor =
        (stem, formID, stemException) -> {
          list.add(newStem(stem, stemException));
          return true;
        };
    if (!doStem(word, 0, length, WordContext.SIMPLE_WORD, processor)) {
      return list;
    }

    WordCase wordCase = caseOf(word, length);
    if (wordCase == WordCase.UPPER || wordCase == WordCase.TITLE) {
      CaseVariationProcessor variationProcessor =
          (variant, varLength, originalCase) ->
              doStem(variant, 0, varLength, WordContext.SIMPLE_WORD, processor);
      varyCase(word, length, wordCase, variationProcessor);
    }
    return list;
  }

  interface CaseVariationProcessor {
    boolean process(char[] word, int length, WordCase originalCase);
  }

  boolean varyCase(char[] word, int length, WordCase wordCase, CaseVariationProcessor processor) {
    char[] titleBuffer = wordCase == WordCase.UPPER ? caseFoldTitle(word, length) : null;
    if (wordCase == WordCase.UPPER) {
      char[] aposCase = capitalizeAfterApostrophe(titleBuffer, length);
      if (aposCase != null && !processor.process(aposCase, length, wordCase)) {
        return false;
      }
      if (!processor.process(titleBuffer, length, wordCase)) {
        return false;
      }
      if (dictionary.checkSharpS && !varySharpS(titleBuffer, length, processor)) {
        return false;
      }
    }

    if (dictionary.isDotICaseChangeDisallowed(word)) {
      return true;
    }

    char[] lowerBuffer = caseFoldLower(titleBuffer != null ? titleBuffer : word, length);
    if (!processor.process(lowerBuffer, length, wordCase)) {
      return false;
    }
    if (wordCase == WordCase.UPPER
        && dictionary.checkSharpS
        && !varySharpS(lowerBuffer, length, processor)) {
      return false;
    }
    return true;
  }

  /** returns EXACT_CASE,TITLE_CASE, or UPPER_CASE type for the word */
  WordCase caseOf(char[] word, int length) {
    if (dictionary.ignoreCase || length == 0 || Character.isLowerCase(word[0])) {
      return WordCase.MIXED;
    }

    return WordCase.caseOf(word, length);
  }

  /** folds titlecase variant of word to titleBuffer */
  private char[] caseFoldTitle(char[] word, int length) {
    char[] titleBuffer = new char[length];
    System.arraycopy(word, 0, titleBuffer, 0, length);
    for (int i = 1; i < length; i++) {
      titleBuffer[i] = dictionary.caseFold(titleBuffer[i]);
    }
    return titleBuffer;
  }

  /** folds lowercase variant of word (title cased) to lowerBuffer */
  private char[] caseFoldLower(char[] word, int length) {
    char[] lowerBuffer = new char[length];
    System.arraycopy(word, 0, lowerBuffer, 0, length);
    lowerBuffer[0] = dictionary.caseFold(lowerBuffer[0]);
    return lowerBuffer;
  }

  // Special prefix handling for Catalan, French, Italian:
  // prefixes separated by apostrophe (SANT'ELIA -> Sant'+Elia).
  private static char[] capitalizeAfterApostrophe(char[] word, int length) {
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

  private boolean varySharpS(char[] word, int length, CaseVariationProcessor processor) {
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
    if (result == null) return true;

    String src = new String(word, 0, length);
    for (String s : result.collect(Collectors.toList())) {
      if (!s.equals(src) && !processor.process(s.toCharArray(), s.length(), null)) {
        return false;
      }
    }
    return true;
  }

  boolean doStem(
      char[] word, int offset, int length, WordContext context, RootProcessor processor) {
    IntsRef forms = dictionary.lookupWord(word, offset, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i += formStep) {
        int entryId = forms.ints[forms.offset + i];
        // we can't add this form, it's a pseudostem requiring an affix
        if (dictionary.hasFlag(entryId, dictionary.needaffix)) {
          continue;
        }
        if ((context == WordContext.COMPOUND_BEGIN || context == WordContext.COMPOUND_MIDDLE)
            && dictionary.hasFlag(entryId, dictionary.compoundForbid)) {
          return false;
        }
        if (!isRootCompatibleWithContext(context, -1, entryId)) {
          continue;
        }
        if (!callProcessor(word, offset, length, processor, forms, i)) {
          return false;
        }
      }
    }
    return stem(
        word, offset, length, context, -1, Dictionary.FLAG_UNSET, -1, 0, true, false, processor);
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
    /**
     * @param stem the text of the found dictionary entry
     * @param formID internal id of the dictionary entry, e.g. to be used in {@link
     *     Dictionary#hasFlag(int, char)}
     * @param morphDataId the id of the custom morphological data (0 if none), to be used with
     *     {@link Dictionary#morphData}
     * @return whether the processing should be continued
     */
    boolean processRoot(CharsRef stem, int formID, int morphDataId);
  }

  private String stemException(int morphDataId) {
    if (morphDataId > 0) {
      String data = dictionary.morphData.get(morphDataId);
      int start = data.startsWith("st:") ? 0 : data.indexOf(" st:");
      if (start >= 0) {
        int nextSpace = data.indexOf(' ', start + 3);
        return data.substring(start + 3, nextSpace < 0 ? data.length() : nextSpace);
      }
    }
    return null;
  }

  private CharsRef newStem(CharsRef stem, int morphDataId) {
    String exception = stemException(morphDataId);

    if (dictionary.oconv != null) {
      StringBuilder scratchSegment = new StringBuilder();
      if (exception != null) {
        scratchSegment.append(exception);
      } else {
        scratchSegment.append(stem.chars, stem.offset, stem.length);
      }
      dictionary.oconv.applyMappings(scratchSegment);
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
   * @param previousWasPrefix true if the previous removal was a prefix: if we are removing a
   *     suffix, and it has no continuation requirements, it's ok. but two prefixes
   *     (COMPLEXPREFIXES) or two suffixes must have continuation requirements to recurse.
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
      boolean previousWasPrefix,
      RootProcessor processor) {
    FST.Arc<IntsRef> arc = new FST.Arc<>();
    if (doPrefix && dictionary.prefixes != null) {
      FST<IntsRef> fst = dictionary.prefixes;
      FST.BytesReader reader = fst.getBytesReader();
      fst.getFirstArc(arc);
      IntsRef output = fst.outputs.getNoOutput();
      int limit = dictionary.fullStrip ? length + 1 : length;
      for (int i = 0; i < limit; i++) {
        if (i > 0) {
          output = Dictionary.nextArc(fst, arc, reader, output, word[offset + i - 1]);
          if (output == null) {
            break;
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
                processor)) {
              return false;
            }
          }
        }
      }
    }

    if (dictionary.suffixes != null) {
      FST<IntsRef> fst = dictionary.suffixes;
      FST.BytesReader reader = fst.getBytesReader();
      fst.getFirstArc(arc);
      IntsRef output = fst.outputs.getNoOutput();
      int limit = dictionary.fullStrip ? 0 : 1;
      for (int i = length; i >= limit; i--) {
        if (i < length) {
          output = Dictionary.nextArc(fst, arc, reader, output, word[offset + i]);
          if (output == null) {
            break;
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

    if (stripLen + deAffixedLen == 0) return null;

    char[] stripData = dictionary.stripData;
    int condition = dictionary.getAffixCondition(affix);
    if (condition != 0) {
      int deAffixedOffset = isPrefix ? offset + affixLen : offset;
      if (!dictionary.patterns.get(condition).acceptsStem(word, deAffixedOffset, deAffixedLen)) {
        return null;
      }
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
      if (!context.isAffixAllowedWithoutSpecialPermit(isPrefix)
          && !dictionary.hasFlag(append, dictionary.compoundPermit)) {
        return false;
      }
      if (context == WordContext.COMPOUND_END
          && !isPrefix
          && !previousWasPrefix
          && dictionary.hasFlag(append, dictionary.onlyincompound)) {
        return false;
      }
    } else if (dictionary.hasFlag(append, dictionary.onlyincompound)) {
      return false;
    }

    if (recursionDepth == 0) {
      return true;
    }

    if (dictionary.isCrossProduct(affix)) {
      // cross check incoming continuation class (flag of previous affix) against list.
      return previousWasPrefix || dictionary.hasFlag(append, prevFlag);
    }

    return false;
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
      RootProcessor processor) {
    char flag = dictionary.affixData(affix, Dictionary.AFFIX_FLAG);

    boolean skipLookup = needsAnotherAffix(affix, previousAffix, !prefix, prefixId);
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

          if (!isRootCompatibleWithContext(context, affix, entryId)) {
            continue;
          }

          if (!callProcessor(strippedWord, offset, length, processor, forms, i)) {
            return false;
          }
        }
      }
    }

    if (dictionary.isCrossProduct(affix) && recursionDepth <= 1) {
      boolean doPrefix;
      if (recursionDepth == 0) {
        if (prefix) {
          prefixId = affix;
          doPrefix = dictionary.complexPrefixes && dictionary.isSecondStagePrefix(flag);
          // we took away the first prefix.
          // COMPLEXPREFIXES = true:  combine with a second prefix and another suffix
          // COMPLEXPREFIXES = false: combine with a suffix
        } else if (!dictionary.complexPrefixes && dictionary.isSecondStageSuffix(flag)) {
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
        } else if (prefix || dictionary.complexPrefixes || !dictionary.isSecondStageSuffix(flag)) {
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
          prefix,
          processor);
    }

    return true;
  }

  private boolean isRootCompatibleWithContext(WordContext context, int lastAffix, int entryId) {
    if (!context.isCompound() && dictionary.hasFlag(entryId, dictionary.onlyincompound)) {
      return false;
    }
    if (context.isCompound() && context != WordContext.COMPOUND_RULE_END) {
      char cFlag = context.requiredFlag(dictionary);
      return dictionary.hasFlag(entryId, cFlag)
          || isFlagAppendedByAffix(lastAffix, cFlag)
          || dictionary.hasFlag(entryId, dictionary.compoundFlag)
          || isFlagAppendedByAffix(lastAffix, dictionary.compoundFlag);
    }
    return true;
  }

  private boolean callProcessor(
      char[] word, int offset, int length, RootProcessor processor, IntsRef forms, int i) {
    CharsRef stem = new CharsRef(word, offset, length);
    int morphDataId = dictionary.hasCustomMorphData ? forms.ints[forms.offset + i + 1] : 0;
    return processor.processRoot(stem, forms.ints[forms.offset + i], morphDataId);
  }

  private boolean needsAnotherAffix(int affix, int previousAffix, boolean isSuffix, int prefixId) {
    char circumfix = dictionary.circumfix;
    // if circumfix was previously set by a prefix, we must check this suffix,
    // to ensure it has it, and vice versa
    if (isSuffix
        && isFlagAppendedByAffix(prefixId, circumfix) != isFlagAppendedByAffix(affix, circumfix)) {
      return true;
    }
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
}
