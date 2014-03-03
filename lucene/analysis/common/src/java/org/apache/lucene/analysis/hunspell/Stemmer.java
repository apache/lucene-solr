package org.apache.lucene.analysis.hunspell;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.Version;

/**
 * Stemmer uses the affix rules declared in the Dictionary to generate one or more stems for a word.  It
 * conforms to the algorithm in the original hunspell algorithm, including recursive suffix stripping.
 */
final class Stemmer {
  private final Dictionary dictionary;
  private final BytesRef scratch = new BytesRef();
  private final StringBuilder segment = new StringBuilder();
  private final ByteArrayDataInput affixReader;
  private final CharacterUtils charUtils = CharacterUtils.getInstance(Version.LUCENE_CURRENT);

  /**
   * Constructs a new Stemmer which will use the provided Dictionary to create its stems.
   *
   * @param dictionary Dictionary that will be used to create the stems
   */
  public Stemmer(Dictionary dictionary) {
    this.dictionary = dictionary;
    this.affixReader = new ByteArrayDataInput(dictionary.affixData);
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
  public List<CharsRef> stem(char word[], int length) {
    if (dictionary.ignoreCase) {
      charUtils.toLowerCase(word, 0, length);
    }
    List<CharsRef> stems = new ArrayList<CharsRef>();
    IntsRef forms = dictionary.lookupWord(word, 0, length);
    if (forms != null) {
      // TODO: some forms should not be added, e.g. ONLYINCOMPOUND
      // just because it exists, does not make it valid...
      for (int i = 0; i < forms.length; i++) {
        stems.add(new CharsRef(word, 0, length));
      }
    }
    stems.addAll(stem(word, length, -1, -1, -1, 0, true, true, false, false));
    return stems;
  }
  
  /**
   * Find the unique stem(s) of the provided word
   * 
   * @param word Word to find the stems for
   * @return List of stems for the word
   */
  public List<CharsRef> uniqueStems(char word[], int length) {
    List<CharsRef> stems = stem(word, length);
    if (stems.size() < 2) {
      return stems;
    }
    CharArraySet terms = new CharArraySet(Version.LUCENE_CURRENT, 8, dictionary.ignoreCase);
    List<CharsRef> deduped = new ArrayList<>();
    for (CharsRef s : stems) {
      if (!terms.contains(s)) {
        deduped.add(s);
        terms.add(s);
      }
    }
    return deduped;
  }

  // ================================================= Helper Methods ================================================

  /**
   * Generates a list of stems for the provided word
   *
   * @param word Word to generate the stems for
   * @param previous previous affix that was removed (so we dont remove same one twice)
   * @param prevFlag Flag from a previous stemming step that need to be cross-checked with any affixes in this recursive step
   * @param prefixFlag flag of the most inner removed prefix, so that when removing a suffix, its also checked against the word
   * @param recursionDepth current recursiondepth
   * @param doPrefix true if we should remove prefixes
   * @param doSuffix true if we should remove suffixes
   * @param previousWasPrefix true if the previous removal was a prefix:
   *        if we are removing a suffix, and it has no continuation requirements, its ok.
   *        but two prefixes (COMPLEXPREFIXES) or two suffixes must have continuation requirements to recurse. 
   * @param circumfix true if the previous prefix removal was signed as a circumfix
   *        this means inner most suffix must also contain circumfix flag.
   * @return List of stems, or empty list if no stems are found
   */
  private List<CharsRef> stem(char word[], int length, int previous, int prevFlag, int prefixFlag, int recursionDepth, boolean doPrefix, boolean doSuffix, boolean previousWasPrefix, boolean circumfix) {
    
    // TODO: allow this stuff to be reused by tokenfilter
    List<CharsRef> stems = new ArrayList<CharsRef>();
    
    if (doPrefix) {
      for (int i = length - 1; i >= 0; i--) {
        IntsRef prefixes = dictionary.lookupPrefix(word, 0, i);
        if (prefixes == null) {
          continue;
        }
        
        for (int j = 0; j < prefixes.length; j++) {
          int prefix = prefixes.ints[prefixes.offset + j];
          if (prefix == previous) {
            continue;
          }
          affixReader.setPosition(8 * prefix);
          char flag = (char) (affixReader.readShort() & 0xffff);
          char stripOrd = (char) (affixReader.readShort() & 0xffff);
          int condition = (char) (affixReader.readShort() & 0xffff);
          boolean crossProduct = (condition & 1) == 1;
          condition >>>= 1;
          char append = (char) (affixReader.readShort() & 0xffff);
          
          final boolean compatible;
          if (recursionDepth == 0) {
            compatible = true;
          } else if (crossProduct) {
            // cross check incoming continuation class (flag of previous affix) against list.
            dictionary.flagLookup.get(append, scratch);
            char appendFlags[] = Dictionary.decodeFlags(scratch);
            assert prevFlag >= 0;
            compatible = hasCrossCheckedFlag((char)prevFlag, appendFlags, false);
          } else {
            compatible = false;
          }
          
          if (compatible) {
            int deAffixedStart = i;
            int deAffixedLength = length - deAffixedStart;
            
            dictionary.stripLookup.get(stripOrd, scratch);
            String strippedWord = new StringBuilder().append(scratch.utf8ToString())
                .append(word, deAffixedStart, deAffixedLength)
                .toString();
            
            List<CharsRef> stemList = applyAffix(strippedWord.toCharArray(), strippedWord.length(), prefix, -1, recursionDepth, true, circumfix);
            
            stems.addAll(stemList);
          }
        }
      }
    } 
    
    if (doSuffix) {
      for (int i = 0; i < length; i++) {
        IntsRef suffixes = dictionary.lookupSuffix(word, i, length - i);
        if (suffixes == null) {
          continue;
        }
        
        for (int j = 0; j < suffixes.length; j++) {
          int suffix = suffixes.ints[suffixes.offset + j];
          if (suffix == previous) {
            continue;
          }
          affixReader.setPosition(8 * suffix);
          char flag = (char) (affixReader.readShort() & 0xffff);
          char stripOrd = (char) (affixReader.readShort() & 0xffff);
          int condition = (char) (affixReader.readShort() & 0xffff);
          boolean crossProduct = (condition & 1) == 1;
          condition >>>= 1;
          char append = (char) (affixReader.readShort() & 0xffff);
          
          final boolean compatible;
          if (recursionDepth == 0) {
            compatible = true;
          } else if (crossProduct) {
            // cross check incoming continuation class (flag of previous affix) against list.
            dictionary.flagLookup.get(append, scratch);
            char appendFlags[] = Dictionary.decodeFlags(scratch);
            assert prevFlag >= 0;
            compatible = hasCrossCheckedFlag((char)prevFlag, appendFlags, previousWasPrefix);
          } else {
            compatible = false;
          }
          
          if (compatible) {
            int appendLength = length - i;
            int deAffixedLength = length - appendLength;
            // TODO: can we do this in-place?
            dictionary.stripLookup.get(stripOrd, scratch);
            String strippedWord = new StringBuilder().append(word, 0, deAffixedLength).append(scratch.utf8ToString()).toString();
            
            List<CharsRef> stemList = applyAffix(strippedWord.toCharArray(), strippedWord.length(), suffix, prefixFlag, recursionDepth, false, circumfix);
            
            stems.addAll(stemList);
          }
        }
      }
    }

    return stems;
  }

  /**
   * Applies the affix rule to the given word, producing a list of stems if any are found
   *
   * @param strippedWord Word the affix has been removed and the strip added
   * @param length valid length of stripped word
   * @param affix HunspellAffix representing the affix rule itself
   * @param prefixFlag when we already stripped a prefix, we cant simply recurse and check the suffix, unless both are compatible
   *                   so we must check dictionary form against both to add it as a stem!
   * @param recursionDepth current recursion depth
   * @param prefix true if we are removing a prefix (false if its a suffix)
   * @return List of stems for the word, or an empty list if none are found
   */
  List<CharsRef> applyAffix(char strippedWord[], int length, int affix, int prefixFlag, int recursionDepth, boolean prefix, boolean circumfix) {
    segment.setLength(0);
    segment.append(strippedWord, 0, length);
    
    // TODO: just pass this in from before, no need to decode it twice
    affixReader.setPosition(8 * affix);
    char flag = (char) (affixReader.readShort() & 0xffff);
    affixReader.skipBytes(2); // strip
    int condition = (char) (affixReader.readShort() & 0xffff);
    boolean crossProduct = (condition & 1) == 1;
    condition >>>= 1;
    char append = (char) (affixReader.readShort() & 0xffff);
    
    Pattern pattern = dictionary.patterns.get(condition);
    if (!pattern.matcher(segment).matches()) {
      return Collections.emptyList();
    }

    List<CharsRef> stems = new ArrayList<CharsRef>();

    IntsRef forms = dictionary.lookupWord(strippedWord, 0, length);
    if (forms != null) {
      for (int i = 0; i < forms.length; i++) {
        dictionary.flagLookup.get(forms.ints[forms.offset+i], scratch);
        char wordFlags[] = Dictionary.decodeFlags(scratch);
        if (Dictionary.hasFlag(wordFlags, flag)) {
          // confusing: in this one exception, we already chained the first prefix against the second,
          // so it doesnt need to be checked against the word
          boolean chainedPrefix = dictionary.complexPrefixes && recursionDepth == 1 && prefix;
          if (chainedPrefix == false && prefixFlag >= 0 && !Dictionary.hasFlag(wordFlags, (char)prefixFlag)) {
            // see if we can chain prefix thru the suffix continuation class (only if it has any!)
            dictionary.flagLookup.get(append, scratch);
            char appendFlags[] = Dictionary.decodeFlags(scratch);
            if (!hasCrossCheckedFlag((char)prefixFlag, appendFlags, false)) {
              continue;
            }
          }
          
          // if circumfix was previously set by a prefix, we must check this suffix,
          // to ensure it has it, and vice versa
          if (dictionary.circumfix != -1) {
            dictionary.flagLookup.get(append, scratch);
            char appendFlags[] = Dictionary.decodeFlags(scratch);
            boolean suffixCircumfix = Dictionary.hasFlag(appendFlags, (char)dictionary.circumfix);
            if (circumfix != suffixCircumfix) {
              continue;
            }
          }
          stems.add(new CharsRef(strippedWord, 0, length));
        }
      }
    }
    
    // if a circumfix flag is defined in the dictionary, and we are a prefix, we need to check if we have that flag
    if (dictionary.circumfix != -1 && !circumfix && prefix) {
      dictionary.flagLookup.get(append, scratch);
      char appendFlags[] = Dictionary.decodeFlags(scratch);
      circumfix = Dictionary.hasFlag(appendFlags, (char)dictionary.circumfix);
    }

    if (crossProduct) {
      if (recursionDepth == 0) {
        if (prefix) {
          // we took away the first prefix.
          // COMPLEXPREFIXES = true:  combine with a second prefix and another suffix 
          // COMPLEXPREFIXES = false: combine with another suffix
          stems.addAll(stem(strippedWord, length, affix, flag, flag, ++recursionDepth, dictionary.complexPrefixes, true, true, circumfix));
        } else if (!dictionary.complexPrefixes) {
          // we took away a suffix.
          // COMPLEXPREFIXES = true: we don't recurse! only one suffix allowed
          // COMPLEXPREFIXES = false: combine with another suffix
          stems.addAll(stem(strippedWord, length, affix, flag, prefixFlag, ++recursionDepth, false, true, false, circumfix));
        }
      } else if (recursionDepth == 1) {
        if (prefix && dictionary.complexPrefixes) {
          // we took away the second prefix: go look for another suffix
          stems.addAll(stem(strippedWord, length, affix, flag, flag, ++recursionDepth, false, true, true, circumfix));
        } else if (prefix == false && dictionary.complexPrefixes == false) {
          // we took away a prefix, then a suffix: go look for another suffix
          stems.addAll(stem(strippedWord, length, affix, flag, prefixFlag, ++recursionDepth, false, true, false, circumfix));
        }
      }
    }

    return stems;
  }

  /**
   * Checks if the given flag cross checks with the given array of flags
   *
   * @param flag Flag to cross check with the array of flags
   * @param flags Array of flags to cross check against.  Can be {@code null}
   * @return {@code true} if the flag is found in the array or the array is {@code null}, {@code false} otherwise
   */
  private boolean hasCrossCheckedFlag(char flag, char[] flags, boolean matchEmpty) {
    return (flags.length == 0 && matchEmpty) || Arrays.binarySearch(flags, flag) >= 0;
  }
}
