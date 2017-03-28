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
package org.apache.lucene.search.spell;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.SuggestMode;

/**
 * <p>
 * A spell checker whose sole function is to offer suggestions by combining
 * multiple terms into one word and/or breaking terms into multiple words.
 * </p>
 */
public class WordBreakSpellChecker {
  private int minSuggestionFrequency = 1;
  private int minBreakWordLength = 1;
  private int maxCombineWordLength = 20;
  private int maxChanges = 1;
  private int maxEvaluations = 1000;
  private boolean isWordRequiredOnBothSidesOfBreak = true;
  
  /** Term that can be used to prohibit adjacent terms from being combined */
  public static final Term SEPARATOR_TERM = new Term("", "");
  
  /** 
   * Creates a new spellchecker with default configuration values
   * @see #setMaxChanges(int)
   * @see #setMaxCombineWordLength(int)
   * @see #setMaxEvaluations(int)
   * @see #setMinBreakWordLength(int)
   * @see #setMinSuggestionFrequency(int)
   */
  public WordBreakSpellChecker() {}

  /**
   * <p>
   * Determines the order to list word break suggestions
   * </p>
   */
  public enum BreakSuggestionSortMethod {
    /**
     * <p>
     * Sort by Number of word breaks, then by the Sum of all the component
     * term's frequencies
     * </p>
     */
    NUM_CHANGES_THEN_SUMMED_FREQUENCY,
    /**
     * <p>
     * Sort by Number of word breaks, then by the Maximum of all the component
     * term's frequencies
     * </p>
     */
    NUM_CHANGES_THEN_MAX_FREQUENCY
  }
  
  /**
   * <p>
   * Generate suggestions by breaking the passed-in term into multiple words.
   * The scores returned are equal to the number of word breaks needed so a
   * lower score is generally preferred over a higher score.
   * </p>
   * 
   * @param suggestMode
   *          - default = {@link SuggestMode#SUGGEST_WHEN_NOT_IN_INDEX}
   * @param sortMethod
   *          - default =
   *          {@link BreakSuggestionSortMethod#NUM_CHANGES_THEN_MAX_FREQUENCY}
   * @return one or more arrays of words formed by breaking up the original term
   * @throws IOException If there is a low-level I/O error.
   */
  public SuggestWord[][] suggestWordBreaks(Term term, int maxSuggestions,
      IndexReader ir, SuggestMode suggestMode,
      BreakSuggestionSortMethod sortMethod) throws IOException {
    if (maxSuggestions < 1) {
      return new SuggestWord[0][0];
    }
    if (suggestMode == null) {
      suggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
    }
    if (sortMethod == null) {
      sortMethod = BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY;
    }
    
    int queueInitialCapacity = maxSuggestions > 10 ? 10 : maxSuggestions;
    
    Comparator<SuggestWordArrayWrapper> queueComparator;
    if (sortMethod == BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY) {
      if (isWordRequiredOnBothSidesOfBreak)
        queueComparator = new LengthThenMaxFreqComparator();
      else
        queueComparator = new MostRealWordUseThenLengthAndMaxFreqComparator();
    } else {
      if (isWordRequiredOnBothSidesOfBreak)
        queueComparator = new LengthThenSumFreqComparator();
      else
        queueComparator = new MostRealWordUseThenLengthAndSumFreqComparator();
    }
    Queue<SuggestWordArrayWrapper> suggestions = new PriorityQueue<>(
        queueInitialCapacity, queueComparator);
    
    int origFreq = ir.docFreq(term);
    if (origFreq > 0 && suggestMode == SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX) {
      return new SuggestWord[0][];
    }
    
    int useMinSuggestionFrequency = minSuggestionFrequency;
    if (suggestMode == SuggestMode.SUGGEST_MORE_POPULAR) {
      useMinSuggestionFrequency = (origFreq == 0 ? 1 : origFreq);
    }
    
    generateBreakUpSuggestions(term, ir, 1, maxSuggestions,
        useMinSuggestionFrequency, new SuggestWord[0], new SuggestWord[0], suggestions, 0,
        sortMethod);
    
    SuggestWord[][] suggestionArray = new SuggestWord[suggestions.size()][];
    for (int i = suggestions.size() - 1; i >= 0; i--) {
      suggestionArray[i] = suggestions.remove().suggestWords;
    }
    
    return suggestionArray;
  }
  
  /**
   * <p>
   * Generate suggestions by combining one or more of the passed-in terms into
   * single words. The returned {@link CombineSuggestion} contains both a
   * {@link SuggestWord} and also an array detailing which passed-in terms were
   * involved in creating this combination. The scores returned are equal to the
   * number of word combinations needed, also one less than the length of the
   * array {@link CombineSuggestion#originalTermIndexes}. Generally, a
   * suggestion with a lower score is preferred over a higher score.
   * </p>
   * <p>
   * To prevent two adjacent terms from being combined (for instance, if one is
   * mandatory and the other is prohibited), separate the two terms with
   * {@link WordBreakSpellChecker#SEPARATOR_TERM}
   * </p>
   * <p>
   * When suggestMode equals {@link SuggestMode#SUGGEST_WHEN_NOT_IN_INDEX}, each
   * suggestion will include at least one term not in the index.
   * </p>
   * <p>
   * When suggestMode equals {@link SuggestMode#SUGGEST_MORE_POPULAR}, each
   * suggestion will have the same, or better frequency than the most-popular
   * included term.
   * </p>
   * 
   * @return an array of words generated by combining original terms
   * @throws IOException If there is a low-level I/O error.
   */
  public CombineSuggestion[] suggestWordCombinations(Term[] terms,
      int maxSuggestions, IndexReader ir, SuggestMode suggestMode)
      throws IOException {
    if (maxSuggestions < 1) {
      return new CombineSuggestion[0];
    }
    
    int[] origFreqs = null;
    if (suggestMode != SuggestMode.SUGGEST_ALWAYS) {
      origFreqs = new int[terms.length];
      for (int i = 0; i < terms.length; i++) {
        origFreqs[i] = ir.docFreq(terms[i]);
      }
    }
    
    int queueInitialCapacity = maxSuggestions > 10 ? 10 : maxSuggestions;
    Comparator<CombineSuggestionWrapper> queueComparator = new CombinationsThenFreqComparator();
    Queue<CombineSuggestionWrapper> suggestions = new PriorityQueue<>(
        queueInitialCapacity, queueComparator);
    
    int thisTimeEvaluations = 0;
    for (int i = 0; i < terms.length - 1; i++) {
      if (terms[i].equals(SEPARATOR_TERM)) {
        continue;
      }      
      String leftTermText = terms[i].text();
      int leftTermLength = leftTermText.codePointCount(0, leftTermText.length());
      if (leftTermLength > maxCombineWordLength) {
       continue;
      } 
      int maxFreq = 0;
      int minFreq = Integer.MAX_VALUE;
      if (origFreqs != null) {
        maxFreq = origFreqs[i];
        minFreq = origFreqs[i];
      } 
      String combinedTermText = leftTermText;
      int combinedLength = leftTermLength;
      for (int j = i + 1; j < terms.length && j - i <= maxChanges; j++) {
        if (terms[j].equals(SEPARATOR_TERM)) {
          break;
        }
        String rightTermText = terms[j].text();
        int rightTermLength = rightTermText.codePointCount(0, rightTermText.length());
        combinedTermText += rightTermText;
        combinedLength +=rightTermLength;
        if (combinedLength > maxCombineWordLength) {
          break;
        }
        
        if (origFreqs != null) {
          maxFreq = Math.max(maxFreq, origFreqs[j]);
          minFreq = Math.min(minFreq, origFreqs[j]);
        }
                
        Term combinedTerm = new Term(terms[0].field(), combinedTermText);
        int combinedTermFreq = ir.docFreq(combinedTerm);
        
        if (suggestMode != SuggestMode.SUGGEST_MORE_POPULAR
            || combinedTermFreq >= maxFreq) {
          if (suggestMode != SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX
              || minFreq == 0) {
            if (combinedTermFreq >= minSuggestionFrequency) {
              int[] origIndexes = new int[j - i + 1];
              origIndexes[0] = i;
              for (int k = 1; k < origIndexes.length; k++) {
                origIndexes[k] = i + k;
              }
              SuggestWord word = new SuggestWord();
              word.freq = combinedTermFreq;
              word.score = origIndexes.length - 1;
              word.string = combinedTerm.text();
              CombineSuggestionWrapper suggestion = new CombineSuggestionWrapper(
                  new CombineSuggestion(word, origIndexes),
                  (origIndexes.length - 1));
              suggestions.offer(suggestion);
              if (suggestions.size() > maxSuggestions) {
                suggestions.poll();
              }
            }
          }
        }
        thisTimeEvaluations++;
        if (thisTimeEvaluations == maxEvaluations) {
          break;
        }
      }
    }
    CombineSuggestion[] combineSuggestions = new CombineSuggestion[suggestions
        .size()];
    for (int i = suggestions.size() - 1; i >= 0; i--) {
      combineSuggestions[i] = suggestions.remove().combineSuggestion;
    }
    return combineSuggestions;
  }
  
  private int generateBreakUpSuggestions(Term term, IndexReader ir,
      int numberBreaks, int maxSuggestions, int useMinSuggestionFrequency,
      SuggestWord[] prefix, SuggestWord[] suffix, Queue<SuggestWordArrayWrapper> suggestions,
      int totalEvaluations, BreakSuggestionSortMethod sortMethod)
      throws IOException {
    String termText = term.text();
    int termLength = termText.codePointCount(0, termText.length());
    int useMinBreakWordLength = minBreakWordLength;
    if (useMinBreakWordLength < 1) {
      useMinBreakWordLength = 1;
    }
    if (termLength < (useMinBreakWordLength * 2)) {
      return 0;
    }    
    
    int thisTimeEvaluations = 0;
    for (int i = useMinBreakWordLength; i <= (termLength - useMinBreakWordLength); i++) {
      int end = termText.offsetByCodePoints(0, i);
      String leftText = termText.substring(0, end);
      String rightText = termText.substring(end);
      SuggestWord leftWord = generateSuggestWord(ir, term.field(), leftText);
      
      if (leftWord.freq >= useMinSuggestionFrequency || !isWordRequiredOnBothSidesOfBreak) {
        SuggestWord rightWord = generateSuggestWord(ir, term.field(), rightText);
        if (rightWord.freq >= useMinSuggestionFrequency || 
            (!isWordRequiredOnBothSidesOfBreak && leftWord.freq + rightWord.freq >= useMinSuggestionFrequency)) {
            //This second if check is to make sure that there aren't consecutive non-words.  It should never fail
            //while isWordRequiredOnBothSidesOfBreak is set to true.
            if ((leftWord.freq > 0 || prefix.length == 0 || prefix[prefix.length - 1].freq > 0)
                && (rightWord.freq > 0 || suffix.length == 0 || suffix[0].freq > 0)) {
          
          SuggestWordArrayWrapper suggestion = new SuggestWordArrayWrapper(
                  newSuggestion(prefix, leftWord, rightWord, suffix));
              //Duplicates are possible due to how we recurse if a "word" is not required on both sides, so if either both sides 
              //have to be a word, or there are no duplicates, then add it.  We check first to see if a "word" is required on both
              //sides so that we don't have to check the contains method, for efficiency.
              if (isWordRequiredOnBothSidesOfBreak || !suggestions.contains(suggestion)) {
          suggestions.offer(suggestion);
          if (suggestions.size() > maxSuggestions) {
            suggestions.poll();
          }
        }        
            }
        }        
        int newNumberBreaks = numberBreaks + 1;
        if (newNumberBreaks <= maxChanges) {
          SuggestWord[] newPrefix = newPrefix(prefix, leftWord);
          SuggestWord[] newSuffix = newSuffix(rightWord, suffix);
          
          int consecutivePrefixesWithNoFrequency = 0;
          for (SuggestWord word : newPrefix) {
            if (word.freq == 0)
              consecutivePrefixesWithNoFrequency++;
            else 
              consecutivePrefixesWithNoFrequency = 0;
            
            if (consecutivePrefixesWithNoFrequency == 2)
              break;
          }

          int consecutiveSuffixesWithNoFrequency = 0;
          for (SuggestWord word : newSuffix) {
            if (word.freq == 0)
              consecutiveSuffixesWithNoFrequency++;
            else 
              consecutiveSuffixesWithNoFrequency = 0;
            
            if (consecutiveSuffixesWithNoFrequency == 2)
              break;
          }

          int evaluationsRight = 0;
          int evaluationsLeft = 0;
          
          //If there are multiple non-words in a row, it is not a valid suggestion, so go no further.
          if (consecutivePrefixesWithNoFrequency < 2 && consecutiveSuffixesWithNoFrequency < 2) {
            evaluationsRight = generateBreakUpSuggestions(new Term(term.field(),
              rightWord.string), ir, newNumberBreaks, maxSuggestions,
                useMinSuggestionFrequency, newPrefix, suffix,
                suggestions, totalEvaluations, sortMethod);
            //If every break has to be a word, then you don't need to re-process both sides, but working from left to right
            //is sufficient, so this step can be skipped for efficiency.
            //In fact, if you leave this on, then you may get false positives, as we have not verified that rightWord.freq > 0.
            if (!isWordRequiredOnBothSidesOfBreak) {
              evaluationsLeft = generateBreakUpSuggestions(new Term(term.field(),
                  leftWord.string), ir, newNumberBreaks, maxSuggestions,
                  useMinSuggestionFrequency, prefix, newSuffix,
              suggestions, totalEvaluations, sortMethod);
            }
          }

          totalEvaluations += evaluationsRight + evaluationsLeft;

        }
      }
      
      thisTimeEvaluations++;
      totalEvaluations++;
      if (totalEvaluations >= maxEvaluations) {
        break;
      }
    }
    return thisTimeEvaluations;
  }
  
  private SuggestWord[] newPrefix(SuggestWord[] oldPrefix, SuggestWord append) {
    SuggestWord[] newPrefix = new SuggestWord[oldPrefix.length + 1];
    System.arraycopy(oldPrefix, 0, newPrefix, 0, oldPrefix.length);
    newPrefix[newPrefix.length - 1] = append;
    return newPrefix;
  }
  
  private SuggestWord[] newSuffix(SuggestWord append, SuggestWord[] oldSuffix) {
    SuggestWord[] newSuffix = new SuggestWord[oldSuffix.length + 1];
    System.arraycopy(oldSuffix, 0, newSuffix, 1, oldSuffix.length);
    newSuffix[0] = append;
    return newSuffix;
  }

  private SuggestWord[] newSuggestion(SuggestWord[] prefix,
      SuggestWord append1, SuggestWord append2, SuggestWord[] suffix) {
    SuggestWord[] newSuggestion = new SuggestWord[prefix.length + suffix.length + 2];
    int score = prefix.length + suffix.length + 1;
    for (int i = 0; i < prefix.length; i++) {
      SuggestWord word = new SuggestWord();
      word.string = prefix[i].string;
      word.freq = prefix[i].freq;
      word.score = score;
      newSuggestion[i] = word;
    }
    append1.score = score;
    append2.score = score;
    newSuggestion[prefix.length] = append1;
    newSuggestion[prefix.length + 1] = append2;
    for (int i = 0; i < suffix.length; i++) {
      SuggestWord word = new SuggestWord();
      word.string = suffix[i].string;
      word.freq = suffix[i].freq;
      word.score = score;
      newSuggestion[i + prefix.length + 2] = word;
    }
    return newSuggestion;
  }
  
  private SuggestWord generateSuggestWord(IndexReader ir, String fieldname, String text) throws IOException {
    Term term = new Term(fieldname, text);
    int freq = ir.docFreq(term);
    SuggestWord word = new SuggestWord();
    word.freq = freq;
    word.score = 1;
    word.string = text;
    return word;
  }
  
  /**
   * Returns the minimum frequency a term must have
   * to be part of a suggestion.
   * @see #setMinSuggestionFrequency(int)
   */
  public int getMinSuggestionFrequency() {
    return minSuggestionFrequency;
  }
  
  /**
   * Returns the maximum length of a combined suggestion
   * @see #setMaxCombineWordLength(int)
   */
  public int getMaxCombineWordLength() {
    return maxCombineWordLength;
  }
  
  /**
   * Returns the minimum size of a broken word
   * @see #setMinBreakWordLength(int)
   */
  public int getMinBreakWordLength() {
    return minBreakWordLength;
  }
  
  /**
   * Returns the maximum number of changes to perform on the input
   * @see #setMaxChanges(int)
   */
  public int getMaxChanges() {
    return maxChanges;
  }
  
  /**
   * Returns the maximum number of word combinations to evaluate.
   * @see #setMaxEvaluations(int)
   */
  public int getMaxEvaluations() {
    return maxEvaluations;
  }
  
  /**
   * Returns whether or not a word is required on both sides of the suggested break.
   * @see #setIsWordRequiredOnBothSidesOfBreak(boolean)
   */
  public boolean getIsWordRequiredOnBothSidesOfBreak() {
    return isWordRequiredOnBothSidesOfBreak;
  }
  
  /**
   * <p>
   * The minimum frequency a term must have to be included as part of a
   * suggestion. Default=1 Not applicable when used with
   * {@link SuggestMode#SUGGEST_MORE_POPULAR}
   * </p>
   * 
   * @see #getMinSuggestionFrequency()
   */
  public void setMinSuggestionFrequency(int minSuggestionFrequency) {
    this.minSuggestionFrequency = minSuggestionFrequency;
  }
  
  /**
   * <p>
   * The maximum length of a suggestion made by combining 1 or more original
   * terms. Default=20
   * </p>
   * 
   * @see #getMaxCombineWordLength()
   */
  public void setMaxCombineWordLength(int maxCombineWordLength) {
    this.maxCombineWordLength = maxCombineWordLength;
  }
  
  /**
   * <p>
   * The minimum length to break words down to. Default=1
   * </p>
   * 
   * @see #getMinBreakWordLength()
   */
  public void setMinBreakWordLength(int minBreakWordLength) {
    this.minBreakWordLength = minBreakWordLength;
  }
  
  /**
   * <p>
   * The maximum numbers of changes (word breaks or combinations) to make on the
   * original term(s). Default=1
   * </p>
   * 
   * @see #getMaxChanges()
   */
  public void setMaxChanges(int maxChanges) {
    this.maxChanges = maxChanges;
  }
  
  /**
   * <p>
   * The maximum number of word combinations to evaluate. Default=1000. A higher
   * value might improve result quality. A lower value might improve
   * performance.
   * </p>
   * 
   * @see #getMaxEvaluations()
   */
  public void setMaxEvaluations(int maxEvaluations) {
    this.maxEvaluations = maxEvaluations;
  }
  
  /**
   * <p>
   * Whether or not both sides of the suggested break have to be "words."  Defaults to true.
   * </p>
   * 
   * @see #getIsWordRequiredOnBothSidesOfBreak()
   */
  public void setIsWordRequiredOnBothSidesOfBreak(boolean isWordRequiredOnBothSidesOfBreak) {
    this.isWordRequiredOnBothSidesOfBreak = isWordRequiredOnBothSidesOfBreak;
  }

  private class MostRealWordUseThenLengthAndMaxFreqComparator implements
    Comparator<SuggestWordArrayWrapper> {
    @Override
    public int compare(SuggestWordArrayWrapper o1, SuggestWordArrayWrapper o2) {
      if (o1.realWordCharacterCount != o2.realWordCharacterCount)
        return o1.realWordCharacterCount - o2.realWordCharacterCount;
      
      if (o1.suggestWords.length != o2.suggestWords.length) {
        return o2.suggestWords.length - o1.suggestWords.length;
      }
      if (o1.freqMax != o2.freqMax) {
        return o1.freqMax - o2.freqMax;
      }
      return 0;
    }
  }

  private class MostRealWordUseThenLengthAndSumFreqComparator implements
  Comparator<SuggestWordArrayWrapper> {
  @Override
  public int compare(SuggestWordArrayWrapper o1, SuggestWordArrayWrapper o2) {
    if (o1.realWordCharacterCount != o2.realWordCharacterCount)
      return o1.realWordCharacterCount - o2.realWordCharacterCount;
    
    if (o1.suggestWords.length != o2.suggestWords.length) {
      return o2.suggestWords.length - o1.suggestWords.length;
    }
    if (o1.freqSum != o2.freqSum) {
      return o1.freqSum - o2.freqSum;
    }
    return 0;
  }
}

  private class LengthThenMaxFreqComparator implements
      Comparator<SuggestWordArrayWrapper> {
    @Override
    public int compare(SuggestWordArrayWrapper o1, SuggestWordArrayWrapper o2) {
      if (o1.suggestWords.length != o2.suggestWords.length) {
        return o2.suggestWords.length - o1.suggestWords.length;
      }
      if (o1.freqMax != o2.freqMax) {
        return o1.freqMax - o2.freqMax;
      }
      return 0;
    }
  }
  
  private class LengthThenSumFreqComparator implements
      Comparator<SuggestWordArrayWrapper> {
    @Override
    public int compare(SuggestWordArrayWrapper o1, SuggestWordArrayWrapper o2) {
      if (o1.suggestWords.length != o2.suggestWords.length) {
        return o2.suggestWords.length - o1.suggestWords.length;
      }
      if (o1.freqSum != o2.freqSum) {
        return o1.freqSum - o2.freqSum;
      }
      return 0;
    }
  }
  
  private class CombinationsThenFreqComparator implements
      Comparator<CombineSuggestionWrapper> {
    @Override
    public int compare(CombineSuggestionWrapper o1, CombineSuggestionWrapper o2) {
      if (o1.numCombinations != o2.numCombinations) {
        return o2.numCombinations - o1.numCombinations;
      }
      if (o1.combineSuggestion.suggestion.freq != o2.combineSuggestion.suggestion.freq) {
        return o1.combineSuggestion.suggestion.freq
            - o2.combineSuggestion.suggestion.freq;
      }
      return 0;
    }
  }
  
  private class SuggestWordArrayWrapper {
    final SuggestWord[] suggestWords;
    final int freqMax;
    final int freqSum;
    final int realWordCharacterCount;
    
    SuggestWordArrayWrapper(SuggestWord[] suggestWords) {
      this.suggestWords = suggestWords;
      int aFreqSum = 0;
      int aFreqMax = 0;
      int aRealWordCharacterCount = 0;
      for (SuggestWord sw : suggestWords) {
        aFreqSum += sw.freq;
        aFreqMax = Math.max(aFreqMax, sw.freq);
        if (sw.freq > 0)
          aRealWordCharacterCount += sw.string.length();
      }
      this.freqSum = aFreqSum;
      this.freqMax = aFreqMax;
      this.realWordCharacterCount = aRealWordCharacterCount;
    }
    
    public boolean equals(Object obj) {
      if (obj == null || !obj.getClass().equals(this.getClass()))
        return false;
      
      SuggestWordArrayWrapper other = (SuggestWordArrayWrapper)obj;
      if (this.suggestWords == null || other.suggestWords == null || this.suggestWords.length != other.suggestWords.length)
        return false;
      
      for (int i=0; i<suggestWords.length; i++) {
        if (!suggestWords[i].string.equals(other.suggestWords[i].string))
          return false;
      }
        
      return true;
    }
  }
  
  private class CombineSuggestionWrapper {
    final CombineSuggestion combineSuggestion;
    final int numCombinations;
    
    CombineSuggestionWrapper(CombineSuggestion combineSuggestion,
        int numCombinations) {
      this.combineSuggestion = combineSuggestion;
      this.numCombinations = numCombinations;
    }
  }
}
