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

package org.apache.lucene.analysis.cn.smart.hhmm;

import java.util.List;

import org.apache.lucene.analysis.cn.smart.CharType;
import org.apache.lucene.analysis.cn.smart.Utility;
import org.apache.lucene.analysis.cn.smart.WordType;
import org.apache.lucene.analysis.cn.smart.hhmm.SegToken;//javadoc @link

/**
 * Finds the optimal segmentation of a sentence into Chinese words
 * @lucene.experimental
 */
public class HHMMSegmenter {

  private static WordDictionary wordDict = WordDictionary.getInstance();

  /**
   * Create the {@link SegGraph} for a sentence.
   * 
   * @param sentence input sentence, without start and end markers
   * @return {@link SegGraph} corresponding to the input sentence.
   */
  @SuppressWarnings("fallthrough")
  private SegGraph createSegGraph(String sentence) {
    int i = 0, j;
    int length = sentence.length();
    int foundIndex;
    int[] charTypeArray = getCharTypes(sentence);
    StringBuilder wordBuf = new StringBuilder();
    SegToken token;
    int frequency = 0; // the number of times word appears.
    boolean hasFullWidth;
    int wordType;
    char[] charArray;

    SegGraph segGraph = new SegGraph();
    while (i < length) {
      hasFullWidth = false;
      switch (charTypeArray[i]) {
        case CharType.SPACE_LIKE:
          i++;
          break;
        case CharType.HANZI:
          j = i + 1;
          wordBuf.delete(0, wordBuf.length());
          // It doesn't matter if a single Chinese character (Hanzi) can form a phrase or not, 
          // it will store that single Chinese character (Hanzi) in the SegGraph.  Otherwise, it will 
          // cause word division.
          wordBuf.append(sentence.charAt(i));
          charArray = new char[] { sentence.charAt(i) };
          frequency = wordDict.getFrequency(charArray);
          token = new SegToken(charArray, i, j, WordType.CHINESE_WORD,
              frequency);
          segGraph.addToken(token);

          foundIndex = wordDict.getPrefixMatch(charArray);
          while (j <= length && foundIndex != -1) {
            if (wordDict.isEqual(charArray, foundIndex) && charArray.length > 1) {
              // It is the phrase we are looking for; In other words, we have found a phrase SegToken
              // from i to j.  It is not a monosyllabic word (single word).
              frequency = wordDict.getFrequency(charArray);
              token = new SegToken(charArray, i, j, WordType.CHINESE_WORD,
                  frequency);
              segGraph.addToken(token);
            }

            while (j < length && charTypeArray[j] == CharType.SPACE_LIKE)
              j++;

            if (j < length && charTypeArray[j] == CharType.HANZI) {
              wordBuf.append(sentence.charAt(j));
              charArray = new char[wordBuf.length()];
              wordBuf.getChars(0, charArray.length, charArray, 0);
              // idArray has been found (foundWordIndex!=-1) as a prefix before.  
              // Therefore, idArray after it has been lengthened can only appear after foundWordIndex.  
              // So start searching after foundWordIndex.
              foundIndex = wordDict.getPrefixMatch(charArray, foundIndex);
              j++;
            } else {
              break;
            }
          }
          i++;
          break;
        case CharType.FULLWIDTH_LETTER:
          hasFullWidth = true; /* intentional fallthrough */
        case CharType.LETTER:
          j = i + 1;
          while (j < length
              && (charTypeArray[j] == CharType.LETTER || charTypeArray[j] == CharType.FULLWIDTH_LETTER)) {
            if (charTypeArray[j] == CharType.FULLWIDTH_LETTER)
              hasFullWidth = true;
            j++;
          }
          // Found a Token from i to j. Type is LETTER char string.
          charArray = Utility.STRING_CHAR_ARRAY;
          frequency = wordDict.getFrequency(charArray);
          wordType = hasFullWidth ? WordType.FULLWIDTH_STRING : WordType.STRING;
          token = new SegToken(charArray, i, j, wordType, frequency);
          segGraph.addToken(token);
          i = j;
          break;
        case CharType.FULLWIDTH_DIGIT:
          hasFullWidth = true; /* intentional fallthrough */
        case CharType.DIGIT:
          j = i + 1;
          while (j < length
              && (charTypeArray[j] == CharType.DIGIT || charTypeArray[j] == CharType.FULLWIDTH_DIGIT)) {
            if (charTypeArray[j] == CharType.FULLWIDTH_DIGIT)
              hasFullWidth = true;
            j++;
          }
          // Found a Token from i to j. Type is NUMBER char string.
          charArray = Utility.NUMBER_CHAR_ARRAY;
          frequency = wordDict.getFrequency(charArray);
          wordType = hasFullWidth ? WordType.FULLWIDTH_NUMBER : WordType.NUMBER;
          token = new SegToken(charArray, i, j, wordType, frequency);
          segGraph.addToken(token);
          i = j;
          break;
        case CharType.DELIMITER:
          j = i + 1;
          // No need to search the weight for the punctuation.  Picking the highest frequency will work.
          frequency = Utility.MAX_FREQUENCE;
          charArray = new char[] { sentence.charAt(i) };
          token = new SegToken(charArray, i, j, WordType.DELIMITER, frequency);
          segGraph.addToken(token);
          i = j;
          break;
        default:
          j = i + 1;
          // Treat the unrecognized char symbol as unknown string.
          // For example, any symbol not in GB2312 is treated as one of these.
          charArray = Utility.STRING_CHAR_ARRAY;
          frequency = wordDict.getFrequency(charArray);
          token = new SegToken(charArray, i, j, WordType.STRING, frequency);
          segGraph.addToken(token);
          i = j;
          break;
      }
    }

    // Add two more Tokens: "beginning xx beginning"
    charArray = Utility.START_CHAR_ARRAY;
    frequency = wordDict.getFrequency(charArray);
    token = new SegToken(charArray, -1, 0, WordType.SENTENCE_BEGIN, frequency);
    segGraph.addToken(token);

    // "end xx end"
    charArray = Utility.END_CHAR_ARRAY;
    frequency = wordDict.getFrequency(charArray);
    token = new SegToken(charArray, length, length + 1, WordType.SENTENCE_END,
        frequency);
    segGraph.addToken(token);

    return segGraph;
  }

  /**
   * Get the character types for every character in a sentence.
   * 
   * @see Utility#getCharType(char)
   * @param sentence input sentence
   * @return array of character types corresponding to character positions in the sentence
   */
  private static int[] getCharTypes(String sentence) {
    int length = sentence.length();
    int[] charTypeArray = new int[length];
    // the type of each character by position
    for (int i = 0; i < length; i++) {
      charTypeArray[i] = Utility.getCharType(sentence.charAt(i));
    }

    return charTypeArray;
  }

  /**
   * Return a list of {@link SegToken} representing the best segmentation of a sentence
   * @param sentence input sentence
   * @return best segmentation as a {@link List}
   */
  public List<SegToken> process(String sentence) {
    SegGraph segGraph = createSegGraph(sentence);
    BiSegGraph biSegGraph = new BiSegGraph(segGraph);
    List<SegToken> shortPath = biSegGraph.getShortPath();
    return shortPath;
  }
}
