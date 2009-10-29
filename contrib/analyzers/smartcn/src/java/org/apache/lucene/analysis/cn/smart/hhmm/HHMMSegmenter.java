/**
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
 * <p><font color="#FF0000">
 * WARNING: The status of the analyzers/smartcn <b>analysis.cn.smart</b> package is experimental. 
 * The APIs and file formats introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 * </p>
 */
public class HHMMSegmenter {

  private static WordDictionary wordDict = WordDictionary.getInstance();

  /**
   * Create the {@link SegGraph} for a sentence.
   * 
   * @param sentence input sentence, without start and end markers
   * @return {@link SegGraph} corresponding to the input sentence.
   */
  private SegGraph createSegGraph(String sentence) {
    int i = 0, j;
    int length = sentence.length();
    int foundIndex;
    int[] charTypeArray = getCharTypes(sentence);
    StringBuffer wordBuf = new StringBuffer();
    SegToken token;
    int frequency = 0; // word的出现次数
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
          // 不管单个汉字能不能构成词，都将单个汉字存到segGraph中去，否则会造成分此图断字
          wordBuf.append(sentence.charAt(i));
          charArray = new char[] { sentence.charAt(i) };
          frequency = wordDict.getFrequency(charArray);
          token = new SegToken(charArray, i, j, WordType.CHINESE_WORD,
              frequency);
          segGraph.addToken(token);

          foundIndex = wordDict.getPrefixMatch(charArray);
          while (j <= length && foundIndex != -1) {
            if (wordDict.isEqual(charArray, foundIndex) && charArray.length > 1) {
              // 就是我们要找的词， 也就是说找到了从i到j的一个成词SegToken，并且不是单字词
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
              // idArray作为前缀已经找到过(foundWordIndex!=-1),
              // 因此加长过后的idArray只可能出现在foundWordIndex以后,
              // 故从foundWordIndex之后开始查找
              foundIndex = wordDict.getPrefixMatch(charArray, foundIndex);
              j++;
            } else {
              break;
            }
          }
          i++;
          break;
        case CharType.FULLWIDTH_LETTER:
          hasFullWidth = true;
        case CharType.LETTER:
          j = i + 1;
          while (j < length
              && (charTypeArray[j] == CharType.LETTER || charTypeArray[j] == CharType.FULLWIDTH_LETTER)) {
            if (charTypeArray[j] == CharType.FULLWIDTH_LETTER)
              hasFullWidth = true;
            j++;
          }
          // 找到了从i到j的一个Token，类型为LETTER的字符串
          charArray = Utility.STRING_CHAR_ARRAY;
          frequency = wordDict.getFrequency(charArray);
          wordType = hasFullWidth ? WordType.FULLWIDTH_STRING : WordType.STRING;
          token = new SegToken(charArray, i, j, wordType, frequency);
          segGraph.addToken(token);
          i = j;
          break;
        case CharType.FULLWIDTH_DIGIT:
          hasFullWidth = true;
        case CharType.DIGIT:
          j = i + 1;
          while (j < length
              && (charTypeArray[j] == CharType.DIGIT || charTypeArray[j] == CharType.FULLWIDTH_DIGIT)) {
            if (charTypeArray[j] == CharType.FULLWIDTH_DIGIT)
              hasFullWidth = true;
            j++;
          }
          // 找到了从i到j的一个Token，类型为NUMBER的字符串
          charArray = Utility.NUMBER_CHAR_ARRAY;
          frequency = wordDict.getFrequency(charArray);
          wordType = hasFullWidth ? WordType.FULLWIDTH_NUMBER : WordType.NUMBER;
          token = new SegToken(charArray, i, j, wordType, frequency);
          segGraph.addToken(token);
          i = j;
          break;
        case CharType.DELIMITER:
          j = i + 1;
          // 标点符号的weight不用查了，选个最大的频率即可
          frequency = Utility.MAX_FREQUENCE;
          charArray = new char[] { sentence.charAt(i) };
          token = new SegToken(charArray, i, j, WordType.DELIMITER, frequency);
          segGraph.addToken(token);
          i = j;
          break;
        default:
          j = i + 1;
          // 把不认识的字符当作未知串看待，例如GB2312编码之外的字符，每个字符当作一个
          charArray = Utility.STRING_CHAR_ARRAY;
          frequency = wordDict.getFrequency(charArray);
          token = new SegToken(charArray, i, j, WordType.STRING, frequency);
          segGraph.addToken(token);
          i = j;
          break;
      }
    }

    // 为segGraph增加两个新Token： "始##始","末##末"
    charArray = Utility.START_CHAR_ARRAY;
    frequency = wordDict.getFrequency(charArray);
    token = new SegToken(charArray, -1, 0, WordType.SENTENCE_BEGIN, frequency);
    segGraph.addToken(token);

    // "末##末"
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
  public List process(String sentence) {
    SegGraph segGraph = createSegGraph(sentence);
    BiSegGraph biSegGraph = new BiSegGraph(segGraph);
    List shortPath = biSegGraph.getShortPath();
    return shortPath;
  }
}
