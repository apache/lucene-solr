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

package org.apache.lucene.analysis.cn.smart;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.cn.smart.hhmm.HHMMSegmenter;
import org.apache.lucene.analysis.cn.smart.hhmm.SegToken;
import org.apache.lucene.analysis.cn.smart.hhmm.SegTokenFilter;

/**
 * Segment a sentence of Chinese text into words.
 */
class WordSegmenter {

  private HHMMSegmenter hhmmSegmenter = new HHMMSegmenter();

  private SegTokenFilter tokenFilter = new SegTokenFilter();

  /**
   * Segment a sentence into words with {@link HHMMSegmenter}
   * 
   * @param sentenceToken sentence {@link Token}
   * @return {@link List} of {@link SegToken}
   */
  public List segmentSentence(Token sentenceToken) {
    String sentence = sentenceToken.term();

    List segTokenList = hhmmSegmenter.process(sentence);

    List result = new ArrayList();

    // tokens from sentence, excluding WordType.SENTENCE_BEGIN and WordType.SENTENCE_END
    for (int i = 1; i < segTokenList.size() - 1; i++) {
      result.add(convertSegToken((SegToken) segTokenList.get(i), sentence,
          sentenceToken.startOffset(), "word"));
    }
    return result;

  }

  /**
   * Convert a {@link SegToken} to a Lucene {@link Token}
   * 
   * @param st input {@link SegToken}
   * @param sentence associated Sentence
   * @param sentenceStartOffset offset into sentence
   * @param type token type, default is word
   * @return Lucene {@link Token}
   */
  public Token convertSegToken(SegToken st, String sentence,
      int sentenceStartOffset, String type) {
    Token result;
    switch (st.wordType) {
      case WordType.STRING:
      case WordType.NUMBER:
      case WordType.FULLWIDTH_NUMBER:
      case WordType.FULLWIDTH_STRING:
        st.charArray = sentence.substring(st.startOffset, st.endOffset)
            .toCharArray();
        break;
      default:
        break;
    }

    st = tokenFilter.filter(st);

    result = new Token(st.charArray, 0, st.charArray.length, st.startOffset
        + sentenceStartOffset, st.endOffset + sentenceStartOffset);
    return result;
  }
}
