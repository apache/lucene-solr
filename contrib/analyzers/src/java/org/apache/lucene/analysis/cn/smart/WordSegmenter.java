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

public class WordSegmenter {

  private HHMMSegmenter hhmmSegmenter = new HHMMSegmenter();

  private SegTokenFilter tokenFilter = new SegTokenFilter();

  /**
   * 调用HHMMSegment程序将当前的sentence Token分词，返回分词结果，保存在Token List中
   * 
   * @param sentenceToken 句子的Token
   * @param shortPathCount HHMM算法分词所需要的优化前的最短路径个数。一般越大分词结果越精确，但是计算代价也较高。
   * @return 分词结果的Token List
   */
  public List segmentSentence(Token sentenceToken, int shortPathCount) {
    String sentence = sentenceToken.term();

    List segTokenList = hhmmSegmenter.process(sentence);

    List result = new ArrayList();

    // i从1到rawTokens.length-2，也就是说将“始##始”，“末##末”两个RawToken去掉
    for (int i = 1; i < segTokenList.size() - 1; i++) {
      result.add(convertSegToken((SegToken) segTokenList.get(i), sentence,
          sentenceToken.startOffset(), "word"));
    }
    return result;

  }

  /**
   * 
   * 将RawToken类型转换成索引需要的Token类型， 因为索引需要RawToken在原句中的内容， 因此转换时需要指定原句子。
   * 
   * @param rt
   * @param sentence 转换需要的句子内容
   * @param sentenceStartOffset sentence在文章中的初始位置
   * @param type token类型，默认应该是word
   * @return
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
