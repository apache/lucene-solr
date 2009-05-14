/**
 * Copyright 2009 www.imdict.net
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

public class WordTokenizer extends Tokenizer {

  /**
   * 分词主程序，WordTokenizer初始化时加载。
   */
  private WordSegmenter wordSegmenter;

  private TokenStream in;

  private Iterator tokenIter;

  private List tokenBuffer;

  private Token sentenceToken = new Token();

  /**
   * 设计上是SentenceTokenizer的下一处理层。将SentenceTokenizer的句子读出，
   * 利用HHMMSegment主程序将句子分词，然后将分词结果返回。
   * 
   * @param in 句子的Token
   * @param smooth 平滑函数
   * @param dataPath 装载核心字典与二叉字典的目录
   * @see init()
   */
  public WordTokenizer(TokenStream in, WordSegmenter wordSegmenter) {
    this.in = in;
    this.wordSegmenter = wordSegmenter;
  }

  public Token next() throws IOException {
    if (tokenIter != null && tokenIter.hasNext())
      return (Token) tokenIter.next();
    else {
      if (processNextSentence()) {
        return (Token) tokenIter.next();
      } else
        return null;
    }
  }

  /**
   * 当当前的句子分词并索引完毕时，需要读取下一个句子Token， 本函数负责调用上一层的SentenceTokenizer去加载下一个句子， 并将其分词，
   * 将分词结果保存成Token放在tokenBuffer中
   * 
   * @return 读取并处理下一个句子成功与否，如果没有成功，说明文件处理完毕，后面没有Token了
   * @throws IOException
   */
  private boolean processNextSentence() throws IOException {
    sentenceToken = in.next(sentenceToken);
    if (sentenceToken == null)
      return false;
    tokenBuffer = wordSegmenter.segmentSentence(sentenceToken, 1);
    tokenIter = tokenBuffer.iterator();
    return tokenBuffer != null && tokenIter.hasNext();
  }

  public void close() throws IOException {
    in.close();
  }

}
