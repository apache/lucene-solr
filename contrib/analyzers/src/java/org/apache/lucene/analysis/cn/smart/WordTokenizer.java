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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * A {@link Tokenizer} that breaks sentences into words.
 */
public class WordTokenizer extends Tokenizer {

  private WordSegmenter wordSegmenter;

  private TokenStream in;

  private Iterator tokenIter;

  private List tokenBuffer;

  private Token sentenceToken = new Token();

  /**
   * Construct a new WordTokenizer.
   * 
   * @param in {@link TokenStream} of sentences
   * @param wordSegmenter {@link WordSegmenter} to break sentences into words 
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
   * Process the next input sentence, placing tokens into tokenBuffer
   * 
   * @return true if more tokens were placed into tokenBuffer.
   * @throws IOException
   */
  private boolean processNextSentence() throws IOException {
    sentenceToken = in.next(sentenceToken);
    if (sentenceToken == null)
      return false;
    tokenBuffer = wordSegmenter.segmentSentence(sentenceToken);
    tokenIter = tokenBuffer.iterator();
    return tokenBuffer != null && tokenIter.hasNext();
  }

  public void close() throws IOException {
    in.close();
  }

}
