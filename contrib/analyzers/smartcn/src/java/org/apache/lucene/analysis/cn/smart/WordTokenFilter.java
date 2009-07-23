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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

/**
 * A {@link TokenFilter} that breaks sentences into words.
 */
public class WordTokenFilter extends TokenFilter {

  private WordSegmenter wordSegmenter;

  private Iterator tokenIter;

  private List tokenBuffer;

  /**
   * Construct a new WordTokenizer.
   * 
   * @param in {@link TokenStream} of sentences 
   */
  public WordTokenFilter(TokenStream in) {
    super(in);
    this.wordSegmenter = new WordSegmenter();
  }

  public Token next(final Token reusableSentenceToken) throws IOException {
    if (tokenIter != null && tokenIter.hasNext())
      return (Token) tokenIter.next();
    else {
      Token nextToken = input.next(reusableSentenceToken);
      if (processNextSentence(nextToken)) {
        return (Token) tokenIter.next();
      } else
        return null;
    }
  }

  /**
   * Process the next input sentence, placing tokens into tokenBuffer
   * 
   * @param reusableSentenceToken input sentence
   * @return true if more tokens were placed into tokenBuffer.
   * @throws IOException
   */
  private boolean processNextSentence(final Token reusableSentenceToken) throws IOException {
    if (reusableSentenceToken == null)
      return false;
    tokenBuffer = wordSegmenter.segmentSentence(reusableSentenceToken);
    tokenIter = tokenBuffer.iterator();
    return tokenBuffer != null && tokenIter.hasNext();
  }
}
