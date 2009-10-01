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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.hhmm.SegToken;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * A {@link TokenFilter} that breaks sentences into words.
 * <p><font color="#FF0000">
 * WARNING: The status of the analyzers/smartcn <b>analysis.cn.smart</b> package is experimental. 
 * The APIs and file formats introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 * </p>
 */
public final class WordTokenFilter extends TokenFilter {

  private WordSegmenter wordSegmenter;

  private Iterator tokenIter;

  private List tokenBuffer;
  
  private TermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private TypeAttribute typeAtt;

  /**
   * Construct a new WordTokenizer.
   * 
   * @param in {@link TokenStream} of sentences 
   */
  public WordTokenFilter(TokenStream in) {
    super(in);
    this.wordSegmenter = new WordSegmenter();
    termAtt = addAttribute(TermAttribute.class);
    offsetAtt = addAttribute(OffsetAttribute.class);
    typeAtt = addAttribute(TypeAttribute.class);
  }
  
  public boolean incrementToken() throws IOException {   
    if (tokenIter == null || !tokenIter.hasNext()) {
      // there are no remaining tokens from the current sentence... are there more sentences?
      if (input.incrementToken()) {
        // a new sentence is available: process it.
        tokenBuffer = wordSegmenter.segmentSentence(termAtt.term(), offsetAtt.startOffset());
        tokenIter = tokenBuffer.iterator();
        /* 
         * it should not be possible to have a sentence with 0 words, check just in case.
         * returning EOS isn't the best either, but its the behavior of the original code.
         */
        if (!tokenIter.hasNext())
          return false;
      } else {
        return false; // no more sentences, end of stream!
      }
    } 
    
    // There are remaining tokens from the current sentence, return the next one. 
    SegToken nextWord = (SegToken) tokenIter.next();
    termAtt.setTermBuffer(nextWord.charArray, 0, nextWord.charArray.length);
    offsetAtt.setOffset(nextWord.startOffset, nextWord.endOffset);
    typeAtt.setType("word");
    return true;
  }

  public void reset() throws IOException {
    super.reset();
    tokenIter = null;
  }
}
