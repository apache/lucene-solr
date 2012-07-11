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

package org.apache.lucene.analysis.cn.smart;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.hhmm.SegToken;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * A {@link TokenFilter} that breaks sentences into words.
 * @lucene.experimental
 */
public final class WordTokenFilter extends TokenFilter {

  private WordSegmenter wordSegmenter;

  private Iterator<SegToken> tokenIter;

  private List<SegToken> tokenBuffer;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  
  private int tokStart; // only used if the length changed before this filter
  private int tokEnd; // only used if the length changed before this filter
  private boolean hasIllegalOffsets; // only if the length changed before this filter

  /**
   * Construct a new WordTokenizer.
   * 
   * @param in {@link TokenStream} of sentences 
   */
  public WordTokenFilter(TokenStream in) {
    super(in);
    this.wordSegmenter = new WordSegmenter();
  }
  
  @Override
  public boolean incrementToken() throws IOException {   
    if (tokenIter == null || !tokenIter.hasNext()) {
      // there are no remaining tokens from the current sentence... are there more sentences?
      if (input.incrementToken()) {
        tokStart = offsetAtt.startOffset();
        tokEnd = offsetAtt.endOffset();
        // if length by start + end offsets doesn't match the term text then assume
        // this is a synonym and don't adjust the offsets.
        hasIllegalOffsets = (tokStart + termAtt.length()) != tokEnd;
        // a new sentence is available: process it.
        tokenBuffer = wordSegmenter.segmentSentence(termAtt.toString(), offsetAtt.startOffset());
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
    // WordTokenFilter must clear attributes, as it is creating new tokens.
    clearAttributes();
    // There are remaining tokens from the current sentence, return the next one. 
    SegToken nextWord = tokenIter.next();
    termAtt.copyBuffer(nextWord.charArray, 0, nextWord.charArray.length);
    if (hasIllegalOffsets) {
      offsetAtt.setOffset(tokStart, tokEnd);
    } else {
      offsetAtt.setOffset(nextWord.startOffset, nextWord.endOffset);
    }
    typeAtt.setType("word");
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    tokenIter = null;
  }
}
