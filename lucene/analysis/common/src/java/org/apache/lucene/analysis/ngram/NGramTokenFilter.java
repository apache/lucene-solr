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
package org.apache.lucene.analysis.ngram;


import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Tokenizes the input into n-grams of the given size(s).
 * As of Lucene 4.4, this token filter:<ul>
 * <li>handles supplementary characters correctly,</li>
 * <li>emits all n-grams for the same token at the same position,</li>
 * <li>does not modify offsets,</li>
 * <li>sorts n-grams by their offset in the original token first, then
 * increasing length (meaning that "abc" will give "a", "ab", "abc", "b", "bc",
 * "c").</li></ul>
 * <p>If you were using this {@link TokenFilter} to perform partial highlighting,
 * this won't work anymore since this filter doesn't update offsets. You should
 * modify your analysis chain to use {@link NGramTokenizer}, and potentially
 * override {@link NGramTokenizer#isTokenChar(int)} to perform pre-tokenization.
 */
public final class NGramTokenFilter extends TokenFilter {
  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;
  public static final boolean DEFAULT_KEEP_SHORT_TERM = false;
  public static final boolean DEFAULT_KEEP_LONG_TERM = false;

  private final int minGram;
  private final int maxGram;
  private final boolean keepShortTerm;
  private final boolean keepLongTerm;

  private char[] curTermBuffer;
  private int curTermLength;
  private int curTermCodePointCount;
  private int curGramSize;
  private int curPos;
  private int curPosIncr;
  private State state;

  private final CharTermAttribute termAtt;
  private final PositionIncrementAttribute posIncrAtt;

  public NGramTokenFilter(TokenStream input, int minGram, int maxGram, boolean keepShortTerm, boolean keepLongTerm) {
    super(input);
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
    this.keepShortTerm = keepShortTerm;
    this.keepLongTerm = keepLongTerm;

    this.termAtt = addAttribute(CharTermAttribute.class);
    this.posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  }
  
  /**
   * Creates NGramTokenFilter with given min and max n-grams.
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public NGramTokenFilter(TokenStream input, int minGram, int maxGram) {
    this(input, minGram, maxGram, DEFAULT_KEEP_SHORT_TERM, DEFAULT_KEEP_LONG_TERM);
  }

  /**
   * Creates NGramTokenFilter with default min and max n-grams.
   * @param input {@link TokenStream} holding the input to be tokenized
   */
  public NGramTokenFilter(TokenStream input) {
    this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    while (true) {
      if (curTermBuffer == null) {
        if (!input.incrementToken()) {
          return false;
        }
        state = captureState();
        
        curTermLength = termAtt.length();
        curTermCodePointCount = Character.codePointCount(termAtt, 0, termAtt.length());
        curPosIncr += posIncrAtt.getPositionIncrement();
        curPos = 0;
        
        if (keepShortTerm && curTermCodePointCount < minGram) {
          // Token is shorter than minGram, but we'd still like to keep it.
          posIncrAtt.setPositionIncrement(curPosIncr);
          curPosIncr = 0;
          return true;
        }
        
        curTermBuffer = termAtt.buffer().clone();
        curGramSize = minGram;
      }

      if (curGramSize > maxGram || (curPos + curGramSize) > curTermCodePointCount) {
        ++curPos;
        curGramSize = minGram;
      }
      if ((curPos + curGramSize) <= curTermCodePointCount) {
        restoreState(state);
        final int start = Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curPos);
        final int end = Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, start, curGramSize);
        termAtt.copyBuffer(curTermBuffer, start, end - start);
        posIncrAtt.setPositionIncrement(curPosIncr);
        curPosIncr = 0;
        curGramSize++;
        return true;
      }
      else if (keepLongTerm && curTermCodePointCount > maxGram) {
        // Token is longer than maxGram, but we'd still like to keep it.
        restoreState(state);
        posIncrAtt.setPositionIncrement(0);
        termAtt.copyBuffer(curTermBuffer, 0, curTermLength);
        curTermBuffer = null;
        return true;
      }
      
      // Done with this input token, get next token on next iteration.
      curTermBuffer = null;  
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    curTermBuffer = null;
    curPosIncr = 0;
  }
}
