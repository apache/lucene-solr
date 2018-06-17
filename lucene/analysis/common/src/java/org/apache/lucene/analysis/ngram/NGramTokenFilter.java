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
  /**
   * @deprecated since 7.4 - this value will be required.
   */
  @Deprecated
  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;

  /**
   * @deprecated since 7.4 - this value will be required.
   */
  @Deprecated
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;
  public static final boolean DEFAULT_PRESERVE_ORIGINAL = false;

  private final int minGram;
  private final int maxGram;
  private final boolean preserveOriginal;

  private char[] curTermBuffer;
  private int curTermLength;
  private int curTermCodePointCount;
  private int curGramSize;
  private int curPos;
  private int curPosIncr;
  private State state;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  /**
   * Creates an NGramTokenFilter that, for a given input term, produces all
   * contained n-grams with lengths &gt;= minGram and &lt;= maxGram. Will
   * optionally preserve the original term when its length is outside of the
   * defined range.
   * 
   * Note: Care must be taken when choosing minGram and maxGram; depending
   * on the input token size, this filter potentially produces a huge number
   * of terms.
   * 
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param minGram the minimum length of the generated n-grams
   * @param maxGram the maximum length of the generated n-grams
   * @param preserveOriginal Whether or not to keep the original term when it
   * is shorter than minGram or longer than maxGram
   */
  public NGramTokenFilter(TokenStream input, int minGram, int maxGram, boolean preserveOriginal) {
    super(input);
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
    this.preserveOriginal = preserveOriginal;
  }
  
  /**
   * Creates an NGramTokenFilter that produces n-grams of the indicated size.
   * 
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param gramSize the size of n-grams to generate.
   */
  public NGramTokenFilter(TokenStream input, int gramSize) {
    this(input, gramSize, gramSize, DEFAULT_PRESERVE_ORIGINAL);
  }

  /**
   * Creates an NGramTokenFilter that, for a given input term, produces all
   * contained n-grams with lengths &gt;= minGram and &lt;= maxGram.
   * 
   * <p>
   * Behaves the same as
   * {@link #NGramTokenFilter(TokenStream, int, int, boolean)
   * NGramTokenFilter(input, minGram, maxGram, false)}
   * 
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param minGram the minimum length of the generated n-grams
   * @param maxGram the maximum length of the generated n-grams
   *
   * @deprecated since 7.4. Use
   * {@link #NGramTokenFilter(TokenStream, int, int, boolean)} instead.
   */
  @Deprecated
  public NGramTokenFilter(TokenStream input, int minGram, int maxGram) {
    this(input, minGram, maxGram, DEFAULT_PRESERVE_ORIGINAL);
  }

  /**
   * Creates NGramTokenFilter with default min and max n-grams.
   * 
   * <p>
   * Behaves the same as
   * {@link #NGramTokenFilter(TokenStream, int, int, boolean)
   * NGramTokenFilter(input, 1, 2, false)}
   * 
   * @param input {@link TokenStream} holding the input to be tokenized
   * @deprecated since 7.4. Use
   * {@link #NGramTokenFilter(TokenStream, int, int, boolean)} instead.
   */
  @Deprecated
  public NGramTokenFilter(TokenStream input) {
    this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE, DEFAULT_PRESERVE_ORIGINAL);
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
        
        if (preserveOriginal && curTermCodePointCount < minGram) {
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
      else if (preserveOriginal && curTermCodePointCount > maxGram) {
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

  @Override
  public void end() throws IOException {
    super.end();
    posIncrAtt.setPositionIncrement(curPosIncr);
  }
}
