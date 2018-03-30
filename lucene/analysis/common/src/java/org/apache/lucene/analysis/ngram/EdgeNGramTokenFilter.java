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
 * Tokenizes the given token into n-grams of given size(s).
 * <p>
 * This {@link TokenFilter} create n-grams from the beginning edge of a input token.
 * <p><a name="match_version"></a>As of Lucene 4.4, this filter handles correctly
 * supplementary characters.
 */
public final class EdgeNGramTokenFilter extends TokenFilter {
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;
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
  private int curPosIncr;
  private State state;
  
  private final CharTermAttribute termAtt;
  private final PositionIncrementAttribute posIncrAtt;

  /**
   * Creates EdgeNGramTokenFilter that generates edge n-grams of sizes in the given range.
   *
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   * @param keepShortTerm whether to pass through tokens that are shorter than minGram
   * @param keepLongTerm whether to pass through tokens that are longer than maxGram
   */
  public EdgeNGramTokenFilter(
      TokenStream input, int minGram, int maxGram, boolean keepShortTerm, boolean keepLongTerm) {
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

  public EdgeNGramTokenFilter(TokenStream input, int minGram, int maxGram) {
    this(input, minGram, maxGram, DEFAULT_KEEP_SHORT_TERM, DEFAULT_KEEP_LONG_TERM);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    while (true) {
      if (curTermBuffer == null) {
        
        // Next token.
        if (!input.incrementToken()) {
          return false;
        }

        curTermBuffer = termAtt.buffer().clone();
        curTermLength = termAtt.length();
        curTermCodePointCount = Character.codePointCount(termAtt, 0, curTermLength);

        state = captureState();
        curPosIncr += posIncrAtt.getPositionIncrement();
        curGramSize = minGram;

        if (keepShortTerm && curTermCodePointCount < minGram) {
          // Token is shorter than minGram, but we'd still like to keep it.
          posIncrAtt.setPositionIncrement(curPosIncr);
          curPosIncr = 0;
          termAtt.copyBuffer(curTermBuffer, 0, curTermLength);
          return true;
        }
      }

      if (curGramSize <= curTermCodePointCount) {
        if (curGramSize <= maxGram) { // curGramSize is between minGram and maxGram
          restoreState(state);
          // first ngram gets increment, others don't
          posIncrAtt.setPositionIncrement(curPosIncr);
          curPosIncr = 0;

          final int charLength = Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curGramSize);
          termAtt.copyBuffer(curTermBuffer, 0, charLength);
          curGramSize++;
          return true;
        }
        else if (keepLongTerm) {
          // Token is longer than maxGram, but we'd still like to keep it.
          restoreState(state);
          posIncrAtt.setPositionIncrement(0);
          termAtt.copyBuffer(curTermBuffer, 0, curTermLength);
          curTermBuffer = null;
          return true;
        }
      }
      // Done with this input token, get next token on the next iteration.
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
