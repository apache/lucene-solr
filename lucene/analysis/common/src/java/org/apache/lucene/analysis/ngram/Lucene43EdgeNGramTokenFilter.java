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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;

import java.io.IOException;

/**
 * Tokenizes the given token into n-grams of given size(s), using pre-4.4 behavior.
 *
 * @deprecated Use {@link org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter}.
 */
@Deprecated
public final class Lucene43EdgeNGramTokenFilter extends TokenFilter {
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;

  private final CharacterUtils charUtils;
  private final int minGram;
  private final int maxGram;
  private char[] curTermBuffer;
  private int curTermLength;
  private int curCodePointCount;
  private int curGramSize;
  private int tokStart;
  private int tokEnd; // only used if the length changed before this filter
  private int savePosIncr;
  private int savePosLen;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);

  /**
   * Creates EdgeNGramTokenFilter that can generate n-grams in the sizes of the given range
   *
   * @param input {@link org.apache.lucene.analysis.TokenStream} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public Lucene43EdgeNGramTokenFilter(TokenStream input, int minGram, int maxGram) {
    super(input);

    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }

    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }

    this.charUtils = CharacterUtils.getJava4Instance();
    this.minGram = minGram;
    this.maxGram = maxGram;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    while (true) {
      if (curTermBuffer == null) {
        if (!input.incrementToken()) {
          return false;
        } else {
          curTermBuffer = termAtt.buffer().clone();
          curTermLength = termAtt.length();
          curCodePointCount = charUtils.codePointCount(termAtt);
          curGramSize = minGram;
          tokStart = offsetAtt.startOffset();
          tokEnd = offsetAtt.endOffset();
          savePosIncr += posIncrAtt.getPositionIncrement();
          savePosLen = posLenAtt.getPositionLength();
        }
      }
      if (curGramSize <= maxGram) {         // if we have hit the end of our n-gram size range, quit
        if (curGramSize <= curCodePointCount) { // if the remaining input is too short, we can't generate any n-grams
          // grab gramSize chars from front or back
          clearAttributes();
          offsetAtt.setOffset(tokStart, tokEnd);
          // first ngram gets increment, others don't
          if (curGramSize == minGram) {
            posIncrAtt.setPositionIncrement(savePosIncr);
            savePosIncr = 0;
          } else {
            posIncrAtt.setPositionIncrement(0);
          }
          posLenAtt.setPositionLength(savePosLen);
          final int charLength = charUtils.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curGramSize);
          termAtt.copyBuffer(curTermBuffer, 0, charLength);
          curGramSize++;
          return true;
        }
      }
      curTermBuffer = null;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    curTermBuffer = null;
    savePosIncr = 0;
  }
}
