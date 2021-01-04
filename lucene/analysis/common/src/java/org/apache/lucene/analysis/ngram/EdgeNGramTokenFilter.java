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
 *
 * <p>This {@link TokenFilter} create n-grams from the beginning edge of a input token.
 *
 * <p><a id="match_version"></a>As of Lucene 4.4, this filter handles correctly supplementary
 * characters.
 */
public final class EdgeNGramTokenFilter extends TokenFilter {
  public static final boolean DEFAULT_PRESERVE_ORIGINAL = false;

  private final int minGram;
  private final int maxGram;
  private final boolean preserveOriginal;

  private char[] curTermBuffer;
  private int curTermLength;
  private int curTermCodePointCount;
  private int curGramSize;
  private int curPosIncr;
  private State state;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt =
      addAttribute(PositionIncrementAttribute.class);

  /**
   * Creates an EdgeNGramTokenFilter that, for a given input term, produces all edge n-grams with
   * lengths &gt;= minGram and &lt;= maxGram. Will optionally preserve the original term when its
   * length is outside of the defined range.
   *
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param minGram the minimum length of the generated n-grams
   * @param maxGram the maximum length of the generated n-grams
   * @param preserveOriginal Whether or not to keep the original term when it is outside the min/max
   *     size range.
   */
  public EdgeNGramTokenFilter(
      TokenStream input, int minGram, int maxGram, boolean preserveOriginal) {
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
   * Creates an EdgeNGramTokenFilter that produces edge n-grams of the given size.
   *
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param gramSize the n-gram size to generate.
   */
  public EdgeNGramTokenFilter(TokenStream input, int gramSize) {
    this(input, gramSize, gramSize, DEFAULT_PRESERVE_ORIGINAL);
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
        curTermCodePointCount = Character.codePointCount(termAtt, 0, curTermLength);
        curPosIncr += posIncrAtt.getPositionIncrement();

        if (preserveOriginal && curTermCodePointCount < minGram) {
          // Token is shorter than minGram, but we'd still like to keep it.
          posIncrAtt.setPositionIncrement(curPosIncr);
          curPosIncr = 0;
          return true;
        }

        curTermBuffer = termAtt.buffer().clone();
        curGramSize = minGram;
      }

      if (curGramSize <= curTermCodePointCount) {
        if (curGramSize <= maxGram) { // curGramSize is between minGram and maxGram
          restoreState(state);
          // first ngram gets increment, others don't
          posIncrAtt.setPositionIncrement(curPosIncr);
          curPosIncr = 0;

          final int charLength =
              Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curGramSize);
          termAtt.copyBuffer(curTermBuffer, 0, charLength);
          curGramSize++;
          return true;
        } else if (preserveOriginal) {
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

  @Override
  public void end() throws IOException {
    super.end();
    posIncrAtt.setPositionIncrement(curPosIncr);
  }
}
