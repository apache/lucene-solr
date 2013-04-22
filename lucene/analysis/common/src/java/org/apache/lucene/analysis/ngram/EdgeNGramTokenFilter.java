package org.apache.lucene.analysis.ngram;

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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;

/**
 * Tokenizes the given token into n-grams of given size(s).
 * <p>
 * This {@link TokenFilter} create n-grams from the beginning edge or ending edge of a input token.
 * </p>
 */
public final class EdgeNGramTokenFilter extends TokenFilter {
  public static final Side DEFAULT_SIDE = Side.FRONT;
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;

  /** Specifies which side of the input the n-gram should be generated from */
  public static enum Side {

    /** Get the n-gram from the front of the input */
    FRONT {
      @Override
      public String getLabel() { return "front"; }
    },

    /** Get the n-gram from the end of the input */
    BACK  {
      @Override
      public String getLabel() { return "back"; }
    };

    public abstract String getLabel();

    // Get the appropriate Side from a string
    public static Side getSide(String sideName) {
      if (FRONT.getLabel().equals(sideName)) {
        return FRONT;
      }
      if (BACK.getLabel().equals(sideName)) {
        return BACK;
      }
      return null;
    }
  }

  private final int minGram;
  private final int maxGram;
  private Side side;
  private char[] curTermBuffer;
  private int curTermLength;
  private int curGramSize;
  private int tokStart;
  private int tokEnd; // only used if the length changed before this filter
  private boolean hasIllegalOffsets; // only if the length changed before this filter
  private int savePosIncr;
  private boolean isFirstToken = true;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  /**
   * Creates EdgeNGramTokenFilter that can generate n-grams in the sizes of the given range
   *
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param side the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenFilter(TokenStream input, Side side, int minGram, int maxGram) {
    super(input);

    if (side == null) {
      throw new IllegalArgumentException("sideLabel must be either front or back");
    }

    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }

    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }

    this.minGram = minGram;
    this.maxGram = maxGram;
    this.side = side;
  }

  /**
   * Creates EdgeNGramTokenFilter that can generate n-grams in the sizes of the given range
   *
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param sideLabel the name of the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenFilter(TokenStream input, String sideLabel, int minGram, int maxGram) {
    this(input, Side.getSide(sideLabel), minGram, maxGram);
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
          curGramSize = minGram;
          tokStart = offsetAtt.startOffset();
          tokEnd = offsetAtt.endOffset();
          // if length by start + end offsets doesn't match the term text then assume
          // this is a synonym and don't adjust the offsets.
          hasIllegalOffsets = (tokStart + curTermLength) != tokEnd;
          savePosIncr = posIncrAtt.getPositionIncrement();
        }
      }
      if (curGramSize <= maxGram) {         // if we have hit the end of our n-gram size range, quit
        if (curGramSize <= curTermLength) { // if the remaining input is too short, we can't generate any n-grams
          // grab gramSize chars from front or back
          int start = side == Side.FRONT ? 0 : curTermLength - curGramSize;
          int end = start + curGramSize;
          clearAttributes();
          if (hasIllegalOffsets) {
            offsetAtt.setOffset(tokStart, tokEnd);
          } else {
            offsetAtt.setOffset(tokStart + start, tokStart + end);
          }
          // first ngram gets increment, others don't
          if (curGramSize == minGram) {
            //  Leave the first token position increment at the cleared-attribute value of 1
            if ( ! isFirstToken) {
              posIncrAtt.setPositionIncrement(savePosIncr);
            }
          } else {
            posIncrAtt.setPositionIncrement(0);
          }
          termAtt.copyBuffer(curTermBuffer, start, curGramSize);
          curGramSize++;
          isFirstToken = false;
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
    isFirstToken = true;
  }
}
