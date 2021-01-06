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

package org.apache.lucene.analysis.shingle;

import java.io.IOException;
import org.apache.lucene.analysis.GraphTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * A FixedShingleFilter constructs shingles (token n-grams) from a token stream. In other words, it
 * creates combinations of tokens as a single token.
 *
 * <p>Unlike the {@link ShingleFilter}, FixedShingleFilter only emits shingles of a fixed size, and
 * never emits unigrams, even at the end of a TokenStream. In addition, if the filter encounters
 * stacked tokens (eg synonyms), then it will output stacked shingles
 *
 * <p>For example, the sentence "please divide this sentence into shingles" might be tokenized into
 * shingles "please divide", "divide this", "this sentence", "sentence into", and "into shingles".
 *
 * <p>This filter handles position increments &gt; 1 by inserting filler tokens (tokens with
 * termtext "_").
 *
 * @lucene.experimental
 */
public final class FixedShingleFilter extends GraphTokenFilter {

  private static final int MAX_SHINGLE_SIZE = 4;

  private final int shingleSize;
  private final String tokenSeparator;
  private final String fillerToken;

  private final PositionIncrementAttribute incAtt = addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  private final CharTermAttribute buffer = new CharTermAttributeImpl();

  /**
   * Creates a FixedShingleFilter over an input token stream
   *
   * @param input the input stream
   * @param shingleSize the shingle size
   */
  public FixedShingleFilter(TokenStream input, int shingleSize) {
    this(input, shingleSize, " ", "_");
  }

  /**
   * Creates a FixedShingleFilter over an input token stream
   *
   * @param input the input tokenstream
   * @param shingleSize the shingle size
   * @param tokenSeparator a String to use as a token separator
   * @param fillerToken a String to use to represent gaps in the input stream (due to eg stopwords)
   */
  public FixedShingleFilter(
      TokenStream input, int shingleSize, String tokenSeparator, String fillerToken) {
    super(input);

    if (shingleSize <= 1 || shingleSize > MAX_SHINGLE_SIZE) {
      throw new IllegalArgumentException(
          "Shingle size must be between 2 and " + MAX_SHINGLE_SIZE + ", got " + shingleSize);
    }
    this.shingleSize = shingleSize;
    this.tokenSeparator = tokenSeparator;
    this.fillerToken = fillerToken;
  }

  @Override
  public boolean incrementToken() throws IOException {

    int shinglePosInc, startOffset, endOffset;

    outer:
    while (true) {
      if (incrementGraph() == false) {
        if (incrementBaseToken() == false) {
          return false;
        }
        // starting a shingle at a new base position, use base position increment
        shinglePosInc = incAtt.getPositionIncrement();
      } else {
        // starting a new shingle at the same base with a different graph, use a 0
        // position increment
        shinglePosInc = 0;
      }

      startOffset = offsetAtt.startOffset();
      endOffset = offsetAtt.endOffset();
      this.buffer.setEmpty();
      this.buffer.append(termAtt);

      // build the shingle by iterating over the current graph, adding
      // filler tokens if we encounter gaps
      for (int i = 1; i < shingleSize; i++) {
        if (incrementGraphToken() == false) {
          // we've reached the end of the token stream, check for trailing
          // positions and add fillers if necessary
          int trailingPositions = getTrailingPositions();
          if (i + trailingPositions < shingleSize) {
            // not enough trailing positions to make a full shingle
            // start again at a different graph
            continue outer;
          }
          while (i < shingleSize) {
            this.buffer.append(tokenSeparator).append(fillerToken);
            i++;
          }
          break;
        }
        int posInc = incAtt.getPositionIncrement();
        if (posInc > 1) {
          // if we have a posInc > 1, we need to fill in the gaps
          if (i + posInc > shingleSize) {
            // if the posInc is greater than the shingle size, we need to add fillers
            // up to the shingle size but no further
            while (i < shingleSize) {
              this.buffer.append(tokenSeparator).append(fillerToken);
              i++;
            }
            break;
          }
          // otherwise just add them in as far as we need
          while (posInc > 1) {
            this.buffer.append(tokenSeparator).append(fillerToken);
            posInc--;
            i++;
          }
        }
        this.buffer.append(tokenSeparator).append(termAtt);
        endOffset = offsetAtt.endOffset();
      }
      break;
    }
    clearAttributes();
    this.offsetAtt.setOffset(startOffset, endOffset);
    this.incAtt.setPositionIncrement(shinglePosInc);
    this.termAtt.setEmpty().append(buffer);
    this.typeAtt.setType("shingle");
    return true;
  }
}
