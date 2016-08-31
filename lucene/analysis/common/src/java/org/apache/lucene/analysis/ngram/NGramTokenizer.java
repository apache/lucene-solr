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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.util.AttributeFactory;

/**
 * Tokenizes the input into n-grams of the given size(s).
 * <p>On the contrary to {@link NGramTokenFilter}, this class sets offsets so
 * that characters between startOffset and endOffset in the original stream are
 * the same as the term chars.
 * <p>For example, "abcde" would be tokenized as (minGram=2, maxGram=3):
 * <table summary="ngram tokens example">
 * <tr><th>Term</th><td>ab</td><td>abc</td><td>bc</td><td>bcd</td><td>cd</td><td>cde</td><td>de</td></tr>
 * <tr><th>Position increment</th><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td></tr>
 * <tr><th>Position length</th><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td></tr>
 * <tr><th>Offsets</th><td>[0,2[</td><td>[0,3[</td><td>[1,3[</td><td>[1,4[</td><td>[2,4[</td><td>[2,5[</td><td>[3,5[</td></tr>
 * </table>
 * <a name="version"></a>
 * <p>This tokenizer changed a lot in Lucene 4.4 in order to:<ul>
 * <li>tokenize in a streaming fashion to support streams which are larger
 * than 1024 chars (limit of the previous version),
 * <li>count grams based on unicode code points instead of java chars (and
 * never split in the middle of surrogate pairs),
 * <li>give the ability to {@link #isTokenChar(int) pre-tokenize} the stream
 * before computing n-grams.</ul>
 * <p>Additionally, this class doesn't trim trailing whitespaces and emits
 * tokens in a different order, tokens are now emitted by increasing start
 * offsets while they used to be emitted by increasing lengths (which prevented
 * from supporting large input streams).
 */
// non-final to allow for overriding isTokenChar, but all other methods should be final
public class NGramTokenizer extends Tokenizer {
  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;

  private CharacterUtils.CharacterBuffer charBuffer;
  private int[] buffer; // like charBuffer, but converted to code points
  private int bufferStart, bufferEnd; // remaining slice in buffer
  private int offset;
  private int gramSize;
  private int minGram, maxGram;
  private boolean exhausted;
  private int lastCheckedChar; // last offset in the buffer that we checked
  private int lastNonTokenChar; // last offset that we found to not be a token char
  private boolean edgesOnly; // leading edges n-grams only

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  NGramTokenizer(int minGram, int maxGram, boolean edgesOnly) {
    init(minGram, maxGram, edgesOnly);
  }

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public NGramTokenizer(int minGram, int maxGram) {
    this(minGram, maxGram, false);
  }

  NGramTokenizer(AttributeFactory factory, int minGram, int maxGram, boolean edgesOnly) {
    super(factory);
    init(minGram, maxGram, edgesOnly);
  }

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param factory {@link org.apache.lucene.util.AttributeFactory} to use
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public NGramTokenizer(AttributeFactory factory, int minGram, int maxGram) {
    this(factory, minGram, maxGram, false);
  }

  /**
   * Creates NGramTokenizer with default min and max n-grams.
   */
  public NGramTokenizer() {
    this(DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE);
  }

  private void init(int minGram, int maxGram, boolean edgesOnly) {
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
    this.edgesOnly = edgesOnly;
    charBuffer = CharacterUtils.newCharacterBuffer(2 * maxGram + 1024); // 2 * maxGram in case all code points require 2 chars and + 1024 for buffering to not keep polling the Reader
    buffer = new int[charBuffer.getBuffer().length];
    // Make the term att large enough
    termAtt.resizeBuffer(2 * maxGram);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();

    // termination of this loop is guaranteed by the fact that every iteration
    // either advances the buffer (calls consumes()) or increases gramSize
    while (true) {
      // compact
      if (bufferStart >= bufferEnd - maxGram - 1 && !exhausted) {
        System.arraycopy(buffer, bufferStart, buffer, 0, bufferEnd - bufferStart);
        bufferEnd -= bufferStart;
        lastCheckedChar -= bufferStart;
        lastNonTokenChar -= bufferStart;
        bufferStart = 0;

        // fill in remaining space
        exhausted = !CharacterUtils.fill(charBuffer, input, buffer.length - bufferEnd);
        // convert to code points
        bufferEnd += CharacterUtils.toCodePoints(charBuffer.getBuffer(), 0, charBuffer.getLength(), buffer, bufferEnd);
      }

      // should we go to the next offset?
      if (gramSize > maxGram || (bufferStart + gramSize) > bufferEnd) {
        if (bufferStart + 1 + minGram > bufferEnd) {
          assert exhausted;
          return false;
        }
        consume();
        gramSize = minGram;
      }

      updateLastNonTokenChar();

      // retry if the token to be emitted was going to not only contain token chars
      final boolean termContainsNonTokenChar = lastNonTokenChar >= bufferStart && lastNonTokenChar < (bufferStart + gramSize);
      final boolean isEdgeAndPreviousCharIsTokenChar = edgesOnly && lastNonTokenChar != bufferStart - 1;
      if (termContainsNonTokenChar || isEdgeAndPreviousCharIsTokenChar) {
        consume();
        gramSize = minGram;
        continue;
      }

      final int length = CharacterUtils.toChars(buffer, bufferStart, gramSize, termAtt.buffer(), 0);
      termAtt.setLength(length);
      posIncAtt.setPositionIncrement(1);
      posLenAtt.setPositionLength(1);
      offsetAtt.setOffset(correctOffset(offset), correctOffset(offset + length));
      ++gramSize;
      return true;
    }
  }

  private void updateLastNonTokenChar() {
    final int termEnd = bufferStart + gramSize - 1;
    if (termEnd > lastCheckedChar) {
      for (int i = termEnd; i > lastCheckedChar; --i) {
        if (!isTokenChar(buffer[i])) {
          lastNonTokenChar = i;
          break;
        }
      }
      lastCheckedChar = termEnd;
    }
  }

  /** Consume one code point. */
  private void consume() {
    offset += Character.charCount(buffer[bufferStart++]);
  }

  /** Only collect characters which satisfy this condition. */
  protected boolean isTokenChar(int chr) {
    return true;
  }

  @Override
  public final void end() throws IOException {
    super.end();
    assert bufferStart <= bufferEnd;
    int endOffset = offset;
    for (int i = bufferStart; i < bufferEnd; ++i) {
      endOffset += Character.charCount(buffer[i]);
    }
    endOffset = correctOffset(endOffset);
    // set final offset
    offsetAtt.setOffset(endOffset, endOffset);
  }

  @Override
  public final void reset() throws IOException {
    super.reset();
    bufferStart = bufferEnd = buffer.length;
    lastNonTokenChar = lastCheckedChar = bufferStart - 1;
    offset = 0;
    gramSize = minGram;
    exhausted = false;
    charBuffer.reset();
  }
}
