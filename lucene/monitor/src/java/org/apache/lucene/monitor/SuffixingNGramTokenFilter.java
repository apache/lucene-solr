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

package org.apache.lucene.monitor;

import java.io.IOException;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;

final class SuffixingNGramTokenFilter extends TokenFilter {

  private final String suffix;
  private final int maxTokenLength;
  private final String anyToken;

  private char[] curTermBuffer;
  private int curTermLength;
  private int curCodePointCount;
  private int curGramSize;
  private int curPos;
  private int curPosInc, curPosLen;
  private int tokStart;
  private int tokEnd;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt;
  private final PositionLengthAttribute posLenAtt;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);

  private final CharArraySet seenSuffixes = new CharArraySet(1024, false);
  private final CharArraySet seenInfixes = new CharArraySet(1024, false);

  /**
   * Creates SuffixingNGramTokenFilter.
   *
   * @param input {@link org.apache.lucene.analysis.TokenStream} holding the input to be tokenized
   * @param suffix a string to suffix to all ngrams
   * @param wildcardToken a token to emit if the input token is longer than maxTokenLength
   * @param maxTokenLength tokens longer than this will not be ngrammed
   */
  public SuffixingNGramTokenFilter(
      TokenStream input, String suffix, String wildcardToken, int maxTokenLength) {
    super(input);

    this.suffix = suffix;
    this.anyToken = wildcardToken;
    this.maxTokenLength = maxTokenLength;

    posIncAtt = addAttribute(PositionIncrementAttribute.class);
    posLenAtt = addAttribute(PositionLengthAttribute.class);
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public final boolean incrementToken() throws IOException {
    while (true) {
      if (curTermBuffer == null) {

        if (!input.incrementToken()) {
          return false;
        }

        if (keywordAtt.isKeyword()) return true;

        curTermBuffer = termAtt.buffer().clone();
        curTermLength = termAtt.length();
        curCodePointCount = Character.codePointCount(termAtt, 0, termAtt.length());
        curGramSize = curTermLength;
        curPos = 0;
        curPosInc = posIncAtt.getPositionIncrement();
        curPosLen = posLenAtt.getPositionLength();
        tokStart = offsetAtt.startOffset();
        tokEnd = offsetAtt.endOffset();
        // termAtt.setEmpty().append(suffix);
        return true;
      }

      if (curTermLength > maxTokenLength) {
        clearAttributes();
        termAtt.append(anyToken);
        curTermBuffer = null;
        return true;
      }

      if (curGramSize == 0) {
        ++curPos;
        curGramSize = curTermLength - curPos;
      }
      if (curGramSize >= 0 && (curPos + curGramSize) <= curCodePointCount) {
        clearAttributes();
        final int start = Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curPos);
        final int end =
            Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, start, curGramSize);
        termAtt.copyBuffer(curTermBuffer, start, end - start);
        termAtt.append(suffix);
        if ((curGramSize == curTermLength - curPos)
            && !seenSuffixes.add(termAtt.subSequence(0, termAtt.length()))) {
          curTermBuffer = null;
          continue;
        }
        if (!seenInfixes.add(termAtt.subSequence(0, termAtt.length()))) {
          curGramSize = 0;
          continue;
        }
        posIncAtt.setPositionIncrement(curPosInc);
        curPosInc = 0;
        posLenAtt.setPositionLength(curPosLen);
        offsetAtt.setOffset(tokStart, tokEnd);
        curGramSize--;
        return true;
      }

      curTermBuffer = null;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    curTermBuffer = null;
    seenInfixes.clear();
    seenSuffixes.clear();
  }
}
