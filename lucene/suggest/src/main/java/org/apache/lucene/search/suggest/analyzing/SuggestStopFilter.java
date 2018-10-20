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
package org.apache.lucene.search.suggest.analyzing;

import java.io.IOException;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/** Like {@link StopFilter} except it will not remove the
 *  last token if that token was not followed by some token
 *  separator.  For example, a query 'find the' would
 *  preserve the 'the' since it was not followed by a space or
 *  punctuation or something, and mark it KEYWORD so future
 *  stemmers won't touch it either while a query like "find
 *  the popsicle' would remove 'the' as a stopword.
 *
 *  <p>Normally you'd use the ordinary {@link StopFilter}
 *  in your indexAnalyzer and then this class in your
 *  queryAnalyzer, when using one of the analyzing suggesters. */

public final class SuggestStopFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final CharArraySet stopWords;

  private State endState;

  /** Sole constructor. */
  public SuggestStopFilter(TokenStream input, CharArraySet stopWords) {
    super(input);
    this.stopWords = stopWords;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    endState = null;
  }

  @Override
  public void end() throws IOException {
    if (endState == null) {
      super.end();
    } else {
      // NOTE: we already called .end() from our .next() when
      // the stream was complete, so we do not call
      // super.end() here
      restoreState(endState);
    }
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (endState != null) {
      return false;
    }

    if (!input.incrementToken()) {
      return false;
    }

    int skippedPositions = 0;
    while (true) {
      if (stopWords.contains(termAtt.buffer(), 0, termAtt.length())) {
        int posInc = posIncAtt.getPositionIncrement();
        int endOffset = offsetAtt.endOffset();
        // This token may be a stopword, if it's not end:
        State sav = captureState();
        if (input.incrementToken()) {
          // It was a stopword; skip it
          skippedPositions += posInc;
        } else {
          clearAttributes();
          input.end();
          endState = captureState();
          int finalEndOffset = offsetAtt.endOffset();
          assert finalEndOffset >= endOffset;
          if (finalEndOffset > endOffset) {
            // OK there was a token separator after the
            // stopword, so it was a stopword
            return false;
          } else {
            // No token separator after final token that
            // looked like a stop-word; don't filter it:
            restoreState(sav);
            posIncAtt.setPositionIncrement(skippedPositions + posIncAtt.getPositionIncrement());
            keywordAtt.setKeyword(true);
            return true;
          }
        }
      } else {
        // Not a stopword; return the current token:
        posIncAtt.setPositionIncrement(skippedPositions + posIncAtt.getPositionIncrement());
        return true;
      }
    }
  }
}
