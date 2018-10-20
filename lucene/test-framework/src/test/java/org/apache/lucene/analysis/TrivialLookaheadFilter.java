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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Simple example of a filter that seems to show some problems with LookaheadTokenFilter.
 */
final public class TrivialLookaheadFilter extends LookaheadTokenFilter<TestPosition> {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private int insertUpto;

  protected TrivialLookaheadFilter(TokenStream input) {
    super(input);
  }

  @Override
  protected TestPosition newPosition() {
    return new TestPosition();
  }

  @Override
  public boolean incrementToken() throws IOException {
    // At the outset, getMaxPos is -1. So we'll peek. When we reach the end of the sentence and go to the
    // first token of the next sentence, maxPos will be the prev sentence's end token, and we'll go again.
    if (positions.getMaxPos() < outputPos) {
      peekSentence();
    }

    return nextToken();
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    insertUpto = -1;
  }

  @Override
  protected void afterPosition() throws IOException {
    if (insertUpto < outputPos) {
      insertToken();
      // replace term with 'improved' term.
      clearAttributes();
      termAtt.setEmpty();
      posIncAtt.setPositionIncrement(0);
      termAtt.append(positions.get(outputPos).getFact());
      offsetAtt.setOffset(positions.get(outputPos).startOffset,
                          positions.get(outputPos+1).endOffset);
      insertUpto = outputPos;
    }
  }

  private void peekSentence() throws IOException {
    List<String> facts = new ArrayList<>();
    boolean haveSentence = false;
    do {
      if (peekToken()) {

        String term = new String(termAtt.buffer(), 0, termAtt.length());
        facts.add(term + "-huh?");
        if (".".equals(term)) {
          haveSentence = true;
        }

      } else {
        haveSentence = true;
      }

    } while (!haveSentence);

    // attach the (now disambiguated) analyzed tokens to the positions.
    for (int x = 0; x < facts.size(); x++) {
      // sentenceTokens is just relative to sentence, positions is absolute.
      positions.get(outputPos + x).setFact(facts.get(x));
    }
  }
}
