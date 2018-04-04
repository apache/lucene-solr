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
package org.apache.lucene.search.highlight;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * One, or several overlapping tokens, along with the score(s) and the scope of
 * the original text.
 */
public class TokenGroup {

  private static final int MAX_NUM_TOKENS_PER_GROUP = 50;

  private float[] scores = new float[MAX_NUM_TOKENS_PER_GROUP];
  private int numTokens = 0;
  private int startOffset = 0;
  private int endOffset = 0;
  private float tot;
  private int matchStartOffset;
  private int matchEndOffset;

  private OffsetAttribute offsetAtt;
  private CharTermAttribute termAtt;

  public TokenGroup(TokenStream tokenStream) {
    offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
    termAtt = tokenStream.addAttribute(CharTermAttribute.class);
  }

  void addToken(float score) {
    if (numTokens < MAX_NUM_TOKENS_PER_GROUP) {
      final int termStartOffset = offsetAtt.startOffset();
      final int termEndOffset = offsetAtt.endOffset();
      if (numTokens == 0) {
        startOffset = matchStartOffset = termStartOffset;
        endOffset = matchEndOffset = termEndOffset;
        tot += score;
      } else {
        startOffset = Math.min(startOffset, termStartOffset);
        endOffset = Math.max(endOffset, termEndOffset);
        if (score > 0) {
          if (tot == 0) {
            matchStartOffset = termStartOffset;
            matchEndOffset = termEndOffset;
          } else {
            matchStartOffset = Math.min(matchStartOffset, termStartOffset);
            matchEndOffset = Math.max(matchEndOffset, termEndOffset);
          }
          tot += score;
        }
      }

      scores[numTokens] = score;
      numTokens++;
    }
  }

  boolean isDistinct() {
    return offsetAtt.startOffset() >= endOffset;
  }

  void clear() {
    numTokens = 0;
    tot = 0;
  }

  /**
   * 
   * @param index a value between 0 and numTokens -1
   * @return the "n"th score
   */
  public float getScore(int index) {
    return scores[index];
  }

  /**
   * @return the earliest start offset in the original text of a matching token in this group (score &gt; 0), or
   * if there are none then the earliest offset of any token in the group.
   */
  public int getStartOffset() {
    return matchStartOffset;
  }

  /**
   * @return the latest end offset in the original text of a matching token in this group (score &gt; 0), or
   * if there are none then {@link #getEndOffset()}.
   */
  public int getEndOffset() {
    return matchEndOffset;
  }

  /**
   * @return the number of tokens in this group
   */
  public int getNumTokens() {
    return numTokens;
  }

  /**
   * @return all tokens' scores summed up
   */
  public float getTotalScore() {
    return tot;
  }

}
