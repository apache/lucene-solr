package org.apache.lucene.search.highlight;

/**
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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * One, or several overlapping tokens, along with the score(s) and the scope of
 * the original text
 */
public class TokenGroup {

  private static final int MAX_NUM_TOKENS_PER_GROUP = 50;
  Token [] tokens=new Token[MAX_NUM_TOKENS_PER_GROUP];
  float[] scores = new float[MAX_NUM_TOKENS_PER_GROUP];
  int numTokens = 0;
  int startOffset = 0;
  int endOffset = 0;
  float tot;
  int matchStartOffset, matchEndOffset;

  private OffsetAttribute offsetAtt;
  private CharTermAttribute termAtt;

  public TokenGroup(TokenStream tokenStream) {
    offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
    termAtt = tokenStream.addAttribute(CharTermAttribute.class);
  }

  void addToken(float score) {
    if (numTokens < MAX_NUM_TOKENS_PER_GROUP) {
      int termStartOffset = offsetAtt.startOffset();
      int termEndOffset = offsetAtt.endOffset();
      if (numTokens == 0) {
        startOffset = matchStartOffset = termStartOffset;
        endOffset = matchEndOffset = termEndOffset;
        tot += score;
      } else {
        startOffset = Math.min(startOffset, termStartOffset);
        endOffset = Math.max(endOffset, termEndOffset);
        if (score > 0) {
          if (tot == 0) {
            matchStartOffset = offsetAtt.startOffset();
            matchEndOffset = offsetAtt.endOffset();
          } else {
            matchStartOffset = Math.min(matchStartOffset, termStartOffset);
            matchEndOffset = Math.max(matchEndOffset, termEndOffset);
          }
          tot += score;
        }
      }
      Token token = new Token(termStartOffset, termEndOffset);
      token.setEmpty().append(termAtt);
      tokens[numTokens] = token;
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
  
  /* 
  * @param index a value between 0 and numTokens -1
  * @return the "n"th token
  */
 public Token getToken(int index)
 {
     return tokens[index];
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
   * @return the end position in the original text
   */
  public int getEndOffset() {
    return endOffset;
  }

  /**
   * @return the number of tokens in this group
   */
  public int getNumTokens() {
    return numTokens;
  }

  /**
   * @return the start position in the original text
   */
  public int getStartOffset() {
    return startOffset;
  }

  /**
   * @return all tokens' scores summed up
   */
  public float getTotalScore() {
    return tot;
  }
}
