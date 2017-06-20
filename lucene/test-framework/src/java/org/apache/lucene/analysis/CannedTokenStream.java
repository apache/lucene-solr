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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * TokenStream from a canned list of Tokens.
 */
public final class CannedTokenStream extends TokenStream {
  private final Token[] tokens;
  private int upto = 0;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final int finalOffset;
  private final int finalPosInc;

  public CannedTokenStream(Token... tokens) {
    this(0, 0, tokens);
  }

  /** If you want trailing holes, pass a non-zero
   *  finalPosInc. */
  public CannedTokenStream(int finalPosInc, int finalOffset, Token... tokens) {
    super(Token.TOKEN_ATTRIBUTE_FACTORY);
    this.tokens = tokens;
    this.finalOffset = finalOffset;
    this.finalPosInc = finalPosInc;
  }

  @Override
  public void end() throws IOException {
    super.end();
    posIncrAtt.setPositionIncrement(finalPosInc);
    offsetAtt.setOffset(finalOffset, finalOffset);
  }
  
  @Override
  public boolean incrementToken() {
    if (upto < tokens.length) {
      clearAttributes();
      // NOTE: this looks weird, casting offsetAtt to Token, but because we are using the Token class's AttributeFactory, all attributes are
      // in fact backed by the Token class, so we just copy the current token into our Token:
      tokens[upto++].copyTo((Token) offsetAtt);
      return true;
    } else {
      return false;
    }
  }
}
