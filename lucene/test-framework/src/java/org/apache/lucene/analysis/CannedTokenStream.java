package org.apache.lucene.analysis;

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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;

/**
 * TokenStream from a canned list of Tokens.
 */
public final class CannedTokenStream extends TokenStream {
  private final Token[] tokens;
  private int upto = 0;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
  
  public CannedTokenStream(Token... tokens) {
    this.tokens = tokens;
  }
  
  @Override
  public boolean incrementToken() {
    if (upto < tokens.length) {
      final Token token = tokens[upto++];     
      // TODO: can we just capture/restoreState so
      // we get all attrs...?
      clearAttributes();      
      termAtt.setEmpty();
      termAtt.append(token.toString());
      posIncrAtt.setPositionIncrement(token.getPositionIncrement());
      posLengthAtt.setPositionLength(token.getPositionLength());
      offsetAtt.setOffset(token.startOffset(), token.endOffset());
      payloadAtt.setPayload(token.getPayload());
      return true;
    } else {
      return false;
    }
  }
}
