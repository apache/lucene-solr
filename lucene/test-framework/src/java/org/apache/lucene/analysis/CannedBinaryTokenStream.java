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

import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.BytesRef;

/**
 * TokenStream from a canned list of binary (BytesRef-based)
 * tokens.
 */
public final class CannedBinaryTokenStream extends TokenStream {

  /** Represents a binary token. */
  public final static class BinaryToken {
    BytesRef term;
    int posInc;
    int posLen;
    int startOffset;
    int endOffset;

    public BinaryToken(BytesRef term) {
      this.term = term;
      this.posInc = 1;
      this.posLen = 1;
    }

    public BinaryToken(BytesRef term, int posInc, int posLen) {
      this.term = term;
      this.posInc = posInc;
      this.posLen = posLen;
    }
  }

  private final BinaryToken[] tokens;
  private int upto = 0;
  private final BytesTermAttribute termAtt = addAttribute(BytesTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  public CannedBinaryTokenStream(BinaryToken... tokens) {
    super(Token.TOKEN_ATTRIBUTE_FACTORY);
    this.tokens = tokens;
  }
  
  @Override
  public boolean incrementToken() {
    if (upto < tokens.length) {
      final BinaryToken token = tokens[upto++];     
      // TODO: can we just capture/restoreState so
      // we get all attrs...?
      clearAttributes();      
      termAtt.setBytesRef(token.term);
      posIncrAtt.setPositionIncrement(token.posInc);
      posLengthAtt.setPositionLength(token.posLen);
      offsetAtt.setOffset(token.startOffset, token.endOffset);
      return true;
    } else {
      return false;
    }
  }
}
