package org.apache.lucene.analysis.miscellaneous;

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

import java.io.IOException;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * A token stream containing a single token.
 */
public class SingleTokenTokenStream extends TokenStream {

  private boolean exhausted = false;
  // The token needs to be immutable, so work with clones!
  private Token singleToken;

  private TermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private FlagsAttribute flagsAtt;
  private PositionIncrementAttribute posIncAtt;
  private TypeAttribute typeAtt;
  private PayloadAttribute payloadAtt;

  public SingleTokenTokenStream(Token token) {
    assert token != null;
    this.singleToken = (Token) token.clone();
     
    termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    offsetAtt = (OffsetAttribute) addAttribute(OffsetAttribute.class);
    flagsAtt = (FlagsAttribute) addAttribute(FlagsAttribute.class);
    posIncAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
    typeAtt = (TypeAttribute) addAttribute(TypeAttribute.class);
    payloadAtt = (PayloadAttribute) addAttribute(PayloadAttribute.class);
  }


  public final boolean incrementToken() throws IOException {
    if (exhausted) {
      return false;
    }
    
    Token clone = (Token) singleToken.clone();
    
    termAtt.setTermBuffer(clone.termBuffer(), 0, clone.termLength());
    offsetAtt.setOffset(clone.startOffset(), clone.endOffset());
    flagsAtt.setFlags(clone.getFlags());
    typeAtt.setType(clone.type());
    posIncAtt.setPositionIncrement(clone.getPositionIncrement());
    payloadAtt.setPayload(clone.getPayload());
    exhausted = true;
    return true;
  }
  
  /** @deprecated Will be removed in Lucene 3.0. This method is final, as it should
   * not be overridden. Delegates to the backwards compatibility layer. */
  public final Token next(final Token reusableToken) throws java.io.IOException {
    return super.next(reusableToken);
  }

  /** @deprecated Will be removed in Lucene 3.0. This method is final, as it should
   * not be overridden. Delegates to the backwards compatibility layer. */
  public final Token next() throws java.io.IOException {
    return super.next();
  }

  public void reset() throws IOException {
    exhausted = false;
  }

  public Token getToken() {
    return (Token) singleToken.clone();
  }

  public void setToken(Token token) {
    this.singleToken = (Token) token.clone();
  }
}
