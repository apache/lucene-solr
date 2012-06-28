package org.apache.lucene.analysis.miscellaneous;

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

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * A {@link TokenStream} containing a single token.
 */
public final class SingleTokenTokenStream extends TokenStream {

  private boolean exhausted = false;
  
  // The token needs to be immutable, so work with clones!
  private Token singleToken;
  private final AttributeImpl tokenAtt;

  public SingleTokenTokenStream(Token token) {
    super(Token.TOKEN_ATTRIBUTE_FACTORY);
    
    assert token != null;
    this.singleToken = token.clone();
    
    tokenAtt = (AttributeImpl) addAttribute(CharTermAttribute.class);
    assert (tokenAtt instanceof Token);
  }

  @Override
  public final boolean incrementToken() {
    if (exhausted) {
      return false;
    } else {
      clearAttributes();
      singleToken.copyTo(tokenAtt);
      exhausted = true;
      return true;
    }
  }

  @Override
  public void reset() {
    exhausted = false;
  }

  public Token getToken() {
    return singleToken.clone();
  }

  public void setToken(Token token) {
    this.singleToken = token.clone();
  }
}
