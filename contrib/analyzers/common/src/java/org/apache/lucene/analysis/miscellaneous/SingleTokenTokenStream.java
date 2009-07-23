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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;

/**
 * A token stream containing a single token.
 */
public class SingleTokenTokenStream extends TokenStream {

  private boolean exhausted = false;
  // The token needs to be immutable, so work with clones!
  private Token token;


  public SingleTokenTokenStream(Token token) {
    assert token != null;
    this.token = (Token) token.clone();
  }


  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (exhausted) {
      return null;
    }
    exhausted = true;
    return (Token) token.clone();
  }


  public void reset() throws IOException {
    exhausted = false;
  }

  public Token getToken() {
    return (Token) token.clone();
  }

  public void setToken(Token token) {
    this.token = (Token) token.clone();
  }
}
