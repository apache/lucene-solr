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
package org.apache.solr.analysis;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

public class DoubleMetaphoneFilter extends TokenFilter {

  private static final String TOKEN_TYPE = "DoubleMetaphone";
  
  private final LinkedList<Token> remainingTokens = new LinkedList<Token>();
  private final DoubleMetaphone encoder = new DoubleMetaphone();
  private final boolean inject;
  
  protected DoubleMetaphoneFilter(TokenStream input, int maxCodeLength, boolean inject) {
    super(input);
    this.encoder.setMaxCodeLen(maxCodeLength);
    this.inject = inject;
  }

  @Override
  public final Token next(Token in) throws IOException {
    if (!remainingTokens.isEmpty()) {
      return remainingTokens.removeFirst();
    }

    Token t = input.next(in);
    if (t != null) {
      if (inject) {
        remainingTokens.addLast(t);
      }

      boolean isPhonetic = false;
      String v = new String(t.termBuffer(), 0, t.termLength());
      String primaryPhoneticValue = encoder.doubleMetaphone(v);
      if (primaryPhoneticValue.length() > 0) {
        Token token = (Token) t.clone();
        if( inject ) {
          token.setPositionIncrement( 0 );
        }
        token.setType( TOKEN_TYPE );
        token.setTermBuffer(primaryPhoneticValue);
        remainingTokens.addLast(token);
        isPhonetic = true;
      }

      String alternatePhoneticValue = encoder.doubleMetaphone(v, true);
      if (alternatePhoneticValue.length() > 0
          && !primaryPhoneticValue.equals(alternatePhoneticValue)) {
        Token token = (Token) t.clone();
        token.setPositionIncrement( 0 );
        token.setType( TOKEN_TYPE );
        token.setTermBuffer(alternatePhoneticValue);
        remainingTokens.addLast(token);
        isPhonetic = true;
      }
      
      // If we did not add something, then go to the next one...
      if( !isPhonetic ) {
        t = next(in);
        if( t != null ) {
          t.setPositionIncrement( t.getPositionIncrement()+1 ); 
        }
        return t;
      }
    }

    return remainingTokens.isEmpty() ? null : remainingTokens.removeFirst();
  }
}
