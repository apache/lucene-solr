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

import org.apache.commons.codec.Encoder;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

import java.io.IOException;

/**
 * Create tokens for phonetic matches.  See:
 * http://jakarta.apache.org/commons/codec/api-release/org/apache/commons/codec/language/package-summary.html
 *
 * @version $Id$
 */
public class PhoneticFilter extends TokenFilter 
{
  protected boolean inject = true; 
  protected Encoder encoder = null;
  protected String name = null;
  
  protected Token save = null;

  public PhoneticFilter(TokenStream in, Encoder encoder, String name, boolean inject) {
    super(in);
    this.encoder = encoder;
    this.name = name;
    this.inject = inject;
  }

  @Override
  public final Token next(Token in) throws IOException {
    if( save != null ) {
      Token temp = save;
      save = null;
      return temp;
    }
    
    Token t = input.next(in);
    if( t != null ) {
      String value = new String(t.termBuffer(), 0, t.termLength());
      try {
        value = encoder.encode(value).toString();
      } 
      catch (Exception ignored) {} // just use the direct text
      //Token m = new Token(value, t.startOffset(), t.endOffset(), name );
      if( inject ) {
        save = (Token) t.clone();
        save.setPositionIncrement(0);
        save.setTermBuffer(value.toCharArray(), 0, value.length());
      } else {
        t.setTermBuffer(value.toCharArray(), 0, value.length());
      }
    }
    return t;
  }
}
