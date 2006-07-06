/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * A TokenFilter which filters out Tokens at the same position and Term
 * text as the previous token in the stream.
 */
public class RemoveDuplicatesTokenFilter extends BufferedTokenStream {
  public RemoveDuplicatesTokenFilter(TokenStream input) {super(input);}
  protected Token process(Token t) throws IOException {
    Token tok = read();
    OUT: while (tok != null && tok.getPositionIncrement()==0) {
      if (null != t) {
        write(t);
        t = null;
      }
      boolean dup=false;
      IN: for (Token outTok : output()) {
        if (outTok.termText().equals(tok.termText())) {
          dup=true;
          break IN;
        }
      }
      if (!dup)
        write(tok);
      tok = read();
    }
    if (tok != null) pushBack(tok);
    return t;
  }
} 
