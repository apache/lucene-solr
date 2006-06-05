package org.apache.lucene.analysis.th;

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

import java.io.IOException;
import java.util.Locale;
import java.lang.Character.UnicodeBlock;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import java.text.BreakIterator;

/**
 * TokenFilter that use java.text.BreakIterator to break each 
 * Token that is Thai into separate Token(s) for each Thai word.
 * @author Samphan Raruenrom <samphan@osdev.co.th> for To-Be-One Technology Co., Ltd.
 * @version 0.2
 */
public class ThaiWordFilter extends TokenFilter {
  
  private BreakIterator breaker = null;
  private Token thaiToken = null;
  
  public ThaiWordFilter(TokenStream input) {
    super(input);
    breaker = BreakIterator.getWordInstance(new Locale("th"));
  }
  
  public Token next() throws IOException {
    if (thaiToken != null) {
      String text = thaiToken.termText();
      int start = breaker.current();
      int end = breaker.next();
      if (end != BreakIterator.DONE) {
        return new Token(text.substring(start, end), 
            thaiToken.startOffset()+start, thaiToken.startOffset()+end, thaiToken.type());
      }
      thaiToken = null;
    }
    Token tk = input.next();
    if (tk == null) {
      return null;
    }
    String text = tk.termText();
    if (UnicodeBlock.of(text.charAt(0)) != UnicodeBlock.THAI) {
      return new Token(text.toLowerCase(), tk.startOffset(), tk.endOffset(), tk.type());
    }
    thaiToken = tk;
    breaker.setText(text);
    int end = breaker.next();
    if (end != BreakIterator.DONE) {
      return new Token(text.substring(0, end), 
          thaiToken.startOffset(), thaiToken.startOffset()+end, thaiToken.type());
    }
    return null;
  }
}
