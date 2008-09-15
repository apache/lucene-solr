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
 * @version 0.2
 */
public class ThaiWordFilter extends TokenFilter {
  
  private BreakIterator breaker = null;
  private Token thaiToken = null;
  
  public ThaiWordFilter(TokenStream input) {
    super(input);
    breaker = BreakIterator.getWordInstance(new Locale("th"));
  }
  
  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (thaiToken != null) {
      int start = breaker.current();
      int end = breaker.next();
      if (end != BreakIterator.DONE) {
        reusableToken.reinit(thaiToken, thaiToken.termBuffer(), start, end - start);
        reusableToken.setStartOffset(thaiToken.startOffset()+start);
        reusableToken.setEndOffset(thaiToken.endOffset()+end);
        return reusableToken;
      }
      thaiToken = null;
    }

    Token nextToken = input.next(reusableToken);
    if (nextToken == null || nextToken.termLength() == 0) {
      return null;
    }

    String text = nextToken.term();
    if (UnicodeBlock.of(text.charAt(0)) != UnicodeBlock.THAI) {
      nextToken.setTermBuffer(text.toLowerCase());
      return nextToken;
    }

    thaiToken = (Token) nextToken.clone();
    breaker.setText(text);
    int end = breaker.next();
    if (end != BreakIterator.DONE) {
      nextToken.setTermBuffer(text, 0, end);
      nextToken.setEndOffset(nextToken.startOffset() + end);
      return nextToken;
    }
    return null;
  }
}
