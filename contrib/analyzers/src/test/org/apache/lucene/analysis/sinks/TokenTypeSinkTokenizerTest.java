package org.apache.lucene.analysis.sinks;

/**
 * Copyright 2004 The Apache Software Foundation
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

import junit.framework.TestCase;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.payloads.NumericPayloadTokenFilter;

import java.io.IOException;
import java.io.StringReader;

public class TokenTypeSinkTokenizerTest extends TestCase {


  public TokenTypeSinkTokenizerTest(String s) {
    super(s);
  }

  protected void setUp() {
  }

  protected void tearDown() {

  }

  public void test() throws IOException {
    TokenTypeSinkTokenizer sink = new TokenTypeSinkTokenizer("D");
    String test = "The quick red fox jumped over the lazy brown dogs";

    TeeTokenFilter ttf = new TeeTokenFilter(new WordTokenFilter(new WhitespaceTokenizer(new StringReader(test))), sink);
    Token tok = new Token();
    boolean seenDogs = false;
    while ((tok = ttf.next(tok)) != null) {
      if (tok.termText().equals("dogs")) {
        seenDogs = true;
        assertTrue(tok.type() + " is not equal to " + "D", tok.type().equals("D") == true);
      } else {
        assertTrue(tok.type() + " is not null and it should be", tok.type().equals("word"));
      }
    }
    assertTrue(seenDogs + " does not equal: " + true, seenDogs == true);
    assertTrue("sink Size: " + sink.getTokens().size() + " is not: " + 1, sink.getTokens().size() == 1);
  }

  private class WordTokenFilter extends TokenFilter {
    private WordTokenFilter(TokenStream input) {
      super(input);
    }

    public Token next(Token result) throws IOException {
      result = input.next(result);
      if (result != null && result.termText().equals("dogs")) {
        result.setType("D");
      }
      return result;
    }
  }
}