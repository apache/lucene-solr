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

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.TeeTokenFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

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
    boolean seenDogs = false;
    final Token reusableToken = new Token();
    for (Token nextToken = ttf.next(reusableToken); nextToken != null; nextToken = ttf.next(reusableToken)) {
      if (nextToken.term().equals("dogs")) {
        seenDogs = true;
        assertTrue(nextToken.type() + " is not equal to " + "D", nextToken.type().equals("D") == true);
      } else {
        assertTrue(nextToken.type() + " is not null and it should be", nextToken.type().equals("word"));
      }
    }
    assertTrue(seenDogs + " does not equal: " + true, seenDogs == true);
    assertTrue("sink Size: " + sink.getTokens().size() + " is not: " + 1, sink.getTokens().size() == 1);
  }

  private class WordTokenFilter extends TokenFilter {
    private WordTokenFilter(TokenStream input) {
      super(input);
    }

    public Token next(final Token reusableToken) throws IOException {
      assert reusableToken != null;
      Token nextToken = input.next(reusableToken);
      if (nextToken != null && nextToken.term().equals("dogs")) {
        nextToken.setType("D");
      }
      return nextToken;
    }
  }
}