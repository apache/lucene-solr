package org.apache.lucene.analysis.payloads;

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
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

import java.io.IOException;
import java.io.StringReader;

public class NumericPayloadTokenFilterTest extends TestCase {


  public NumericPayloadTokenFilterTest(String s) {
    super(s);
  }

  protected void setUp() {
  }

  protected void tearDown() {

  }

  public void test() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    NumericPayloadTokenFilter nptf = new NumericPayloadTokenFilter(new WordTokenFilter(new WhitespaceTokenizer(new StringReader(test))), 3, "D");
    boolean seenDogs = false;
    final Token reusableToken = new Token();
    for (Token nextToken = nptf.next(reusableToken); nextToken != null; nextToken = nptf.next(reusableToken)) {
      if (nextToken.term().equals("dogs")){
        seenDogs = true;
        assertTrue(nextToken.type() + " is not equal to " + "D", nextToken.type().equals("D") == true);
        assertTrue("nextToken.getPayload() is null and it shouldn't be", nextToken.getPayload() != null);
        byte [] bytes = nextToken.getPayload().getData();//safe here to just use the bytes, otherwise we should use offset, length
        assertTrue(bytes.length + " does not equal: " + nextToken.getPayload().length(), bytes.length == nextToken.getPayload().length());
        assertTrue(nextToken.getPayload().getOffset() + " does not equal: " + 0, nextToken.getPayload().getOffset() == 0);
        float pay = PayloadHelper.decodeFloat(bytes);
        assertTrue(pay + " does not equal: " + 3, pay == 3);
      } else {
        assertTrue(nextToken.type() + " is not null and it should be", nextToken.type().equals("word"));
      }
    }
    assertTrue(seenDogs + " does not equal: " + true, seenDogs == true);
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