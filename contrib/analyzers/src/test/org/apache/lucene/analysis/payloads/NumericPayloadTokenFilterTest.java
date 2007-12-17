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
    Token tok = new Token();
    boolean seenDogs = false;
    while ((tok = nptf.next(tok)) != null){
      if (tok.termText().equals("dogs")){
        seenDogs = true;
        assertTrue(tok.type() + " is not equal to " + "D", tok.type().equals("D") == true);
        assertTrue("tok.getPayload() is null and it shouldn't be", tok.getPayload() != null);
        byte [] bytes = tok.getPayload().getData();//safe here to just use the bytes, otherwise we should use offset, length
        assertTrue(bytes.length + " does not equal: " + tok.getPayload().length(), bytes.length == tok.getPayload().length());
        assertTrue(tok.getPayload().getOffset() + " does not equal: " + 0, tok.getPayload().getOffset() == 0);
        float pay = PayloadHelper.decodeFloat(bytes);
        assertTrue(pay + " does not equal: " + 3, pay == 3);
      } else {
        assertTrue(tok.type() + " is not null and it should be", tok.type().equals("word"));
      }
    }
    assertTrue(seenDogs + " does not equal: " + true, seenDogs == true);
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