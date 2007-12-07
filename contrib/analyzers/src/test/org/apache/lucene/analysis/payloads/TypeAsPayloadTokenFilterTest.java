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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.WhitespaceTokenizer;

import java.io.IOException;
import java.io.StringReader;

public class TypeAsPayloadTokenFilterTest extends TestCase {


  public TypeAsPayloadTokenFilterTest(String s) {
    super(s);
  }

  protected void setUp() {
  }

  protected void tearDown() {

  }


  public void test() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    TypeAsPayloadTokenFilter nptf = new TypeAsPayloadTokenFilter(new WordTokenFilter(new WhitespaceTokenizer(new StringReader(test))));
    Token tok = new Token();
    int count = 0;
    while ((tok = nptf.next(tok)) != null){
      assertTrue(tok.type() + " is not null and it should be", tok.type().equals(String.valueOf(Character.toUpperCase(tok.termBuffer()[0]))));
      assertTrue("tok.getPayload() is null and it shouldn't be", tok.getPayload() != null);
      String type = new String(tok.getPayload().getData(), "UTF-8");
      assertTrue("type is null and it shouldn't be", type != null);
      assertTrue(type + " is not equal to " + tok.type(), type.equals(tok.type()) == true);
      count++;
    }
    assertTrue(count + " does not equal: " + 10, count == 10);
  }

  private class WordTokenFilter extends TokenFilter {
    private WordTokenFilter(TokenStream input) {
      super(input);
    }



    public Token next(Token result) throws IOException {
      result = input.next(result);
      if (result != null) {
        result.setType(String.valueOf(Character.toUpperCase(result.termBuffer()[0])));
      }
      return result;
    }
  }

}