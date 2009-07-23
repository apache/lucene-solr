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
    int count = 0;
    final Token reusableToken = new Token();
    for (Token nextToken = nptf.next(reusableToken); nextToken != null; nextToken = nptf.next(reusableToken)) {
      assertTrue(nextToken.type() + " is not null and it should be", nextToken.type().equals(String.valueOf(Character.toUpperCase(nextToken.termBuffer()[0]))));
      assertTrue("nextToken.getPayload() is null and it shouldn't be", nextToken.getPayload() != null);
      String type = new String(nextToken.getPayload().getData(), "UTF-8");
      assertTrue("type is null and it shouldn't be", type != null);
      assertTrue(type + " is not equal to " + nextToken.type(), type.equals(nextToken.type()) == true);
      count++;
    }
    assertTrue(count + " does not equal: " + 10, count == 10);
  }

  private class WordTokenFilter extends TokenFilter {
    private WordTokenFilter(TokenStream input) {
      super(input);
    }



    public Token next(final Token reusableToken) throws IOException {
      assert reusableToken != null;
      Token nextToken = input.next(reusableToken);
      if (nextToken != null) {
        nextToken.setType(String.valueOf(Character.toUpperCase(nextToken.termBuffer()[0])));
      }
      return nextToken;
    }
  }

}