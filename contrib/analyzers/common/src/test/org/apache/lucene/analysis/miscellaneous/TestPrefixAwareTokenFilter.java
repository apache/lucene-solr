package org.apache.lucene.analysis.miscellaneous;

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

import junit.framework.TestCase;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

import java.io.IOException;
import java.io.StringReader;

public class TestPrefixAwareTokenFilter extends TestCase {

  public void test() throws IOException {

    PrefixAwareTokenFilter ts;

    ts = new PrefixAwareTokenFilter(
        new SingleTokenTokenStream(createToken("a", 0, 1)),
        new SingleTokenTokenStream(createToken("b", 0, 1)));
    final Token reusableToken = new Token();
    assertNext(ts, reusableToken, "a", 0, 1);
    assertNext(ts, reusableToken, "b", 1, 2);
    assertNull(ts.next(reusableToken));


    // prefix and suffix using 2x prefix

    ts = new PrefixAwareTokenFilter(new SingleTokenTokenStream(createToken("^", 0, 0)), new WhitespaceTokenizer(new StringReader("hello world")));
    ts = new PrefixAwareTokenFilter(ts, new SingleTokenTokenStream(createToken("$", 0, 0)));

    assertNext(ts, reusableToken, "^", 0, 0);
    assertNext(ts, reusableToken, "hello", 0, 5);
    assertNext(ts, reusableToken, "world", 6, 11);
    assertNext(ts, reusableToken, "$", 11, 11);
    assertNull(ts.next(reusableToken));
  }


  private Token assertNext(TokenStream ts, final Token reusableToken, String text, int startOffset, int endOffset) throws IOException {
    Token nextToken = ts.next(reusableToken);
    assertNotNull(nextToken);
    assertEquals(text, nextToken.term());
    assertEquals(startOffset, nextToken.startOffset());
    assertEquals(endOffset, nextToken.endOffset());
    return nextToken;
  }

  private static Token createToken(String term, int start, int offset)
  {
    Token token = new Token(start, offset);
    token.setTermBuffer(term);
    return token;
  }
}
