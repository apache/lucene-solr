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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;
import java.io.StringReader;

public class TestPrefixAndSuffixAwareTokenFilter extends BaseTokenStreamTestCase {

  public void test() throws IOException {

    PrefixAndSuffixAwareTokenFilter ts = new PrefixAndSuffixAwareTokenFilter(
        new SingleTokenTokenStream(createToken("^", 0, 0)),
        new WhitespaceTokenizer(new StringReader("hello world")),
        new SingleTokenTokenStream(createToken("$", 0, 0)));

    assertNext(ts, "^", 0, 0);
    assertNext(ts, "hello", 0, 5);
    assertNext(ts, "world", 6, 11);
    assertNext(ts, "$", 11, 11);
    assertFalse(ts.incrementToken());
  }


  private void assertNext(TokenStream ts, String text, int startOffset, int endOffset) throws IOException {
    TermAttribute termAtt = ts.addAttribute(TermAttribute.class);
    OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);

    assertTrue(ts.incrementToken());
    assertEquals(text, termAtt.term());
    assertEquals(startOffset, offsetAtt.startOffset());
    assertEquals(endOffset, offsetAtt.endOffset());
  }

  private static Token createToken(String term, int start, int offset)
  {
    Token token = new Token(start, offset);
    token.setTermBuffer(term);
    return token;
  }
}
