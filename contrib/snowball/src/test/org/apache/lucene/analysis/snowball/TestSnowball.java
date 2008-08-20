package org.apache.lucene.analysis.snowball;

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

import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.Payload;
import org.apache.lucene.analysis.TokenStream;

public class TestSnowball extends TestCase {

  public void assertAnalyzesTo(Analyzer a,
                               String input,
                               String[] output) throws Exception {
    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    final Token reusableToken = new Token();
    for (int i = 0; i < output.length; i++) {
      Token nextToken = ts.next(reusableToken);
      assertEquals(output[i], nextToken.term());
    }
    assertNull(ts.next(reusableToken));
    ts.close();
  }

  public void testEnglish() throws Exception {
    Analyzer a = new SnowballAnalyzer("English");
    assertAnalyzesTo(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
  }


  public void testFilterTokens() throws Exception {
    final Token tok = new Token(2, 7, "wrd");
    tok.setTermBuffer("accents");
    tok.setPositionIncrement(3);
    Payload tokPayload = new Payload(new byte[]{0,1,2,3});
    tok.setPayload(tokPayload);
    int tokFlags = 77;
    tok.setFlags(tokFlags);

    SnowballFilter filter = new SnowballFilter(
        new TokenStream() {
          public Token next(final Token reusableToken) {
            assert reusableToken != null;
            return tok;
          }
        },
        "English"
    );

    final Token reusableToken = new Token();
    Token nextToken = filter.next(reusableToken);

    assertEquals("accent", nextToken.term());
    assertEquals(2, nextToken.startOffset());
    assertEquals(7, nextToken.endOffset());
    assertEquals("wrd", nextToken.type());
    assertEquals(3, nextToken.getPositionIncrement());
    assertEquals(tokFlags, nextToken.getFlags());
    assertEquals(tokPayload, nextToken.getPayload());
  }
}