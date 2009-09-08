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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.util.List;


/**
 * @version $Id:$
 */
public class TestTrimFilter extends BaseTokenTestCase {

  public void testTrim() throws Exception {
    char[] a = " a ".toCharArray();
    char[] b = "b   ".toCharArray();
    char[] ccc = "cCc".toCharArray();
    char[] whitespace = "   ".toCharArray();
    char[] empty = "".toCharArray();
    TokenStream ts = new TrimFilter
            (new IterTokenStream(new Token(a, 0, a.length, 1, 5),
                    new Token(b, 0, b.length, 6, 10),
                    new Token(ccc, 0, ccc.length, 11, 15),
                    new Token(whitespace, 0, whitespace.length, 16, 20),
                    new Token(empty, 0, empty.length, 21, 21)), false);

    TermAttribute token;
    assertTrue(ts.incrementToken());
    token = (TermAttribute) ts.getAttribute(TermAttribute.class);
    assertEquals("a", new String(token.termBuffer(), 0, token.termLength()));
    assertTrue(ts.incrementToken());
    assertEquals("b", new String(token.termBuffer(), 0, token.termLength()));
    assertTrue(ts.incrementToken());
    assertEquals("cCc", new String(token.termBuffer(), 0, token.termLength()));
    assertTrue(ts.incrementToken());
    assertEquals("", new String(token.termBuffer(), 0, token.termLength()));
    assertTrue(ts.incrementToken());
    assertEquals("", new String(token.termBuffer(), 0, token.termLength()));
    assertFalse(ts.incrementToken());

    a = " a".toCharArray();
    b = "b ".toCharArray();
    ccc = " c ".toCharArray();
    whitespace = "   ".toCharArray();
    ts = new TrimFilter(new IterTokenStream(
            new Token(a, 0, a.length, 0, 2),
            new Token(b, 0, b.length, 0, 2),
            new Token(ccc, 0, ccc.length, 0, 3),
            new Token(whitespace, 0, whitespace.length, 0, 3)), true);
    
    List<Token> expect = tokens("a,1,1,2 b,1,0,1 c,1,1,2 ,1,3,3");
    List<Token> real = getTokens(ts);
    for (Token t : expect) {
      System.out.println("TEST:" + t);
    }
    for (Token t : real) {
      System.out.println("REAL:" + t);
    }
    assertTokEqualOff(expect, real);
  }

}
