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
import org.apache.lucene.analysis.WhitespaceTokenizer;

import java.io.StringReader;
import java.util.regex.Pattern;

/**
 * @version $Id:$
 */
public class TestPatternReplaceFilter extends AnalysisTestCase {

  public void testReplaceAll() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (new WhitespaceTokenizer(new StringReader(input)),
                    Pattern.compile("a*b"),
                    "-", true);
    Token token = ts.next();
    assertEquals("-foo-foo-foo-", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("-", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("c-", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertNull(token);
  }

  public void testReplaceFirst() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (new WhitespaceTokenizer(new StringReader(input)),
                    Pattern.compile("a*b"),
                    "-", false);
    Token token = ts.next();
    assertEquals("-fooaabfooabfoob", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("-", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("c-", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertNull(token);
  }

  public void testStripFirst() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (new WhitespaceTokenizer(new StringReader(input)),
                    Pattern.compile("a*b"),
                    null, false);
    Token token = ts.next();
    assertEquals("fooaabfooabfoob", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("c", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertNull(token);
  }

  public void testStripAll() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (new WhitespaceTokenizer(new StringReader(input)),
                    Pattern.compile("a*b"),
                    null, true);
    Token token = ts.next();
    assertEquals("foofoofoo", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("c", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertNull(token);
  }

  public void testReplaceAllWithBackRef() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (new WhitespaceTokenizer(new StringReader(input)),
                    Pattern.compile("(a*)b"),
                    "$1\\$", true);
    Token token = ts.next();
    assertEquals("aa$fooaa$fooa$foo$", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("a$", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertEquals("caaaaaaaaa$", new String(token.termBuffer(), 0, token.termLength()));
    token = ts.next();
    assertNull(token);
  }

}
