/*
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
package org.apache.lucene.analysis.pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

import java.io.IOException;
import java.util.regex.Pattern;

public class TestPatternReplaceFilter extends BaseTokenStreamTestCase {
  
  public void testReplaceAll() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (whitespaceMockTokenizer(input),
                    Pattern.compile("a*b"),
                    "-", true);
    assertTokenStreamContents(ts, 
        new String[] { "-foo-foo-foo-", "-", "c-" });
  }

  public void testReplaceFirst() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (whitespaceMockTokenizer(input),
                    Pattern.compile("a*b"),
                    "-", false);
    assertTokenStreamContents(ts, 
        new String[] { "-fooaabfooabfoob", "-", "c-" });
  }

  public void testStripFirst() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (whitespaceMockTokenizer(input),
                    Pattern.compile("a*b"),
                    null, false);
    assertTokenStreamContents(ts,
        new String[] { "fooaabfooabfoob", "", "c" });
  }

  public void testStripAll() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (whitespaceMockTokenizer(input),
                    Pattern.compile("a*b"),
                    null, true);
    assertTokenStreamContents(ts,
        new String[] { "foofoofoo", "", "c" });
  }

  public void testReplaceAllWithBackRef() throws Exception {
    String input = "aabfooaabfooabfoob ab caaaaaaaaab";
    TokenStream ts = new PatternReplaceFilter
            (whitespaceMockTokenizer(input),
                    Pattern.compile("(a*)b"),
                    "$1\\$", true);
    assertTokenStreamContents(ts,
        new String[] { "aa$fooaa$fooa$foo$", "a$", "caaaaaaaaa$" });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TokenStream filter = new PatternReplaceFilter(tokenizer, Pattern.compile("a"), "b", false);
        return new TokenStreamComponents(tokenizer, filter);
      }    
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
    
    Analyzer b = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TokenStream filter = new PatternReplaceFilter(tokenizer, Pattern.compile("a"), "b", true);
        return new TokenStreamComponents(tokenizer, filter);
      }    
    };
    checkRandomData(random(), b, 1000*RANDOM_MULTIPLIER);
    b.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer,  new PatternReplaceFilter(tokenizer, Pattern.compile("a"), "b", true));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }

}
