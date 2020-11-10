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
package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 */
public class TestTrimFilter extends BaseTokenStreamTestCase {

  public void testTrim() throws Exception {
    char[] a = " a ".toCharArray();
    char[] b = "b   ".toCharArray();
    char[] ccc = "cCc".toCharArray();
    char[] whitespace = "   ".toCharArray();
    char[] empty = "".toCharArray();

    TokenStream ts = new CannedTokenStream(new Token(new String(a, 0, a.length), 1, 5),
                    new Token(new String(b, 0, b.length), 6, 10),
                    new Token(new String(ccc, 0, ccc.length), 11, 15),
                    new Token(new String(whitespace, 0, whitespace.length), 16, 20),
                    new Token(new String(empty, 0, empty.length), 21, 21));
    ts = new TrimFilter(ts);

    assertTokenStreamContents(ts, new String[] { "a", "b", "cCc", "", ""});
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
        return new TokenStreamComponents(tokenizer, new TrimFilter(tokenizer));
      } 
    };
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new TrimFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
