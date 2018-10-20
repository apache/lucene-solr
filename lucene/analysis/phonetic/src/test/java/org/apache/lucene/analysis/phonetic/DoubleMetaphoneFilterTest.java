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
package org.apache.lucene.analysis.phonetic;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.TestUtil;

public class DoubleMetaphoneFilterTest extends BaseTokenStreamTestCase {

  public void testSize4FalseInject() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("international");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, false);
    assertTokenStreamContents(filter, new String[] { "ANTR" });
  }

  public void testSize4TrueInject() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("international");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, true);
    assertTokenStreamContents(filter, new String[] { "international", "ANTR" });
  }

  public void testAlternateInjectFalse() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("Kuczewski");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, false);
    assertTokenStreamContents(filter, new String[] { "KSSK", "KXFS" });
  }

  public void testSize8FalseInject() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("international");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, false);
    assertTokenStreamContents(filter, new String[] { "ANTRNXNL" });
  }

  public void testNonConvertableStringsWithInject() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("12345 #$%@#^%&");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, true);
    assertTokenStreamContents(filter, new String[] { "12345", "#$%@#^%&" });
  }

  public void testNonConvertableStringsWithoutInject() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("12345 #$%@#^%&");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, false);
    assertTokenStreamContents(filter, new String[] { "12345", "#$%@#^%&" });
    
    // should have something after the stream
    stream = whitespaceMockTokenizer("12345 #$%@#^%& hello");
    filter = new DoubleMetaphoneFilter(stream, 8, false);
    assertTokenStreamContents(filter, new String[] { "12345", "#$%@#^%&", "HL" });
  }

  public void testRandom() throws Exception {
    final int codeLen = TestUtil.nextInt(random(), 1, 8);
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new DoubleMetaphoneFilter(tokenizer, codeLen, false));
      }
      
    };
    checkRandomData(random(), a, 1000 * RANDOM_MULTIPLIER);
    a.close();
    
    Analyzer b = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new DoubleMetaphoneFilter(tokenizer, codeLen, true));
      }
      
    };
    checkRandomData(random(), b, 1000 * RANDOM_MULTIPLIER); 
    b.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new DoubleMetaphoneFilter(tokenizer, 8, random().nextBoolean()));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
