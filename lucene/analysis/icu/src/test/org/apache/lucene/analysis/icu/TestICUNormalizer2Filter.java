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
package org.apache.lucene.analysis.icu;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

import com.ibm.icu.text.Normalizer2;

/**
 * Tests the ICUNormalizer2Filter
 */
public class TestICUNormalizer2Filter extends BaseTokenStreamTestCase {
  Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new ICUNormalizer2Filter(tokenizer));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }

  public void testDefaults() throws IOException {
    // case folding
    assertAnalyzesTo(a, "This is a test", new String[] { "this", "is", "a", "test" });

    // case folding
    assertAnalyzesTo(a, "Ruß", new String[] { "russ" });
    
    // case folding
    assertAnalyzesTo(a, "ΜΆΪΟΣ", new String[] { "μάϊοσ" });
    assertAnalyzesTo(a, "Μάϊος", new String[] { "μάϊοσ" });

    // supplementary case folding
    assertAnalyzesTo(a, "𐐖", new String[] { "𐐾" });
    
    // normalization
    assertAnalyzesTo(a, "ﴳﴺﰧ", new String[] { "طمطمطم" });

    // removal of default ignorables
    assertAnalyzesTo(a, "क्‍ष", new String[] { "क्ष" });
  }
  
  public void testAlternate() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new ICUNormalizer2Filter(
            tokenizer,
            /* specify nfc with decompose to get nfd */
            Normalizer2.getInstance(null, "nfc", Normalizer2.Mode.DECOMPOSE)));
      }
    };
    
    // decompose EAcute into E + combining Acute
    assertAnalyzesTo(a, "\u00E9", new String[] { "\u0065\u0301" });
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new ICUNormalizer2Filter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
