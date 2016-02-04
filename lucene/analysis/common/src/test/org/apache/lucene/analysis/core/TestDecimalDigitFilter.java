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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.TestUtil;

/**
 * Tests for {@link DecimalDigitFilter}
 */
public class TestDecimalDigitFilter extends BaseTokenStreamTestCase {
  private Analyzer tokenized;
  private Analyzer keyword;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    tokenized = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new DecimalDigitFilter(tokenizer));
      }
    };
    keyword = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new DecimalDigitFilter(tokenizer));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    tokenized.close();
    keyword.close();
    super.tearDown();
  }

  /**
   * test that digits are normalized
   */
  public void testSimple() throws Exception {
    checkOneTerm(tokenized, "١٢٣٤", "1234");
  }
  
  /**
   * test all digits in different locations of strings.
   */
  public void testRandom() throws Exception {
    for (int codepoint = Character.MIN_CODE_POINT; codepoint < Character.MAX_CODE_POINT; codepoint++) {
      if (Character.isDigit(codepoint)) {
        // add some a-z before/after the string
        String prefix = TestUtil.randomSimpleString(random());
        String suffix = TestUtil.randomSimpleString(random());
        
        StringBuilder expected = new StringBuilder();
        expected.append(prefix);
        int value = Character.getNumericValue(codepoint);
        assert value >= 0 && value <= 9;
        expected.append(Integer.toString(value));
        expected.append(suffix);
        
        StringBuilder actual = new StringBuilder();
        actual.append(prefix);
        actual.appendCodePoint(codepoint);
        actual.append(suffix);
        
        checkOneTerm(keyword, actual.toString(), expected.toString());
      }
    }
  }
  
  /**
   * check the filter is a no-op for the empty string term
   */
  public void testEmptyTerm() throws Exception {
    checkOneTerm(keyword, "", "");
  }
  
  /** 
   * blast some random strings through the filter
   */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), tokenized, 1000*RANDOM_MULTIPLIER);
  }
}
