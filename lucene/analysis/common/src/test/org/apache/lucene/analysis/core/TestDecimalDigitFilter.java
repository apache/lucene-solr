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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.TestUtil;

import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests for {@link DecimalDigitFilter}
 */
public class TestDecimalDigitFilter extends BaseTokenStreamTestCase {
  private Analyzer tokenized;
  private Analyzer keyword;

  private static SparseFixedBitSet DECIMAL_DIGIT_CODEPOINTS;

  @BeforeClass
  public static void init_DECIMAL_DIGIT_CODEPOINTS() {
    DECIMAL_DIGIT_CODEPOINTS = new SparseFixedBitSet(Character.MAX_CODE_POINT);
    for (int codepoint = Character.MIN_CODE_POINT; codepoint < Character.MAX_CODE_POINT; codepoint++) {
      if (Character.isDigit(codepoint)) {
        DECIMAL_DIGIT_CODEPOINTS.set(codepoint);
      }
    }
    assert 0 < DECIMAL_DIGIT_CODEPOINTS.cardinality();
  }
  
  @AfterClass
  public static void destroy_DECIMAL_DIGIT_CODEPOINTS() {
    DECIMAL_DIGIT_CODEPOINTS = null;
  }

  
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
   * test that double struck digits are normalized
   */
  public void testDoubleStruck() throws Exception {
    // MATHEMATICAL DOUBLE-STRUCK DIGIT ... 1, 9, 8, 4
    final String input = "𝟙 𝟡 𝟠 𝟜";
    final String expected = "1 9 8 4";
    checkOneTerm(keyword, input, expected);
    checkOneTerm(keyword, input.replaceAll("\\s",""), expected.replaceAll("\\s",""));
  }

  /**
   * test sequences of digits mixed with other random simple string data
   */
  public void testRandomSequences() throws Exception {
    
    // test numIters random strings containing a sequence of numDigits codepoints
    final int numIters = atLeast(5);
    for (int iter = 0; iter < numIters; iter++) {
      final int numDigits = atLeast(20);
      final StringBuilder expected = new StringBuilder();
      final StringBuilder actual = new StringBuilder();
      for (int digitCounter = 0; digitCounter < numDigits; digitCounter++) {
        
        // increased odds of 0 length random string prefix
        final String prefix = random().nextBoolean() ? "" : TestUtil.randomSimpleString(random());
        expected.append(prefix);
        actual.append(prefix);
        
        int codepoint = getRandomDecimalDigit(random());

        int value = Character.getNumericValue(codepoint);
        assert value >= 0 && value <= 9;
        expected.append(Integer.toString(value));
        actual.appendCodePoint(codepoint);
      }
      // occasional suffix, increased odds of 0 length random string
      final String suffix = random().nextBoolean() ? "" : TestUtil.randomSimpleString(random());
      expected.append(suffix);
      actual.append(suffix);
      
      checkOneTerm(keyword, actual.toString(), expected.toString());
    }

  }
  
  /**
   * test each individual digit in different locations of strings.
   */
  public void testRandom() throws Exception {
    int numCodePointsChecked = 0; // sanity check
    for (int codepoint = DECIMAL_DIGIT_CODEPOINTS.nextSetBit(0);
         codepoint != DocIdSetIterator.NO_MORE_DOCS;
         codepoint = DECIMAL_DIGIT_CODEPOINTS.nextSetBit(codepoint+1)) {
      
      assert Character.isDigit(codepoint);
      
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
      
      numCodePointsChecked++;
    }
    assert DECIMAL_DIGIT_CODEPOINTS.cardinality() == numCodePointsChecked;
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
    checkRandomData(random(), tokenized, 200 * RANDOM_MULTIPLIER);
  }

  /** returns a psuedo-random codepoint which is a Decimal Digit */
  public static int getRandomDecimalDigit(Random r) {
    final int aprox = TestUtil.nextInt(r, 0, DECIMAL_DIGIT_CODEPOINTS.length()-1);
    
    if (DECIMAL_DIGIT_CODEPOINTS.get(aprox)) { // lucky guess
      assert Character.isDigit(aprox);
      return aprox;
    }
    
    // seek up and down for closest set bit
    final int lower = DECIMAL_DIGIT_CODEPOINTS.prevSetBit(aprox);
    final int higher = DECIMAL_DIGIT_CODEPOINTS.nextSetBit(aprox);
    
    // sanity check edge cases
    if (lower < 0) {
      assert higher != DocIdSetIterator.NO_MORE_DOCS;
      assert Character.isDigit(higher);
      return higher;
    }
    if (higher == DocIdSetIterator.NO_MORE_DOCS) {
      assert 0 <= lower;
      assert Character.isDigit(lower);
      return lower;
    }
    
    // which is closer?
    final int cmp = Integer.compare(aprox - lower, higher - aprox);
    
    if (0 == cmp) {
      // dead even, flip a coin
      final int result = random().nextBoolean() ? lower : higher;
      assert Character.isDigit(result);
      return result;
    }
    
    final int result = (cmp < 0) ? lower : higher;
    assert Character.isDigit(result);
    return result;
  }
}
