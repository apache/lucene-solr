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
package org.apache.lucene.analysis.ngram;



import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

/**
 * Tests {@link EdgeNGramTokenizer} for correctness.
 */
public class EdgeNGramTokenizerTest extends BaseTokenStreamTestCase {
  private StringReader input;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    input = new StringReader("abcde");
  }

  public void testInvalidInput() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenizer(0, 0).setReader(input);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testInvalidInput2() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenizer(2, 1).setReader(input);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testInvalidInput3() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenizer(-1, 2).setReader(input);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testFrontUnigram() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(1, 1);
    tokenizer.setReader(input);
    assertTokenStreamContents(tokenizer, new String[]{"a"}, new int[]{0}, new int[]{1}, 5 /* abcde */);
  }

  public void testOversizedNgrams() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(6, 6);
    tokenizer.setReader(input);;
    assertTokenStreamContents(tokenizer, new String[0], new int[0], new int[0], 5 /* abcde */);
  }

  public void testFrontRangeOfNgrams() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(1, 3);
    tokenizer.setReader(input);
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3}, 5 /* abcde */);
  }
  
  public void testReset() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(1, 3);
    tokenizer.setReader(input);
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3}, 5 /* abcde */);
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3}, 5 /* abcde */);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    int numIters = TEST_NIGHTLY ? 10 : 1;
    for (int i = 0; i < numIters; i++) {
      final int min = TestUtil.nextInt(random(), 2, 10);
      final int max = TestUtil.nextInt(random(), min, 20);
      
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new EdgeNGramTokenizer(min, max);
          return new TokenStreamComponents(tokenizer, tokenizer);
        }    
      };
      checkRandomData(random(), a, 100*RANDOM_MULTIPLIER, 20);
      checkRandomData(random(), a, 10*RANDOM_MULTIPLIER, 8192);
      a.close();
    }
  }

  public void testTokenizerPositions() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(1, 3);
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(tokenizer,
                              new String[]{"a","ab","abc"},
                              new int[]{0,0,0},
                              new int[]{1,2,3},
                              null,
                              new int[]{1,1,1},
                              null,
                              null,
                              false);
  }

  private static void testNGrams(int minGram, int maxGram, int length, final String nonTokenChars) throws IOException {
    final String s = RandomStrings.randomAsciiOfLength(random(), length);
    testNGrams(minGram, maxGram, s, nonTokenChars);
  }

  private static void testNGrams(int minGram, int maxGram, String s, String nonTokenChars) throws IOException {
    NGramTokenizerTest.testNGrams(minGram, maxGram, s, nonTokenChars, true);
  }

  public void testLargeInput() throws IOException {
    // test sliding
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 3 * 1024, 4 * 1024), "");
  }

  public void testLargeMaxGram() throws IOException {
    // test sliding with maxGram > 1024
    final int minGram = TestUtil.nextInt(random(), 1290, 1300);
    final int maxGram = TestUtil.nextInt(random(), minGram, 1300);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 3 * 1024, 4 * 1024), "");
  }

  public void testPreTokenization() throws IOException {
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 0, 4 * 1024), "a");
  }

  public void testHeavyPreTokenization() throws IOException {
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 0, 4 * 1024), "abcdef");
  }

  public void testFewTokenChars() throws IOException {
    final char[] chrs = new char[TestUtil.nextInt(random(), 4000, 5000)];
    Arrays.fill(chrs, ' ');
    for (int i = 0; i < chrs.length; ++i) {
      if (random().nextFloat() < 0.1) {
        chrs[i] = 'a';
      }
    }
    final int minGram = TestUtil.nextInt(random(), 1, 2);
    final int maxGram = TestUtil.nextInt(random(), minGram, 2);
    testNGrams(minGram, maxGram, new String(chrs), " ");
  }

  public void testFullUTF8Range() throws IOException {
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    final String s = TestUtil.randomUnicodeString(random(), 4 * 1024);
    testNGrams(minGram, maxGram, s, "");
    testNGrams(minGram, maxGram, s, "abcdef");
  }

}
