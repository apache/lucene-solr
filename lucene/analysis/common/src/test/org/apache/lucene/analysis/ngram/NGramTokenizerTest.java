package org.apache.lucene.analysis.ngram;

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


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util._TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

/**
 * Tests {@link NGramTokenizer} for correctness.
 */
public class NGramTokenizerTest extends BaseTokenStreamTestCase {
  private StringReader input;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    input = new StringReader("abcde");
  }
  
  public void testInvalidInput() throws Exception {
    boolean gotException = false;
    try {        
      new NGramTokenizer(TEST_VERSION_CURRENT, input, 2, 1);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }
  
  public void testInvalidInput2() throws Exception {
    boolean gotException = false;
    try {        
      new NGramTokenizer(TEST_VERSION_CURRENT, input, 0, 1);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }
  
  public void testUnigrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(TEST_VERSION_CURRENT, input, 1, 1);
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
  }
  
  public void testBigrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(TEST_VERSION_CURRENT, input, 2, 2);
    assertTokenStreamContents(tokenizer, new String[]{"ab","bc","cd","de"}, new int[]{0,1,2,3}, new int[]{2,3,4,5}, 5 /* abcde */);
  }
  
  public void testNgrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(TEST_VERSION_CURRENT, input, 1, 3);
    assertTokenStreamContents(tokenizer,
        new String[]{"a","ab", "abc", "b", "bc", "bcd", "c", "cd", "cde", "d", "de", "e"},
        new int[]{0,0,0,1,1,1,2,2,2,3,3,4},
        new int[]{1,2,3,2,3,4,3,4,5,4,5,5},
        null,
        null,
        null,
        5 /* abcde */,
        false
        );
  }
  
  public void testOversizedNgrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(TEST_VERSION_CURRENT, input, 6, 7);
    assertTokenStreamContents(tokenizer, new String[0], new int[0], new int[0], 5 /* abcde */);
  }
  
  public void testReset() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(TEST_VERSION_CURRENT, input, 1, 1);
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new NGramTokenizer(TEST_VERSION_CURRENT, reader, 2, 4);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }    
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER, 20, false, false);
    checkRandomData(random(), a, 50*RANDOM_MULTIPLIER, 1027, false, false);
  }

  private void testNGrams(int minGram, int maxGram, int length) throws IOException {
    final String s = RandomStrings.randomAsciiOfLength(random(), length);
    final TokenStream grams = new NGramTokenizer(TEST_VERSION_CURRENT, new StringReader(s), minGram, maxGram);
    final CharTermAttribute termAtt = grams.addAttribute(CharTermAttribute.class);
    final PositionIncrementAttribute posIncAtt = grams.addAttribute(PositionIncrementAttribute.class);
    final PositionLengthAttribute posLenAtt = grams.addAttribute(PositionLengthAttribute.class);
    final OffsetAttribute offsetAtt = grams.addAttribute(OffsetAttribute.class);
    grams.reset();
    for (int start = 0; start < s.length(); ++start) {
      for (int end = start + minGram; end <= start + maxGram && end <= s.length(); ++end) {
        assertTrue(grams.incrementToken());
        assertEquals(s.substring(start, end), termAtt.toString());
        assertEquals(1, posIncAtt.getPositionIncrement());
        assertEquals(start, offsetAtt.startOffset());
        assertEquals(end, offsetAtt.endOffset());
      }
    }
    grams.end();
    assertEquals(s.length(), offsetAtt.startOffset());
    assertEquals(s.length(), offsetAtt.endOffset());
  }

  public void testLargeInput() throws IOException {
    // test sliding
    final int minGram = _TestUtil.nextInt(random(), 1, 100);
    final int maxGram = _TestUtil.nextInt(random(), minGram, 100);
    testNGrams(minGram, maxGram, _TestUtil.nextInt(random(), 3 * 1024, 4 * 1024));
  }

  public void testLargeMaxGram() throws IOException {
    // test sliding with maxGram > 1024
    final int minGram = _TestUtil.nextInt(random(), 1200, 1300);
    final int maxGram = _TestUtil.nextInt(random(), minGram, 1300);
    testNGrams(minGram, maxGram, _TestUtil.nextInt(random(), 3 * 1024, 4 * 1024));
  }

}
