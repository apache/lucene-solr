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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util._TestUtil;

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
      new EdgeNGramTokenizer(TEST_VERSION_CURRENT, input, 0, 0);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testInvalidInput2() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenizer(TEST_VERSION_CURRENT, input, 2, 1);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testInvalidInput3() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenizer(TEST_VERSION_CURRENT, input, -1, 2);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testFrontUnigram() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(TEST_VERSION_CURRENT, input, 1, 1);
    assertTokenStreamContents(tokenizer, new String[]{"a"}, new int[]{0}, new int[]{1}, 5 /* abcde */);
  }

  public void testOversizedNgrams() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(TEST_VERSION_CURRENT, input, 6, 6);
    assertTokenStreamContents(tokenizer, new String[0], new int[0], new int[0], 5 /* abcde */);
  }

  public void testFrontRangeOfNgrams() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(TEST_VERSION_CURRENT, input, 1, 3);
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3}, 5 /* abcde */);
  }
  
  public void testReset() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(TEST_VERSION_CURRENT, input, 1, 3);
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3}, 5 /* abcde */);
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3}, 5 /* abcde */);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new EdgeNGramTokenizer(TEST_VERSION_CURRENT, reader, 2, 4);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }    
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER, 20, false, false);
    checkRandomData(random(), a, 100*RANDOM_MULTIPLIER, 8192, false, false);
  }

  public void testTokenizerPositions() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(TEST_VERSION_CURRENT, new StringReader("abcde"), 1, 3);
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

  public void testLargeInput() throws IOException {
    final String input = _TestUtil.randomSimpleString(random(), 1024 * 5);
    final int minGram = _TestUtil.nextInt(random(), 1, 1024);
    final int maxGram = _TestUtil.nextInt(random(), minGram, 5 * 1024);
    EdgeNGramTokenizer tk = new EdgeNGramTokenizer(TEST_VERSION_CURRENT, new StringReader(input), minGram, maxGram);
    final CharTermAttribute charTermAtt = tk.addAttribute(CharTermAttribute.class);
    final OffsetAttribute offsetAtt = tk.addAttribute(OffsetAttribute.class);
    final PositionIncrementAttribute posIncAtt = tk.addAttribute(PositionIncrementAttribute.class);
    tk.reset();
    for (int i = minGram; i <= maxGram && i <= input.length(); ++i) {
      assertTrue(tk.incrementToken());
      assertEquals(0, offsetAtt.startOffset());
      assertEquals(i, offsetAtt.endOffset());
      assertEquals(1, posIncAtt.getPositionIncrement());
      assertEquals(input.substring(0, i), charTermAtt.toString());
    }
    assertFalse(tk.incrementToken());
    tk.end();
    assertEquals(input.length(), offsetAtt.startOffset());
  }

}
