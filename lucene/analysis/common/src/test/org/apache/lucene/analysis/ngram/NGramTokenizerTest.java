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


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.LuceneTestCase.Slow;

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
      new NGramTokenizer(input, 2, 1);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }
  
  public void testInvalidInput2() throws Exception {
    boolean gotException = false;
    try {        
      new NGramTokenizer(input, 0, 1);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }
  
  public void testUnigrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(input, 1, 1);
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
  }
  
  public void testBigrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(input, 2, 2);
    assertTokenStreamContents(tokenizer, new String[]{"ab","bc","cd","de"}, new int[]{0,1,2,3}, new int[]{2,3,4,5}, 5 /* abcde */);
  }
  
  public void testNgrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(input, 1, 3);
    assertTokenStreamContents(tokenizer,
        new String[]{"a","b","c","d","e", "ab","bc","cd","de", "abc","bcd","cde"}, 
        new int[]{0,1,2,3,4, 0,1,2,3, 0,1,2},
        new int[]{1,2,3,4,5, 2,3,4,5, 3,4,5},
        null,
        null,
        null,
        5 /* abcde */,
        false
        );
  }
  
  public void testOversizedNgrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(input, 6, 7);
    assertTokenStreamContents(tokenizer, new String[0], new int[0], new int[0], 5 /* abcde */);
  }
  
  public void testReset() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(input, 1, 1);
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new NGramTokenizer(reader, 2, 4);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }    
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER, 20, false, false);
    checkRandomData(random(), a, 50*RANDOM_MULTIPLIER, 1027, false, false);
  }
}
