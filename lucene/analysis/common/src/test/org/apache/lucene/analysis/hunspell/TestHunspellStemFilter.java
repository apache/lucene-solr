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
package org.apache.lucene.analysis.hunspell;


import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestHunspellStemFilter extends BaseTokenStreamTestCase {
  private static Dictionary dictionary;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // no multiple try-with to workaround bogus VerifyError
    InputStream affixStream = TestStemmer.class.getResourceAsStream("simple.aff");
    InputStream dictStream = TestStemmer.class.getResourceAsStream("simple.dic");
    Directory tempDir = getDirectory();
    
    try {
      dictionary = new Dictionary(tempDir, "dictionary", affixStream, dictStream);
    } finally {
      IOUtils.closeWhileHandlingException(affixStream, dictStream);
    }
    tempDir.close();
  }
  
  @AfterClass
  public static void afterClass() {
    dictionary = null;
  }
  
  /** Simple test for KeywordAttribute */
  public void testKeywordAttribute() throws IOException {
    MockTokenizer tokenizer = whitespaceMockTokenizer("lucene is awesome");
    tokenizer.setEnableChecks(true);
    HunspellStemFilter filter = new HunspellStemFilter(tokenizer, dictionary);
    assertTokenStreamContents(filter, new String[]{"lucene", "lucen", "is", "awesome"}, new int[] {1, 0, 1, 1});
    
    // assert with keyword marker
    tokenizer = whitespaceMockTokenizer("lucene is awesome");
    CharArraySet set = new CharArraySet( Arrays.asList("Lucene"), true);
    filter = new HunspellStemFilter(new SetKeywordMarkerFilter(tokenizer, set), dictionary);
    assertTokenStreamContents(filter, new String[]{"lucene", "is", "awesome"}, new int[] {1, 1, 1});
  }
  
  /** simple test for longestOnly option */
  public void testLongestOnly() throws IOException {
    MockTokenizer tokenizer = whitespaceMockTokenizer("lucene is awesome");
    tokenizer.setEnableChecks(true);
    HunspellStemFilter filter = new HunspellStemFilter(tokenizer, dictionary, true, true);
    assertTokenStreamContents(filter, new String[]{"lucene", "is", "awesome"}, new int[] {1, 1, 1});
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new HunspellStemFilter(tokenizer, dictionary));
      }  
    };
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new HunspellStemFilter(tokenizer, dictionary));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
  
  public void testIgnoreCaseNoSideEffects() throws Exception {
    final Dictionary d;
    // no multiple try-with to workaround bogus VerifyError
    InputStream affixStream = TestStemmer.class.getResourceAsStream("simple.aff");
    InputStream dictStream = TestStemmer.class.getResourceAsStream("simple.dic");
    Directory tempDir = getDirectory();
    try {
      d = new Dictionary(tempDir, "dictionary", affixStream, Collections.singletonList(dictStream), true);
    } finally {
      IOUtils.closeWhileHandlingException(affixStream, dictStream);
    }
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new HunspellStemFilter(tokenizer, d));
      }
    };
    checkOneTerm(a, "NoChAnGy", "NoChAnGy");
    a.close();
    tempDir.close();
  }

  private static Directory getDirectory() {
    return newDirectory();
  }
}
