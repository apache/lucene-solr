package org.apache.lucene.analysis.hunspell;
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
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HunspellStemFilterTest  extends BaseTokenStreamTestCase {
  
  private static HunspellDictionary DICTIONARY;
  @BeforeClass
  public static void beforeClass() throws IOException, ParseException {
    DICTIONARY = createDict(true);
  }
  @AfterClass
  public static void afterClass() {
    DICTIONARY = null;
  }
  public static HunspellDictionary createDict(boolean ignoreCase) throws IOException, ParseException {
    InputStream affixStream = HunspellStemmerTest.class.getResourceAsStream("test.aff");
    InputStream dictStream = HunspellStemmerTest.class.getResourceAsStream("test.dic");

    return new HunspellDictionary(affixStream, dictStream, TEST_VERSION_CURRENT, ignoreCase);
  }
  
  /**
   * Simple test for KeywordAttribute
   */
  public void testKeywordAttribute() throws IOException {
    MockTokenizer tokenizer = new MockTokenizer(new StringReader("lucene is awesome"), MockTokenizer.WHITESPACE, true);
    tokenizer.setEnableChecks(true);
    HunspellStemFilter filter = new HunspellStemFilter(tokenizer, DICTIONARY);
    assertTokenStreamContents(filter, new String[]{"lucene", "lucen", "is", "awesome"}, new int[] {1, 0, 1, 1});
    
    // assert with keywork marker
    tokenizer = new MockTokenizer(new StringReader("lucene is awesome"), MockTokenizer.WHITESPACE, true);
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, Arrays.asList("Lucene"), true);
    filter = new HunspellStemFilter(new SetKeywordMarkerFilter(tokenizer, set), DICTIONARY);
    assertTokenStreamContents(filter, new String[]{"lucene", "is", "awesome"}, new int[] {1, 1, 1});
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new HunspellStemFilter(tokenizer, DICTIONARY));
      }  
    };
    checkRandomData(random(), analyzer, 1000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new HunspellStemFilter(tokenizer, DICTIONARY));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
