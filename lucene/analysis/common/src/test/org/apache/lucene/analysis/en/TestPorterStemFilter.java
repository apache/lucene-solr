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
package org.apache.lucene.analysis.en;


import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;

import static org.apache.lucene.analysis.VocabularyAssert.*;

/**
 * Test the PorterStemFilter with Martin Porter's test data.
 */
public class TestPorterStemFilter extends BaseTokenStreamTestCase {
  private Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer t = new MockTokenizer( MockTokenizer.KEYWORD, false);
        return new TokenStreamComponents(t, new PorterStemFilter(t));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }
  
  /**
   * Run the stemmer against all strings in voc.txt
   * The output should be the same as the string in output.txt
   */
  public void testPorterStemFilter() throws Exception {
    assertVocabulary(a, getDataPath("porterTestData.zip"), "voc.txt", "output.txt");
  }
  
  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet( 1, true);
    set.add("yourselves");
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(new StringReader("yourselves yours"));
    TokenStream filter = new PorterStemFilter(new SetKeywordMarkerFilter(tokenizer, set));   
    assertTokenStreamContents(filter, new String[] {"yourselves", "your"});
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new PorterStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
