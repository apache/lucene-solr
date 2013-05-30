package org.apache.lucene.analysis.en;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import static org.apache.lucene.analysis.VocabularyAssert.*;

/**
 * Test the PorterStemFilter with Martin Porter's test data.
 */
public class TestPorterStemFilter extends BaseTokenStreamTestCase {
  Analyzer a = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName,
        Reader reader) {
      Tokenizer t = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
      return new TokenStreamComponents(t, new PorterStemFilter(t));
    }
  };
  
  /**
   * Run the stemmer against all strings in voc.txt
   * The output should be the same as the string in output.txt
   */
  public void testPorterStemFilter() throws Exception {
    assertVocabulary(a, getDataFile("porterTestData.zip"), "voc.txt", "output.txt");
  }
  
  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
    set.add("yourselves");
    Tokenizer tokenizer = new MockTokenizer(new StringReader("yourselves yours"), MockTokenizer.WHITESPACE, false);
    TokenStream filter = new PorterStemFilter(new SetKeywordMarkerFilter(tokenizer, set));   
    assertTokenStreamContents(filter, new String[] {"yourselves", "your"});
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new PorterStemFilter(tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
