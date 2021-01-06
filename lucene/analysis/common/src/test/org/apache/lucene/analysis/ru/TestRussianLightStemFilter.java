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
package org.apache.lucene.analysis.ru;

import static org.apache.lucene.analysis.VocabularyAssert.*;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;

/** Simple tests for {@link RussianLightStemFilter} */
public class TestRussianLightStemFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(source, new RussianLightStemFilter(source));
          }
        };
  }

  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }

  /** Test against a vocabulary from the reference impl */
  public void testVocabulary() throws IOException {
    assertVocabulary(analyzer, getDataPath("rulighttestdata.zip"), "rulight.txt");
  }

  public void testKeyword() throws IOException {
    final CharArraySet exclusionSet = new CharArraySet(asSet("энергии"), false);
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            TokenStream sink = new SetKeywordMarkerFilter(source, exclusionSet);
            return new TokenStreamComponents(source, new RussianLightStemFilter(sink));
          }
        };
    checkOneTerm(a, "энергии", "энергии");
    a.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new RussianLightStemFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }
}
