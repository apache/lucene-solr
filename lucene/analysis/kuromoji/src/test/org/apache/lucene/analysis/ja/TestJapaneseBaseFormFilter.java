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
package org.apache.lucene.analysis.ja;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;

public class TestJapaneseBaseFormFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, true, JapaneseTokenizer.DEFAULT_MODE);
        return new TokenStreamComponents(tokenizer, new JapaneseBaseFormFilter(tokenizer));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }
  
  public void testBasics() throws IOException {
    assertAnalyzesTo(analyzer, "それはまだ実験段階にあります",
        new String[] { "それ", "は", "まだ", "実験", "段階", "に", "ある", "ます"  }
    );
  }
  
  public void testKeyword() throws IOException {
    final CharArraySet exclusionSet = new CharArraySet(asSet("あり"), false);
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new JapaneseTokenizer(newAttributeFactory(), null, true, JapaneseTokenizer.DEFAULT_MODE);
        TokenStream sink = new SetKeywordMarkerFilter(source, exclusionSet);
        return new TokenStreamComponents(source, new JapaneseBaseFormFilter(sink));
      }
    };
    assertAnalyzesTo(a, "それはまだ実験段階にあります",
        new String[] { "それ", "は", "まだ", "実験", "段階", "に", "あり", "ます"  }
    );
    a.close();
  }
  
  public void testEnglish() throws IOException {
    assertAnalyzesTo(analyzer, "this atest",
        new String[] { "this", "atest" });
  }
  
  public void testRandomStrings() throws IOException {
    checkRandomData(random(), analyzer, atLeast(1000));
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new JapaneseBaseFormFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
