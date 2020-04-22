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


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.Random;

/**
 * Tests for {@link TestJapaneseReadingFormFilter}
 */
public class TestJapaneseReadingFormFilter extends BaseTokenStreamTestCase {
  private Analyzer katakanaAnalyzer, romajiAnalyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    katakanaAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, true, JapaneseTokenizer.Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, new JapaneseReadingFormFilter(tokenizer, false));
      }
    };
    romajiAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, true, JapaneseTokenizer.Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, new JapaneseReadingFormFilter(tokenizer, true));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(katakanaAnalyzer, romajiAnalyzer);
    super.tearDown();
  }

  public void testKatakanaReadings() throws IOException {
    assertAnalyzesTo(katakanaAnalyzer, "今夜はロバート先生と話した",
        new String[] { "コンヤ", "ハ", "ロバート", "センセイ", "ト", "ハナシ", "タ" }
    );
  }
  
  public void testKatakanaReadingsHalfWidth() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, true, JapaneseTokenizer.Mode.SEARCH);
        TokenStream stream = new CJKWidthFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, new JapaneseReadingFormFilter(stream, false));
      }
    };
    assertAnalyzesTo(a, "今夜はﾛﾊﾞｰﾄ先生と話した",
        new String[] { "コンヤ", "ハ", "ロバート", "センセイ", "ト", "ハナシ", "タ" }
    );
    a.close();
  }

  public void testRomajiReadings() throws IOException {
    assertAnalyzesTo(romajiAnalyzer, "今夜はロバート先生と話した",
        new String[] { "kon'ya", "ha", "robato", "sensei", "to", "hanashi", "ta" }
    );
  }
  
  public void testRomajiReadingsHalfWidth() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, true, JapaneseTokenizer.Mode.SEARCH);
        TokenStream stream = new CJKWidthFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, new JapaneseReadingFormFilter(stream, true));
      }
    };
    assertAnalyzesTo(a, "今夜はﾛﾊﾞｰﾄ先生と話した",
        new String[] { "kon'ya", "ha", "robato", "sensei", "to", "hanashi", "ta" }
    );
    a.close();
  }

  public void testRandomData() throws IOException {
    Random random = random();
    checkRandomData(random, katakanaAnalyzer, 200 * RANDOM_MULTIPLIER);
    checkRandomData(random, romajiAnalyzer, 200 * RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new JapaneseReadingFormFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
