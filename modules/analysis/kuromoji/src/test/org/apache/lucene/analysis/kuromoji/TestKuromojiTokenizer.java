package org.apache.lucene.analysis.kuromoji;

/**
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
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;

public class TestKuromojiTokenizer extends BaseTokenStreamTestCase {
  private Analyzer analyzer = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new KuromojiTokenizer(reader);
      return new TokenStreamComponents(tokenizer, tokenizer);
    }
  };
  
  public void testDecomposition1() throws Exception {
    assertAnalyzesTo(analyzer, "本来は、貧困層の女性や子供に医療保護を提供するために創設された制度である、" +
                         "アメリカ低所得者医療援助制度が、今日では、その予算の約３分の１を老人に費やしている。",
     new String[] { "本来", "は",  "貧困", "層", "の", "女性", "や", "子供", "に", "医療", "保護", "を",      
                    "提供", "する", "ため", "に", "創設", "さ", "れ", "た", "制度", "で", "ある",  "アメリカ", 
                    "低", "所得", "者", "医療", "援助", "制度", "が",  "今日", "で", "は",  "その",
                    "予算", "の", "約", "３", "分の", "１", "を", "老人", "に", "費やし", "て", "いる" },
     new int[] { 0, 2, 4, 6, 7,  8, 10, 11, 13, 14, 16, 18, 19, 21, 23, 25, 26, 28, 29, 30, 
                 31, 33, 34, 37, 41, 42, 44, 45, 47, 49, 51, 53, 55, 56, 58, 60,
                 62, 63, 64, 65, 67, 68, 69, 71, 72, 75, 76 },
     new int[] { 2, 3, 6, 7, 8, 10, 11, 13, 14, 16, 18, 19, 21, 23, 25, 26, 28, 29, 30, 31,
                 33, 34, 36, 41, 42, 44, 45, 47, 49, 51, 52, 55, 56, 57, 60, 62,
                 63, 64, 65, 67, 68, 69, 71, 72, 75, 76, 78 }
    );
  }
  
  public void testDecomposition2() throws Exception {
    assertAnalyzesTo(analyzer, "麻薬の密売は根こそぎ絶やさなければならない",
      new String[] { "麻薬", "の", "密売", "は", "根こそぎ", "絶やさ", "なけれ", "ば", "なら", "ない" },
      new int[] { 0, 2, 3, 5, 6,  10, 13, 16, 17, 19 },
      new int[] { 2, 3, 5, 6, 10, 13, 16, 17, 19, 21 }
    );
  }
  
  public void testDecomposition3() throws Exception {
    assertAnalyzesTo(analyzer, "魔女狩大将マシュー・ホプキンス。",
      new String[] { "魔女", "狩", "大将", "マシュー",  "ホプキンス" },
      new int[] { 0, 2, 3, 5, 10 },
      new int[] { 2, 3, 5, 9, 15 }
    );
  }

  public void testDecomposition4() throws Exception {
    assertAnalyzesTo(analyzer, "これは本ではない",
      new String[] { "これ", "は", "本", "で", "は", "ない" },
      new int[] { 0, 2, 3, 4, 5, 6 },
      new int[] { 2, 3, 4, 5, 6, 8 }
    );
  }

  /* Note this is really a stupid test just to see if things arent horribly slow.
   * ideally the test would actually fail instead of hanging...
   */
  public void testDecomposition5() throws Exception {
    TokenStream ts = analyzer.tokenStream("bogus", new StringReader("くよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよ"));
    ts.reset();
    while (ts.incrementToken()) {
      
    }
    ts.end();
    ts.close();
  }

  /** Tests that sentence offset is incorporated into the resulting offsets */
  public void testTwoSentences() throws Exception {
    assertAnalyzesTo(analyzer, "魔女狩大将マシュー・ホプキンス。 魔女狩大将マシュー・ホプキンス。",
      new String[] { "魔女", "狩", "大将", "マシュー", "ホプキンス",  "魔女", "狩", "大将", "マシュー",  "ホプキンス"  },
      new int[] { 0, 2, 3, 5, 10, 17, 19, 20, 22, 27 },
      new int[] { 2, 3, 5, 9, 15, 19, 20, 22, 26, 32 }
    );
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random, analyzer, 10000*RANDOM_MULTIPLIER);
  }
  
  public void testLargeDocReliability() throws Exception {
    for (int i = 0; i < 100; i++) {
      String s = _TestUtil.randomUnicodeString(random, 10000);
      TokenStream ts = analyzer.tokenStream("foo", new StringReader(s));
      ts.reset();
      while (ts.incrementToken()) {
      }
    }
  }
  
  /** simple test for supplementary characters */
  public void testSurrogates() throws IOException {
    assertAnalyzesTo(analyzer, "𩬅艱鍟䇹愯瀛",
      new String[] { "𩬅", "艱", "鍟", "䇹", "愯", "瀛" });
  }
  
  /** random test ensuring we don't ever split supplementaries */
  public void testSurrogates2() throws IOException {
    int numIterations = atLeast(10000);
    for (int i = 0; i < numIterations; i++) {
      String s = _TestUtil.randomUnicodeString(random, 100);
      TokenStream ts = analyzer.tokenStream("foo", new StringReader(s));
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        assertTrue(UnicodeUtil.validUTF16String(termAtt));
      }
    }
  }
  
  // note: test is kinda silly since kuromoji emits punctuation tokens.
  // but, when/if we filter these out it will be useful.
  public void testEnd() throws Exception {
    assertTokenStreamContents(analyzer.tokenStream("foo", new StringReader("これは本ではない")),
        new String[] { "これ", "は", "本", "で", "は", "ない" },
        new int[] { 0, 2, 3, 4, 5, 6 },
        new int[] { 2, 3, 4, 5, 6, 8 },
        new Integer(8)
    );
    
    assertTokenStreamContents(analyzer.tokenStream("foo", new StringReader("これは本ではない    ")),
        new String[] { "これ", "は", "本", "で", "は", "ない"  },
        new int[] { 0, 2, 3, 4, 5, 6, 8 },
        new int[] { 2, 3, 4, 5, 6, 8, 9 },
        new Integer(12)
    );
  }
}
