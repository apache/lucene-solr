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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockGraphTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.ja.dict.ConnectionCosts;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.ja.tokenattributes.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;

public class
    TestJapaneseTokenizer extends BaseTokenStreamTestCase {

  public static UserDictionary readDict() {
    InputStream is = TestJapaneseTokenizer.class.getResourceAsStream("userdict.txt");
    if (is == null) {
      throw new RuntimeException("Cannot find userdict.txt in test classpath!");
    }
    try {
      try {
        Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        return UserDictionary.open(reader);
      } finally {
        is.close();
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private Analyzer analyzer, analyzerNormal, analyzerNormalNBest, analyzerNoPunct, extendedModeAnalyzerNoPunct;

  private JapaneseTokenizer makeTokenizer(boolean discardPunctuation, Mode mode) {
    return new JapaneseTokenizer(newAttributeFactory(), readDict(), discardPunctuation, mode);
  }

  private Analyzer makeAnalyzer(final Tokenizer t) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(t, t);
      }
    };
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), false, Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    analyzerNormal = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), false, Mode.NORMAL);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    analyzerNormalNBest = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        JapaneseTokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), false, Mode.NORMAL);
        tokenizer.setNBestCost(2000);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    analyzerNoPunct = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), true, Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    extendedModeAnalyzerNoPunct = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), true, Mode.EXTENDED);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(analyzer, analyzerNormal, analyzerNoPunct, extendedModeAnalyzerNoPunct);
    super.tearDown();
  }

  public void testNormalMode() throws Exception {
    assertAnalyzesTo(analyzerNormal,
                     "シニアソフトウェアエンジニア",
                     new String[] {"シニアソフトウェアエンジニア"});
  }

  public void testNormalModeNbest() throws Exception {
    JapaneseTokenizer t = makeTokenizer(true, Mode.NORMAL);
    Analyzer a = makeAnalyzer(t);

    t.setNBestCost(2000);
    assertAnalyzesTo(a,
                     "シニアソフトウェアエンジニア",
                     new String[] {"シニア", "シニアソフトウェアエンジニア", "ソフトウェア", "エンジニア"});

    t.setNBestCost(5000);
    assertAnalyzesTo(a,
                     "シニアソフトウェアエンジニア",
                     new String[] {"シニア", "シニアソフトウェアエンジニア", "ソフト", "ソフトウェア", "ウェア", "エンジニア"});

    t.setNBestCost(0);
    assertAnalyzesTo(a,
                     "数学部長谷川",
                     new String[] {"数学", "部長", "谷川"});

    t.setNBestCost(3000);
    assertAnalyzesTo(a,
                     "数学部長谷川",
                     new String[] {"数学", "部", "部長", "長谷川", "谷川"});

    t.setNBestCost(0);
    assertAnalyzesTo(a,
                     "経済学部長",
                     new String[] {"経済", "学", "部長"});

    t.setNBestCost(2000);
    assertAnalyzesTo(a,
                     "経済学部長",
                     new String[] {"経済", "経済学部", "学", "部長", "長"});

    t.setNBestCost(0);
    assertAnalyzesTo(a,
                     "成田空港、米原油流出",
                     new String[] {"成田空港", "米", "原油", "流出"});

    t.setNBestCost(4000);
    assertAnalyzesTo(a,
                     "成田空港、米原油流出",
                     new String[] {"成田空港", "米", "米原", "原油", "油", "流出"});
  }

  public void testSearchModeNbest() throws Exception {
    JapaneseTokenizer t = makeTokenizer(true, Mode.SEARCH);
    Analyzer a = makeAnalyzer(t);

    t.setNBestCost(0);
    assertAnalyzesTo(a,
                     "成田空港、米原油流出",
                     new String[] {"成田", "成田空港", "空港", "米", "原油", "流出"});

    t.setNBestCost(4000);
    assertAnalyzesTo(a,
                     "成田空港、米原油流出",
                     new String[] {"成田", "成田空港", "空港", "米", "米原", "原油", "油", "流出"});
  }

  private ArrayList<String> makeTokenList(Analyzer a, String in) throws Exception {
    ArrayList<String> list = new ArrayList<>();
    TokenStream ts = a.tokenStream("dummy", in);
    CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);

    ts.reset();
    while (ts.incrementToken()) {
      list.add(termAtt.toString());
    }
    ts.end();
    ts.close();
    return list;
  }

  private boolean checkToken(Analyzer a, String in, String requitedToken) throws Exception {
    return makeTokenList(a, in).indexOf(requitedToken) != -1;
  }

  public void testNBestCost() throws Exception {
    JapaneseTokenizer t = makeTokenizer(true, Mode.NORMAL);
    Analyzer a = makeAnalyzer(t);

    t.setNBestCost(0);
    assertFalse("学部 is not a token of 数学部長谷川", checkToken(a, "数学部長谷川", "学部"));

    assertTrue("cost calculated /数学部長谷川-学部/", 0 <= t.calcNBestCost("/数学部長谷川-学部/"));
    t.setNBestCost(t.calcNBestCost("/数学部長谷川-学部/"));
    assertTrue("学部 is a token of 数学部長谷川", checkToken(a, "数学部長谷川", "学部"));

    assertTrue("cost calculated /数学部長谷川-数/成田空港-成/", 0 <= t.calcNBestCost("/数学部長谷川-数/成田空港-成/"));
    t.setNBestCost(t.calcNBestCost("/数学部長谷川-数/成田空港-成/"));
    assertTrue("数 is a token of 数学部長谷川", checkToken(a, "数学部長谷川", "数"));
    assertTrue("成 is a token of 成田空港", checkToken(a, "成田空港", "成"));
  }

  public void testDecomposition1() throws Exception {
    assertAnalyzesTo(analyzerNoPunct, "本来は、貧困層の女性や子供に医療保護を提供するために創設された制度である、" +
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
    assertAnalyzesTo(analyzerNoPunct, "麻薬の密売は根こそぎ絶やさなければならない",
      new String[] { "麻薬", "の", "密売", "は", "根こそぎ", "絶やさ", "なけれ", "ば", "なら", "ない" },
      new int[] { 0, 2, 3, 5, 6,  10, 13, 16, 17, 19 },
      new int[] { 2, 3, 5, 6, 10, 13, 16, 17, 19, 21 }
    );
  }

  public void testDecomposition3() throws Exception {
    assertAnalyzesTo(analyzerNoPunct, "魔女狩大将マシュー・ホプキンス。",
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
    try (TokenStream ts = analyzer.tokenStream("bogus", "くよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよくよ")) {
      ts.reset();
      while (ts.incrementToken()) {

      }
      ts.end();
    }
  }

  /*
    // NOTE: intentionally fails!  Just trying to debug this
    // one input...
  public void testDecomposition6() throws Exception {
    assertAnalyzesTo(analyzer, "奈良先端科学技術大学院大学",
      new String[] { "これ", "は", "本", "で", "は", "ない" },
      new int[] { 0, 2, 3, 4, 5, 6 },
      new int[] { 2, 3, 4, 5, 6, 8 }
                     );
  }
  */

  /** Tests that sentence offset is incorporated into the resulting offsets */
  public void testTwoSentences() throws Exception {
    /*
    //TokenStream ts = a.tokenStream("foo", "妹の咲子です。俺と年子で、今受験生です。");
    TokenStream ts = analyzer.tokenStream("foo", "&#x250cdf66<!--\"<!--#<!--;?><!--#<!--#><!---->?>-->;");
    ts.reset();
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    while(ts.incrementToken()) {
      System.out.println("  " + termAtt.toString());
    }
    System.out.println("DONE PARSE\n\n");
    */

    assertAnalyzesTo(analyzerNoPunct, "魔女狩大将マシュー・ホプキンス。 魔女狩大将マシュー・ホプキンス。",
      new String[] { "魔女", "狩", "大将", "マシュー", "ホプキンス",  "魔女", "狩", "大将", "マシュー",  "ホプキンス"  },
      new int[] { 0, 2, 3, 5, 10, 17, 19, 20, 22, 27 },
      new int[] { 2, 3, 5, 9, 15, 19, 20, 22, 26, 32 }
    );
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), analyzer, 500*RANDOM_MULTIPLIER);
    checkRandomData(random(), analyzerNoPunct, 500*RANDOM_MULTIPLIER);
    checkRandomData(random(), analyzerNormalNBest, 500*RANDOM_MULTIPLIER);
  }

  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, analyzer, 20*RANDOM_MULTIPLIER, 8192);
    checkRandomData(random, analyzerNoPunct, 20*RANDOM_MULTIPLIER, 8192);
    checkRandomData(random, analyzerNormalNBest, 20*RANDOM_MULTIPLIER, 8192);
  }

  public void testRandomHugeStringsMockGraphAfter() throws Exception {
    // Randomly inject graph tokens after JapaneseTokenizer:
    Random random = random();
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), false, Mode.SEARCH);
        TokenStream graph = new MockGraphTokenFilter(random(), tokenizer);
        return new TokenStreamComponents(tokenizer, graph);
      }
    };
    checkRandomData(random, analyzer, 20*RANDOM_MULTIPLIER, 8192);
    analyzer.close();
  }

  public void testLargeDocReliability() throws Exception {
    for (int i = 0; i < 10; i++) {
      String s = TestUtil.randomUnicodeString(random(), 10000);
      try (TokenStream ts = analyzer.tokenStream("foo", s)) {
        ts.reset();
        while (ts.incrementToken()) {
        }
        ts.end();
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
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + i);
      }
      String s = TestUtil.randomUnicodeString(random(), 100);
      try (TokenStream ts = analyzer.tokenStream("foo", s)) {
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
          assertTrue(UnicodeUtil.validUTF16String(termAtt));
        }
        ts.end();
      }
    }
  }

  public void testOnlyPunctuation() throws IOException {
    try (TokenStream ts = analyzerNoPunct.tokenStream("foo", "。、。。")) {
      ts.reset();
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  public void testOnlyPunctuationExtended() throws IOException {
    try (TokenStream ts = extendedModeAnalyzerNoPunct.tokenStream("foo", "......")) {
      ts.reset();
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  // note: test is kinda silly since kuromoji emits punctuation tokens.
  // but, when/if we filter these out it will be useful.
  public void testEnd() throws Exception {
    assertTokenStreamContents(analyzerNoPunct.tokenStream("foo", "これは本ではない"),
        new String[] { "これ", "は", "本", "で", "は", "ない" },
        new int[] { 0, 2, 3, 4, 5, 6 },
        new int[] { 2, 3, 4, 5, 6, 8 },
        new Integer(8)
    );

    assertTokenStreamContents(analyzerNoPunct.tokenStream("foo", "これは本ではない    "),
        new String[] { "これ", "は", "本", "で", "は", "ない"  },
        new int[] { 0, 2, 3, 4, 5, 6, 8 },
        new int[] { 2, 3, 4, 5, 6, 8, 9 },
        new Integer(12)
    );
  }

  public void testUserDict() throws Exception {
    // Not a great test because w/o userdict.txt the
    // segmentation is the same:
    assertTokenStreamContents(analyzer.tokenStream("foo", "関西国際空港に行った"),
                              new String[] { "関西", "国際", "空港", "に", "行っ", "た"  },
                              new int[] { 0, 2, 4, 6, 7, 9 },
                              new int[] { 2, 4, 6, 7, 9, 10 },
                              new Integer(10)
    );
  }

  public void testUserDict2() throws Exception {
    // Better test: w/o userdict the segmentation is different:
    assertTokenStreamContents(analyzer.tokenStream("foo", "朝青龍"),
                              new String[] { "朝青龍"  },
                              new int[] { 0 },
                              new int[] { 3 },
                              new Integer(3)
    );
  }

  public void testUserDict3() throws Exception {
    // Test entry that breaks into multiple tokens:
    assertTokenStreamContents(analyzer.tokenStream("foo", "abcd"),
                              new String[] { "a", "b", "cd"  },
                              new int[] { 0, 1, 2 },
                              new int[] { 1, 2, 4 },
                              new Integer(4)
    );
  }

  // HMM: fails (segments as a/b/cd/efghij)... because the
  // two paths have exactly equal paths (1 KNOWN + 1
  // UNKNOWN) and we don't seem to favor longer KNOWN /
  // shorter UNKNOWN matches:

  /*
  public void testUserDict4() throws Exception {
    // Test entry that has another entry as prefix
    assertTokenStreamContents(analyzer.tokenStream("foo", "abcdefghij"),
                              new String[] { "ab", "cd", "efg", "hij"  },
                              new int[] { 0, 2, 4, 7 },
                              new int[] { 2, 4, 7, 10 },
                              new Integer(10)
    );
  }
  */

  public void testSegmentation() throws Exception {
    // Skip tests for Michelle Kwan -- UniDic segments Kwan as ク ワン
    //   String input = "ミシェル・クワンが優勝しました。スペースステーションに行きます。うたがわしい。";
    //   String[] surfaceForms = {
        //        "ミシェル", "・", "クワン", "が", "優勝", "し", "まし", "た", "。",
        //        "スペース", "ステーション", "に", "行き", "ます", "。",
        //        "うたがわしい", "。"
    //   };
    String input = "スペースステーションに行きます。うたがわしい。";
    String[] surfaceForms = {
        "スペース", "ステーション", "に", "行き", "ます", "。",
        "うたがわしい", "。"
    };
    assertAnalyzesTo(analyzer,
                     input,
                     surfaceForms);
  }

  public void testLatticeToDot() throws Exception {
    final GraphvizFormatter gv2 = new GraphvizFormatter(ConnectionCosts.getInstance());
    final Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        JapaneseTokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), false, Mode.SEARCH);
        tokenizer.setGraphvizFormatter(gv2);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };

    String input = "スペースステーションに行きます。うたがわしい。";
    String[] surfaceForms = {
        "スペース", "ステーション", "に", "行き", "ます", "。",
        "うたがわしい", "。"
    };
    assertAnalyzesTo(analyzer,
                     input,
                     surfaceForms);

    assertTrue(gv2.finish().indexOf("22.0") != -1);
    analyzer.close();
  }

  private void assertReadings(String input, String... readings) throws IOException {
    try (TokenStream ts = analyzer.tokenStream("ignored", input)) {
      ReadingAttribute readingAtt = ts.addAttribute(ReadingAttribute.class);
      ts.reset();
      for(String reading : readings) {
        assertTrue(ts.incrementToken());
        assertEquals(reading, readingAtt.getReading());
      }
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  private void assertPronunciations(String input, String... pronunciations) throws IOException {
    try (TokenStream ts = analyzer.tokenStream("ignored", input)) {
      ReadingAttribute readingAtt = ts.addAttribute(ReadingAttribute.class);
      ts.reset();
      for(String pronunciation : pronunciations) {
        assertTrue(ts.incrementToken());
        assertEquals(pronunciation, readingAtt.getPronunciation());
      }
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  private void assertBaseForms(String input, String... baseForms) throws IOException {
    try (TokenStream ts = analyzer.tokenStream("ignored", input)) {
      BaseFormAttribute baseFormAtt = ts.addAttribute(BaseFormAttribute.class);
      ts.reset();
      for(String baseForm : baseForms) {
        assertTrue(ts.incrementToken());
        assertEquals(baseForm, baseFormAtt.getBaseForm());
      }
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  private void assertInflectionTypes(String input, String... inflectionTypes) throws IOException {
    try (TokenStream ts = analyzer.tokenStream("ignored", input)) {
      InflectionAttribute inflectionAtt = ts.addAttribute(InflectionAttribute.class);
      ts.reset();
      for(String inflectionType : inflectionTypes) {
        assertTrue(ts.incrementToken());
        assertEquals(inflectionType, inflectionAtt.getInflectionType());
      }
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  private void assertInflectionForms(String input, String... inflectionForms) throws IOException {
    try (TokenStream ts = analyzer.tokenStream("ignored", input)) {
      InflectionAttribute inflectionAtt = ts.addAttribute(InflectionAttribute.class);
      ts.reset();
      for(String inflectionForm : inflectionForms) {
        assertTrue(ts.incrementToken());
        assertEquals(inflectionForm, inflectionAtt.getInflectionForm());
      }
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  private void assertPartsOfSpeech(String input, String... partsOfSpeech) throws IOException {
    try (TokenStream ts = analyzer.tokenStream("ignored", input)) {
      PartOfSpeechAttribute partOfSpeechAtt = ts.addAttribute(PartOfSpeechAttribute.class);
      ts.reset();
      for(String partOfSpeech : partsOfSpeech) {
        assertTrue(ts.incrementToken());
        assertEquals(partOfSpeech, partOfSpeechAtt.getPartOfSpeech());
      }
      assertFalse(ts.incrementToken());
      ts.end();
    }
  }

  public void testReadings() throws Exception {
    assertReadings("寿司が食べたいです。",
                   "スシ",
                   "ガ",
                   "タベ",
                   "タイ",
                   "デス",
                   "。");
  }

  public void testReadings2() throws Exception {
    assertReadings("多くの学生が試験に落ちた。",
                   "オオク",
                   "ノ",
                   "ガクセイ",
                   "ガ",
                   "シケン",
                   "ニ",
                   "オチ",
                   "タ",
                   "。");
  }

  public void testPronunciations() throws Exception {
    assertPronunciations("寿司が食べたいです。",
                         "スシ",
                         "ガ",
                         "タベ",
                         "タイ",
                         "デス",
                         "。");
  }

  public void testPronunciations2() throws Exception {
    // pronunciation differs from reading here
    assertPronunciations("多くの学生が試験に落ちた。",
                         "オーク",
                         "ノ",
                         "ガクセイ",
                         "ガ",
                         "シケン",
                         "ニ",
                         "オチ",
                         "タ",
                         "。");
  }

  public void testBasicForms() throws Exception {
    assertBaseForms("それはまだ実験段階にあります。",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "ある",
                    null,
                    null);
  }

  public void testInflectionTypes() throws Exception {
    assertInflectionTypes("それはまだ実験段階にあります。",
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                          "五段・ラ行",
                          "特殊・マス",
                          null);
  }

  public void testInflectionForms() throws Exception {
    assertInflectionForms("それはまだ実験段階にあります。",
                          null,
                          null,
                          null,
                          null,
                          null,
                          null,
                          "連用形",
                          "基本形",
                          null);
  }

  public void testPartOfSpeech() throws Exception {
    assertPartsOfSpeech("それはまだ実験段階にあります。",
                        "名詞-代名詞-一般",
                        "助詞-係助詞",
                        "副詞-助詞類接続",
                        "名詞-サ変接続",
                        "名詞-一般",
                        "助詞-格助詞-一般",
                        "動詞-自立",
                        "助動詞",
                        "記号-句点");
  }

  // TODO: the next 2 tests are no longer using the first/last word ids, maybe lookup the words and fix?
  // do we have a possibility to actually lookup the first and last word from dictionary?
  public void testYabottai() throws Exception {
    assertAnalyzesTo(analyzer, "やぼったい",
                     new String[] {"やぼったい"});
  }

  public void testTsukitosha() throws Exception {
    assertAnalyzesTo(analyzer, "突き通しゃ",
                     new String[] {"突き通しゃ"});
  }

  public void testBocchan() throws Exception {
    doTestBocchan(1);
  }

  @Nightly
  public void testBocchanBig() throws Exception {
    doTestBocchan(100);
  }

  /*
  public void testWikipedia() throws Exception {
    final FileInputStream fis = new FileInputStream("/q/lucene/jawiki-20120220-pages-articles.xml");
    final Reader r = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

    final long startTimeNS = System.nanoTime();
    boolean done = false;
    long compoundCount = 0;
    long nonCompoundCount = 0;
    long netOffset = 0;
    while (!done) {
      final TokenStream ts = analyzer.tokenStream("ignored", r);
      ts.reset();
      final PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
      final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      int count = 0;
      while (true) {
        if (!ts.incrementToken()) {
          done = true;
          break;
        }
        count++;
        if (posIncAtt.getPositionIncrement() == 0) {
          compoundCount++;
        } else {
          nonCompoundCount++;
          if (nonCompoundCount % 1000000 == 0) {
            System.out.println(String.format("%.2f msec [pos=%d, %d, %d]",
                                             (System.nanoTime()-startTimeNS)/1000000.0,
                                             netOffset + offsetAtt.startOffset(),
                                             nonCompoundCount,
                                             compoundCount));
          }
        }
        if (count == 100000000) {
          System.out.println("  again...");
          break;
        }
      }
      ts.end();
      netOffset += offsetAtt.endOffset();
    }
    System.out.println("compoundCount=" + compoundCount + " nonCompoundCount=" + nonCompoundCount);
    r.close();
  }
  */


  private void doTestBocchan(int numIterations) throws Exception {
    LineNumberReader reader = new LineNumberReader(new InputStreamReader(
        this.getClass().getResourceAsStream("bocchan.utf-8"), StandardCharsets.UTF_8));
    String line = reader.readLine();
    reader.close();

    if (VERBOSE) {
      System.out.println("Test for Bocchan without pre-splitting sentences");
    }

    /*
    if (numIterations > 1) {
      // warmup
      for (int i = 0; i < numIterations; i++) {
        final TokenStream ts = analyzer.tokenStream("ignored", line);
        ts.reset();
        while(ts.incrementToken());
      }
    }
    */

    long totalStart = System.currentTimeMillis();
    for (int i = 0; i < numIterations; i++) {
      try (TokenStream ts = analyzer.tokenStream("ignored", line)) {
        ts.reset();
        while(ts.incrementToken());
        ts.end();
      }
    }
    String[] sentences = line.split("、|。");
    if (VERBOSE) {
      System.out.println("Total time : " + (System.currentTimeMillis() - totalStart));
      System.out.println("Test for Bocchan with pre-splitting sentences (" + sentences.length + " sentences)");
    }
    totalStart = System.currentTimeMillis();
    for (int i = 0; i < numIterations; i++) {
      for (String sentence: sentences) {
        try (TokenStream ts = analyzer.tokenStream("ignored", sentence)) {
          ts.reset();
          while(ts.incrementToken());
          ts.end();
        }
      }
    }
    if (VERBOSE) {
      System.out.println("Total time : " + (System.currentTimeMillis() - totalStart));
    }
  }

  public void testWithPunctuation() throws Exception {
    assertAnalyzesTo(analyzerNoPunct, "羽田。空港",
                     new String[] { "羽田", "空港" },
                     new int[] { 1, 1 });
  }

  public void testCompoundOverPunctuation() throws Exception {
    assertAnalyzesToPositions(analyzerNoPunct, "dεε϶ϢϏΎϷΞͺ羽田",
                              new String[] { "d", "ε", "ε", "ϢϏΎϷΞͺ", "羽田" },
                              new int[] { 1, 1, 1, 1, 1},
                              new int[] { 1, 1, 1, 1, 1});
  }

  public void testEmptyUserDict() throws Exception {
    Reader emptyReader = new StringReader("\n# This is an empty user dictionary\n\n");
    UserDictionary emptyDict = UserDictionary.open(emptyReader);

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), emptyDict, false, Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };

    assertAnalyzesTo(analyzer, "これは本ではない",
        new String[]{"これ", "は", "本", "で", "は", "ない"},
        new int[]{0, 2, 3, 4, 5, 6},
        new int[]{2, 3, 4, 5, 6, 8}
    );
    analyzer.close();
  }

  public void testBigDocument() throws Exception {
    String doc = "商品の購入・詳細(サイズ、画像)は商品名をクリックしてください！[L.B　CANDY　STOCK]フラワービジューベアドレス[L.B　DAILY　STOCK]ボーダーニットトップス［L.B　DAILY　STOCK］ボーダーロングニットOP［L.B　DAILY　STOCK］ロゴトートBAG［L.B　DAILY　STOCK］裏毛ロゴプリントプルオーバー【TVドラマ着用】アンゴラワッフルカーディガン【TVドラマ着用】グラフィティーバックリボンワンピース【TVドラマ着用】ボーダーハイネックトップス【TVドラマ着用】レオパードミッドカーフスカート【セットアップ対応商品】起毛ニットスカート【セットアップ対応商品】起毛ニットプルオーバー2wayサングラス33ナンバーリングニット3Dショルダーフレアードレス3周年スリッパ3周年ラグマット3周年ロックグラスキャンドルLily　Brown　2015年　福袋MIXニットプルオーバーPeckhamロゴニットアンゴラジャガードプルオーバーアンゴラタートルアンゴラチュニックアンゴラニットカーディガンアンゴラニットプルオーバーアンゴラフレアワンピースアンゴラロングカーディガンアンゴラワッフルカーディガンヴィンテージファー付コートヴィンテージボーダーニットヴィンテージレースハイネックトップスヴィンテージレースブラウスウエストシースルーボーダーワンピースオーガンジーラインフレアスカートオープンショルダーニットトップスオフショルシャーリングワンピースオフショルニットオフショルニットプルオーバーオフショルボーダーロンパースオフショルワイドコンビネゾンオルテガ柄ニットプルオーバーカシュクールオフショルワンピースカットアシンメトリードレスカットサテンプリーツフレアースカートカラースーパーハイウェストスキニーカラーブロックドレスカラーブロックニットチュニックギャザーフレアスカートキラキラストライプタイトスカートキラキラストライプドレスキルティングファーコートグラデーションベアドレスグラデーションラウンドサングラスグラフティーオフショルトップスグラフティーキュロットグリッターリボンヘアゴムクロップドブラウスケーブルハイウエストスカートコーデュロイ×スエードパネルスカートコーデュロイタイトスカートゴールドバックルベルト付スカートゴシックヒールショートブーツゴシック柄ニットワンピコンビスタジャンサイドステッチボーイズデニムパンツサスペつきショートパンツサスペンダー付プリーツロングスカートシャーリングタイトスカートジャガードタックワンピーススエードフリルフラワーパンツスエード裏毛肩空きトップススクエアショルダーBAGスクエアバックルショルダースクエアミニバッグストーンビーチサンダルストライプサスペ付きスキニーストライプバックスリットシャツスライバーシャギーコートタートル×レースタイトスカートタートルニットプルオーバータイトジャンパースカートダブルクロスチュールフレアスカートダブルストラップパンプスダブルハートリングダブルフェイスチェックストールチェーンコンビビジューネックレスチェーンコンビビジューピアスチェーンコンビビジューブレスチェーンツバ広HATチェーンビジューピアスチェックニットプルオーバーチェックネルミディアムスカートチェック柄スキニーパンツチュールコンビアシメトップスデニムフレアースカートドットオフショルフリルブラウスドットジャガードドレスドットニットプルオーバードットレーストップスニット×オーガンジースカートセットニットキャミソールワンピースニットスヌードパールコンビフープピアスハイウエストショートデニムハイウエストタイトスカートハイウエストデニムショートパンツハイウエストプリーツスカートハイウエストミッドカーフスカートハイゲージタートルニットハイゲージラインニットハイネック切り替えスウェットバタフライネックレスバタフライミニピアスバタフライリングバックタンクリブワンピースバックリボンスキニーデニムパンツバックリボン深Vワンピースビジューストラップサンダルビスチェコンビオフショルブラウスブークレジャガードニットフェイクムートンショートコートフェレットカーディガンフェレットビックタートルニットブラウジングクルーブラウスプリーツブラウスフリルニットプルオーバーフリンジニットプルオーバーフレアニットスカートブロウ型サングラスベーシックフェレットプルオーバーベルト付ガウチョパンツベルト付ショートパンツベルト付タックスカートベルト付タックパンツベルベットインヒールパンプスベロアウェッジパンプスベロアミッドカーフワンピースベロアワンピースベロア風ニットカーディガンボア付コートボーダーVネックTシャツボーダーオフショルカットソーボーダーカットソーワンピースボーダータイトカットソーボーダートップスボーダートップス×スカートセットボストンメガネマオカラーシャツニットセットミックスニットプルオーバーミッドカーフ丈ポンチスカートミリタリーギャザーショートパンツメッシュハイネックトップスメルトンPコートメルトンダッフルコートメルトンダブルコートモヘアニットカーディガンモヘアニットタートルユリ柄プリーツフレアースカートライダースデニムジャケットライナー付チェスターコートラッフルプリーツブラウスラメジャガードハイゲージニットリブニットワンピリボン×パールバレッタリボンバレッタリボンベルトハイウエストパンツリリー刺繍開襟ブラウスレースビスチェローファーサボロゴニットキャップロゴ刺繍ニットワッチロングニットガウンワッフルアンゴラプルオーバーワンショルダワーワンピース光沢ラメニットカーディガン刺繍シフォンブラウス台形ミニスカート配色ニットプルオーバー裏毛プルオーバー×オーガンジースカートセット";

    JapaneseTokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), readDict(), false, Mode.NORMAL);
    tokenizer.setReader(new StringReader(doc));
    tokenizer.reset();
    while (tokenizer.incrementToken());
  }
}
