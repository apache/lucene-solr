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
package org.apache.lucene.analysis.cn.smart;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.IOUtils;

public class TestSmartChineseAnalyzer extends BaseTokenStreamTestCase {

  public void testChineseStopWordsDefault() throws Exception {
    Analyzer ca = new SmartChineseAnalyzer(); /* will load stopwords */
    String sentence = "我购买了道具和服装。";
    String result[] = { "我", "购买", "了", "道具", "和", "服装" };
    assertAnalyzesTo(ca, sentence, result);
    ca.close();
    // set stop-words from the outer world - must yield same behavior
    ca = new SmartChineseAnalyzer(SmartChineseAnalyzer.getDefaultStopSet());
    assertAnalyzesTo(ca, sentence, result);
    ca.close();
  }
  
  /*
   * This test is the same as the above, except with two phrases.
   * This tests to ensure the SentenceTokenizer->WordTokenFilter chain works correctly.
   */
  public void testChineseStopWordsDefaultTwoPhrases() throws Exception {
    Analyzer ca = new SmartChineseAnalyzer(); /* will load stopwords */
    String sentence = "我购买了道具和服装。 我购买了道具和服装。";
    String result[] = { "我", "购买", "了", "道具", "和", "服装", "我", "购买", "了", "道具", "和", "服装" };
    assertAnalyzesTo(ca, sentence, result);
    ca.close();
  }

  /*
   * This test is for test smartcn HHMMSegmenter should correctly handle surrogate character.
   */
  public void testSurrogatePairCharacter() throws Exception {
    Analyzer ca = new SmartChineseAnalyzer(); /* will load stopwords */
    String sentence =
        Stream.of(
                "\uD872\uDF3B",
                "\uD872\uDF4A",
                "\uD872\uDF73",
                "\uD872\uDF5B",
                "\u9FCF",
                "\uD86D\uDFFC",
                "\uD872\uDF2D",
                "\u9FD4")
            .collect(Collectors.joining());
    String result[] = {
      "\uD872\uDF3B",
      "\uD872\uDF4A",
      "\uD872\uDF73",
      "\uD872\uDF5B",
      "\u9FCF",
      "\uD86D\uDFFC",
      "\uD872\uDF2D",
      "\u9FD4"
    };
    assertAnalyzesTo(ca, sentence, result);
    ca.close();
  }

  /*
   * This test is the same as the above, except using an ideographic space as a separator.
   * This tests to ensure the stopwords are working correctly.
   */
  public void testChineseStopWordsDefaultTwoPhrasesIdeoSpace() throws Exception {
    Analyzer ca = new SmartChineseAnalyzer(); /* will load stopwords */
    String sentence = "我购买了道具和服装　我购买了道具和服装。";
    String result[] = { "我", "购买", "了", "道具", "和", "服装", "我", "购买", "了", "道具", "和", "服装" };
    assertAnalyzesTo(ca, sentence, result);
    ca.close();
  }
  
  /*
   * Punctuation is handled in a strange way if you disable stopwords
   * In this example the IDEOGRAPHIC FULL STOP is converted into a comma.
   * if you don't supply (true) to the constructor, or use a different stopwords list,
   * then punctuation is indexed.
   */
  public void testChineseStopWordsOff() throws Exception {
    Analyzer[] analyzers = new Analyzer[] {
      new SmartChineseAnalyzer(false),/* doesn't load stopwords */
      new SmartChineseAnalyzer(null) /* sets stopwords to empty set */};
    String sentence = "我购买了道具和服装。";
    String result[] = { "我", "购买", "了", "道具", "和", "服装", "," };
    for (Analyzer analyzer : analyzers) {
      assertAnalyzesTo(analyzer, sentence, result);
      assertAnalyzesTo(analyzer, sentence, result);
    }
    IOUtils.close(analyzers);
  }
  
  /*
   * Check that position increments after stopwords are correct,
   * when stopfilter is configured with enablePositionIncrements
   */
  public void testChineseStopWords2() throws Exception {
    Analyzer ca = new SmartChineseAnalyzer(); /* will load stopwords */
    String sentence = "Title:San"; // : is a stopword
    String result[] = { "titl", "san"};
    int startOffsets[] = { 0, 6 };
    int endOffsets[] = { 5, 9 };
    int posIncr[] = { 1, 2 };
    assertAnalyzesTo(ca, sentence, result, startOffsets, endOffsets, posIncr);
    ca.close();
  }
  
  public void testChineseAnalyzer() throws Exception {
    Analyzer ca = new SmartChineseAnalyzer(true);
    String sentence = "我购买了道具和服装。";
    String[] result = { "我", "购买", "了", "道具", "和", "服装" };
    assertAnalyzesTo(ca, sentence, result);
    ca.close();
  }
  
  /*
   * English words are lowercased and porter-stemmed.
   */
  public void testMixedLatinChinese() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer(true);
    assertAnalyzesTo(analyzer, "我购买 Tests 了道具和服装",
        new String[] { "我", "购买", "test", "了", "道具", "和", "服装"});
    analyzer.close();
  }
  
  /*
   * Numerics are parsed as their own tokens
   */
  public void testNumerics() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer(true);
    assertAnalyzesTo(analyzer, "我购买 Tests 了道具和服装1234",
      new String[] { "我", "购买", "test", "了", "道具", "和", "服装", "1234"});
    analyzer.close();
  }
  
  /*
   * Full width alphas and numerics are folded to half-width
   */
  public void testFullWidth() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer(true);
    assertAnalyzesTo(analyzer, "我购买 Ｔｅｓｔｓ 了道具和服装１２３４",
        new String[] { "我", "购买", "test", "了", "道具", "和", "服装", "1234"});
    analyzer.close();
  }
  
  /*
   * Presentation form delimiters are removed
   */
  public void testDelimiters() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer(true);
    assertAnalyzesTo(analyzer, "我购买︱ Tests 了道具和服装",
        new String[] { "我", "购买", "test", "了", "道具", "和", "服装"});
    analyzer.close();
  }
  
  /*
   * Text from writing systems other than Chinese and Latin are parsed as individual characters.
   * (regardless of Unicode category)
   */
  public void testNonChinese() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer(true);
    assertAnalyzesTo(analyzer, "我购买 روبرتTests 了道具和服装",
        new String[] { "我", "购买", "ر", "و", "ب", "ر", "ت", "test", "了", "道具", "和", "服装"});
    analyzer.close();
  }
  
  /*
   * Test what the analyzer does with out-of-vocabulary words.
   * In this case the name is Yousaf Raza Gillani.
   * Currently it is being analyzed into single characters...
   */
  public void testOOV() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer(true);
    assertAnalyzesTo(analyzer, "优素福·拉扎·吉拉尼",
      new String[] { "优", "素", "福", "拉", "扎", "吉", "拉", "尼" });
    
    assertAnalyzesTo(analyzer, "优素福拉扎吉拉尼",
      new String[] { "优", "素", "福", "拉", "扎", "吉", "拉", "尼" });
    analyzer.close();
  }

  public void testOffsets() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer(true);
    assertAnalyzesTo(analyzer, "我购买了道具和服装",
        new String[] { "我", "购买", "了", "道具", "和", "服装" },
        new int[] { 0, 1, 3, 4, 6, 7 },
        new int[] { 1, 3, 4, 6, 7, 9 });
    analyzer.close();
  }

  public void testReusableTokenStream() throws Exception {
    Analyzer a = new SmartChineseAnalyzer();
    assertAnalyzesTo(a, "我购买 Tests 了道具和服装",
        new String[] { "我", "购买", "test", "了", "道具", "和", "服装"},
        new int[] { 0, 1, 4, 10, 11, 13, 14 },
        new int[] { 1, 3, 9, 11, 13, 14, 16 });
    assertAnalyzesTo(a, "我购买了道具和服装。",
        new String[] { "我", "购买", "了", "道具", "和", "服装" },
        new int[] { 0, 1, 3, 4, 6, 7 },
        new int[] { 1, 3, 4, 6, 7, 9 });
    a.close();
  }

  // LUCENE-3026
  public void testLargeDocument() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      sb.append("我购买了道具和服装。");
    }
    try (Analyzer analyzer = new SmartChineseAnalyzer();
         TokenStream stream = analyzer.tokenStream("", sb.toString())) {
      stream.reset();
      while (stream.incrementToken()) {
      }
      stream.end();
    }
  }

  // LUCENE-3026
  public void testLargeSentence() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      sb.append("我购买了道具和服装");
    }
    try (Analyzer analyzer = new SmartChineseAnalyzer();
         TokenStream stream = analyzer.tokenStream("", sb.toString())) {
      stream.reset();
      while (stream.incrementToken()) {
      }
      stream.end();
    }
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }

  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Analyzer analyzer = new SmartChineseAnalyzer();
    checkRandomData(random(), analyzer, 3 * RANDOM_MULTIPLIER, 8192);
    analyzer.close();
  }
}
