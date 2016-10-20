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
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Ignore;
import org.junit.Test;

public class TestJapaneseNumberFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, false, JapaneseTokenizer.Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, new JapaneseNumberFilter(tokenizer));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }
  
  @Test
  public void testBasics() throws IOException {

    assertAnalyzesTo(analyzer, "本日十万二千五百円のワインを買った",
        new String[]{"本日", "102500", "円", "の", "ワイン", "を", "買っ", "た"},
        new int[]{0, 2, 8, 9, 10, 13, 14, 16},
        new int[]{2, 8, 9, 10, 13, 14, 16, 17}
    );

    assertAnalyzesTo(analyzer, "昨日のお寿司は１０万円でした。",
        new String[]{"昨日", "の", "お", "寿司", "は", "100000", "円", "でし", "た", "。"},
        new int[]{0, 2, 3, 4, 6, 7, 10, 11, 13, 14},
        new int[]{2, 3, 4, 6, 7, 10, 11, 13, 14, 15}
    );

    assertAnalyzesTo(analyzer, "アティリカの資本金は６００万円です",
        new String[]{"アティリカ", "の", "資本", "金", "は", "6000000", "円", "です"},
        new int[]{0, 5, 6, 8, 9, 10, 14, 15},
        new int[]{5, 6, 8, 9, 10, 14, 15, 17}
    );
  }

  @Test
  public void testVariants() throws IOException {
    // Test variants of three
    assertAnalyzesTo(analyzer, "3", new String[]{"3"});
    assertAnalyzesTo(analyzer, "３", new String[]{"3"});
    assertAnalyzesTo(analyzer, "三", new String[]{"3"});

    // Test three variations with trailing zero
    assertAnalyzesTo(analyzer, "03", new String[]{"3"});
    assertAnalyzesTo(analyzer, "０３", new String[]{"3"});
    assertAnalyzesTo(analyzer, "〇三", new String[]{"3"});
    assertAnalyzesTo(analyzer, "003", new String[]{"3"});
    assertAnalyzesTo(analyzer, "００３", new String[]{"3"});
    assertAnalyzesTo(analyzer, "〇〇三", new String[]{"3"});

    // Test thousand variants
    assertAnalyzesTo(analyzer, "千", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "1千", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "１千", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "一千", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "一〇〇〇", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "１０百", new String[]{"1000"}); // Strange, but supported
  }

  @Test
  public void testLargeVariants() throws IOException {
    // Test large numbers
    assertAnalyzesTo(analyzer, "三五七八九", new String[]{"35789"});
    assertAnalyzesTo(analyzer, "六百二万五千一", new String[]{"6025001"});
    assertAnalyzesTo(analyzer, "兆六百万五千一", new String[]{"1000006005001"});
    assertAnalyzesTo(analyzer, "十兆六百万五千一", new String[]{"10000006005001"});
    assertAnalyzesTo(analyzer, "一京一", new String[]{"10000000000000001"});
    assertAnalyzesTo(analyzer, "十京十", new String[]{"100000000000000010"});
    assertAnalyzesTo(analyzer, "垓京兆億万千百十一", new String[]{"100010001000100011111"});
  }

  @Test
  public void testNegative() throws IOException {
    assertAnalyzesTo(analyzer, "-100万", new String[]{"-", "1000000"});
  }

  @Test
  public void testMixed() throws IOException {
    // Test mixed numbers
    assertAnalyzesTo(analyzer, "三千2百２十三", new String[]{"3223"});
    assertAnalyzesTo(analyzer, "３２二三", new String[]{"3223"});
  }

  @Test
  public void testNininsankyaku() throws IOException {
    // Unstacked tokens
    assertAnalyzesTo(analyzer, "二", new String[]{"2"});
    assertAnalyzesTo(analyzer, "二人", new String[]{"2", "人"});
    assertAnalyzesTo(analyzer, "二人三", new String[]{"2", "人", "3"});
    // Stacked tokens - emit tokens as they are
    assertAnalyzesTo(analyzer, "二人三脚", new String[]{"二", "二人三脚", "人", "三", "脚"});
  }

  @Test
  public void testFujiyaichinisanu() throws IOException {
    // Stacked tokens with a numeral partial
    assertAnalyzesTo(analyzer, "不二家一二三", new String[]{"不", "不二家", "二", "家", "123"});
  }

  @Test
  public void testFunny() throws IOException {
    // Test some oddities for inconsistent input
    assertAnalyzesTo(analyzer, "十十", new String[]{"20"}); // 100?
    assertAnalyzesTo(analyzer, "百百百", new String[]{"300"}); // 10,000?
    assertAnalyzesTo(analyzer, "千千千千", new String[]{"4000"}); // 1,000,000,000,000?
  }

  @Test
  public void testKanjiArabic() throws IOException {
    // Test kanji numerals used as Arabic numbers (with head zero)
    assertAnalyzesTo(analyzer, "〇一二三四五六七八九九八七六五四三二一〇",
        new String[]{"1234567899876543210"}
    );

    // I'm Bond, James "normalized" Bond...
    assertAnalyzesTo(analyzer, "〇〇七", new String[]{"7"});
  }

  @Test
  public void testDoubleZero() throws IOException {
    assertAnalyzesTo(analyzer, "〇〇",
        new String[]{"0"},
        new int[]{0},
        new int[]{2},
        new int[]{1}
    );
  }

  @Test
  public void testName() throws IOException {
    // Test name that normalises to number
    assertAnalyzesTo(analyzer, "田中京一",
        new String[]{"田中", "10000000000000001"}, // 京一 is normalized to a number
        new int[]{0, 2},
        new int[]{2, 4},
        new int[]{1, 1}
    );

    // An analyzer that marks 京一 as a keyword
    Analyzer keywordMarkingAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        CharArraySet set = new CharArraySet(1, false);
        set.add("京一");

        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, false, JapaneseTokenizer.Mode.SEARCH);
        return new TokenStreamComponents(tokenizer, new JapaneseNumberFilter(new SetKeywordMarkerFilter(tokenizer, set)));
      }
    };

    assertAnalyzesTo(keywordMarkingAnalyzer, "田中京一",
        new String[]{"田中", "京一"}, // 京一 is not normalized
        new int[]{0, 2},
        new int[]{2, 4},
        new int[]{1, 1}
    );
    keywordMarkingAnalyzer.close();
  }

  @Test
  public void testDecimal() throws IOException {
    // Test Arabic numbers with punctuation, i.e. 3.2 thousands
    assertAnalyzesTo(analyzer, "１．２万３４５．６７",
        new String[]{"12345.67"}
    );
  }

  @Test
  public void testDecimalPunctuation() throws IOException {
    // Test Arabic numbers with punctuation, i.e. 3.2 thousands yen
    assertAnalyzesTo(analyzer, "３．２千円",
        new String[]{"3200", "円"}
    );
  }

  @Test
  public void testThousandSeparator() throws IOException {
    assertAnalyzesTo(analyzer, "4,647",
        new String[]{"4647"}
    );
  }

  @Test
  public void testDecimalThousandSeparator() throws IOException {
    assertAnalyzesTo(analyzer, "4,647.0010",
        new String[]{"4647.001"}
    );
  }

  @Test
  public void testCommaDecimalSeparator() throws IOException {
    assertAnalyzesTo(analyzer, "15,7",
        new String[]{"157"}
    );
  }

  @Test
  public void testTrailingZeroStripping() throws IOException {
    assertAnalyzesTo(analyzer, "1000.1000",
        new String[]{"1000.1"}
    );
    assertAnalyzesTo(analyzer, "1000.0000",
        new String[]{"1000"}
    );
  }

  @Test
  public void testEmpty() throws IOException {
    assertAnalyzesTo(analyzer, "", new String[]{});
  }

  @Test
  public void testRandomHugeStrings() throws Exception {
    checkRandomData(random(), analyzer, 50 * RANDOM_MULTIPLIER, 8192);
  }

  @Test
  public void testRandomSmallStrings() throws Exception {
    checkRandomData(random(), analyzer, 500 * RANDOM_MULTIPLIER, 128);
  }

  @Test
  public void testFunnyIssue() throws Exception {
    BaseTokenStreamTestCase.checkAnalysisConsistency(
        random(), analyzer, true, "〇〇\u302f\u3029\u3039\u3023\u3033\u302bB", true
    );
  }

  @Ignore("This test is used during development when analyze normalizations in large amounts of text")
  @Test
  public void testLargeData() throws IOException {
    Path input = Paths.get("/tmp/test.txt");
    Path tokenizedOutput = Paths.get("/tmp/test.tok.txt");
    Path normalizedOutput = Paths.get("/tmp/test.norm.txt");

    Analyzer plainAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, false, JapaneseTokenizer.Mode.SEARCH);
        return new TokenStreamComponents(tokenizer);
      }
    };

    analyze(
        plainAnalyzer,
        Files.newBufferedReader(input, StandardCharsets.UTF_8),
        Files.newBufferedWriter(tokenizedOutput, StandardCharsets.UTF_8)
    );

    analyze(
        analyzer,
        Files.newBufferedReader(input, StandardCharsets.UTF_8),
        Files.newBufferedWriter(normalizedOutput, StandardCharsets.UTF_8)
    );
    plainAnalyzer.close();
  }

  public void analyze(Analyzer analyzer, Reader reader, Writer writer) throws IOException {
    TokenStream stream = analyzer.tokenStream("dummy", reader);
    stream.reset();

    CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);

    while (stream.incrementToken()) {
      writer.write(termAttr.toString());
      writer.write("\n");
    }

    reader.close();
    writer.close();
  }
}
