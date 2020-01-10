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
package org.apache.lucene.analysis.ko;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Ignore;
import org.junit.Test;

public class TestKoreanNumberFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;

  public static UserDictionary readDict() {
    InputStream is = TestKoreanTokenizer.class.getResourceAsStream("userdict.txt");
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

  @Override
  public void setUp() throws Exception {
    super.setUp();
    UserDictionary userDictionary = readDict();
    Set<POS.Tag> stopTags = new HashSet<>();
    stopTags.add(POS.Tag.SP);
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KoreanTokenizer(newAttributeFactory(), userDictionary,
            KoreanTokenizer.DEFAULT_DECOMPOUND, false, false);
        TokenStream stream = new KoreanPartOfSpeechStopFilter(tokenizer, stopTags);
        return new TokenStreamComponents(tokenizer, new KoreanNumberFilter(stream));
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

    assertAnalyzesTo(analyzer, "오늘 십만이천오백원의 와인 구입",
        new String[]{"오늘", "102500", "원", "의", "와인", "구입"},
        new int[]{0, 3, 9, 10, 12, 15},
        new int[]{2, 9, 10, 11, 14, 17}
    );

    // Wrong analysis
    // "초밥" => "초밥" O, "초"+"밥" X
    assertAnalyzesTo(analyzer, "어제 초밥 가격은 10만 원",
        new String[]{"어제", "초", "밥", "가격", "은", "100000", "원"},
        new int[]{0, 3, 4, 6, 8, 10, 14, 15, 13},
        new int[]{2, 4, 5, 8, 9, 13, 15, 13, 14}
    );

    assertAnalyzesTo(analyzer, "자본금 600만 원",
        new String[]{"자본", "금", "6000000", "원"},
        new int[]{0, 2, 4, 9, 10},
        new int[]{2, 3, 8, 10, 11}
    );
  }

  @Test
  public void testVariants() throws IOException {
    // Test variants of three
    assertAnalyzesTo(analyzer, "3", new String[]{"3"});
    assertAnalyzesTo(analyzer, "３", new String[]{"3"});
    assertAnalyzesTo(analyzer, "삼", new String[]{"3"});

    // Test three variations with trailing zero
    assertAnalyzesTo(analyzer, "03", new String[]{"3"});
    assertAnalyzesTo(analyzer, "０３", new String[]{"3"});
    assertAnalyzesTo(analyzer, "영삼", new String[]{"3"});
    assertAnalyzesTo(analyzer, "003", new String[]{"3"});
    assertAnalyzesTo(analyzer, "００３", new String[]{"3"});
    assertAnalyzesTo(analyzer, "영영삼", new String[]{"3"});

    // Test thousand variants
    assertAnalyzesTo(analyzer, "천", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "1천", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "１천", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "일천", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "일영영영", new String[]{"1000"});
    assertAnalyzesTo(analyzer, "１０백", new String[]{"1000"}); // Strange, but supported
  }

  @Test
  public void testLargeVariants() throws IOException {
    // Test large numbers
    assertAnalyzesTo(analyzer, "삼오칠팔구", new String[]{"35789"});
    assertAnalyzesTo(analyzer, "육백이만오천일", new String[]{"6025001"});
    assertAnalyzesTo(analyzer, "조육백만오천일", new String[]{"1000006005001"});
    assertAnalyzesTo(analyzer, "십조육백만오천일", new String[]{"10000006005001"});
    assertAnalyzesTo(analyzer, "일경일", new String[]{"10000000000000001"});
    assertAnalyzesTo(analyzer, "십경십", new String[]{"100000000000000010"});
    assertAnalyzesTo(analyzer, "해경조억만천백십일", new String[]{"100010001000100011111"});
  }

  @Test
  public void testNegative() throws IOException {
    assertAnalyzesTo(analyzer, "-백만", new String[]{"-", "1000000"});
  }

  @Test
  public void testMixed() throws IOException {
    // Test mixed numbers
    assertAnalyzesTo(analyzer, "삼천2백２십삼", new String[]{"3223"});
    assertAnalyzesTo(analyzer, "３２이삼", new String[]{"3223"});
  }

  @Test
  public void testFunny() throws IOException {
    // Test some oddities for inconsistent input
    assertAnalyzesTo(analyzer, "십십", new String[]{"20"}); // 100?
    assertAnalyzesTo(analyzer, "백백백", new String[]{"300"}); // 10,000?
    assertAnalyzesTo(analyzer, "천천천천", new String[]{"4000"}); // 1,000,000,000,000?
  }

  @Test
  public void testHangulArabic() throws IOException {
    // Test kanji numerals used as Arabic numbers (with head zero)
    assertAnalyzesTo(analyzer, "영일이삼사오육칠팔구구팔칠육오사삼이일영",
        new String[]{"1234567899876543210"}
    );

    // I'm Bond, James "normalized" Bond...
    assertAnalyzesTo(analyzer, "영영칠", new String[]{"7"});
  }

  @Test
  public void testDoubleZero() throws IOException {
    assertAnalyzesTo(analyzer, "영영",
        new String[]{"0"},
        new int[]{0},
        new int[]{2},
        new int[]{1}
    );
  }

  @Test
  public void testName() throws IOException {
    // Test name that normalises to number
    assertAnalyzesTo(analyzer, "전중경일",
        new String[]{"전중", "10000000000000001"}, // 경일 is normalized to a number
        new int[]{0, 2},
        new int[]{2, 4},
        new int[]{1, 1}
    );

    // An analyzer that marks 경일 as a keyword
    Analyzer keywordMarkingAnalyzer = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        CharArraySet set = new CharArraySet(1, false);
        set.add("경일");
        UserDictionary userDictionary = readDict();
        Set<POS.Tag> stopTags = new HashSet<>();
        stopTags.add(POS.Tag.SP);
        Tokenizer tokenizer = new KoreanTokenizer(newAttributeFactory(), userDictionary,
            KoreanTokenizer.DEFAULT_DECOMPOUND, false, false);
        TokenStream stream = new KoreanPartOfSpeechStopFilter(tokenizer, stopTags);
        return new TokenStreamComponents(tokenizer, new KoreanNumberFilter(new SetKeywordMarkerFilter(stream, set)));
      }
    };

    assertAnalyzesTo(keywordMarkingAnalyzer, "전중경일",
        new String[]{"전중", "경일"}, // 경일 is not normalized
        new int[]{0, 2},
        new int[]{2, 4},
        new int[]{1, 1}
    );
    keywordMarkingAnalyzer.close();
  }

  @Test
  public void testDecimal() throws IOException {
    // Test Arabic numbers with punctuation, i.e. 3.2 thousands
    assertAnalyzesTo(analyzer, "１．２만３４５．６７",
        new String[]{"12345.67"}
    );
  }

  @Test
  public void testDecimalPunctuation() throws IOException {
    // Test Arabic numbers with punctuation, i.e. 3.2 thousands won
    assertAnalyzesTo(analyzer, "３．２천 원",
        new String[]{"3200", "원"}
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
        random(), analyzer, true, "영영\u302f\u3029\u3039\u3023\u3033\u302bB", true
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
        UserDictionary userDictionary = readDict();
        Set<POS.Tag> stopTags = new HashSet<>();
        stopTags.add(POS.Tag.SP);
        Tokenizer tokenizer = new KoreanTokenizer(newAttributeFactory(), userDictionary,
            KoreanTokenizer.DEFAULT_DECOMPOUND, false, false);
        return new TokenStreamComponents(tokenizer, new KoreanPartOfSpeechStopFilter(tokenizer, stopTags));
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
