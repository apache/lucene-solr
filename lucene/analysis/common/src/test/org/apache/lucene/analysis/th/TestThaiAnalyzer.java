package org.apache.lucene.analysis.th;

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
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Test case for ThaiAnalyzer, modified from TestFrenchAnalyzer
 *
 */

public class TestThaiAnalyzer extends BaseTokenStreamTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
  }
  /*
   * testcase for offsets
   */
  public void testOffsets() throws Exception {
    assertAnalyzesTo(new ThaiAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET), "การที่ได้ต้องแสดงว่างานดี",
        new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี" },
        new int[] { 0, 3, 6, 9, 13, 17, 20, 23 },
        new int[] { 3, 6, 9, 13, 17, 20, 23, 25 });
  }

  public void testStopWords() throws Exception {
    assertAnalyzesTo(new ThaiAnalyzer(TEST_VERSION_CURRENT), "การที่ได้ต้องแสดงว่างานดี",
        new String[] { "แสดง", "งาน", "ดี" },
        new int[] { 13, 20, 23 },
        new int[] { 17, 23, 25 },
        new int[] { 5, 2, 1 });
  }

  public void testBackwardsStopWords() throws Exception {
     assertAnalyzesTo(new ThaiAnalyzer(Version.LUCENE_35), "การที่ได้ต้องแสดงว่างานดี",
          new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี" },
          new int[] { 0, 3, 6, 9, 13, 17, 20, 23 },
          new int[] { 3, 6, 9, 13, 17, 20, 23, 25 });
  }

  public void testTokenType() throws Exception {
      assertAnalyzesTo(new ThaiAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET), "การที่ได้ต้องแสดงว่างานดี ๑๒๓", 
                       new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี", "๑๒๓" },
                       new String[] { "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>", 
                                      "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>", 
                                      "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>",
                                      "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>",
                                      "<NUM>" });
  }

  /**
   * Thai numeric tokens were typed as <ALPHANUM> instead of <NUM>.
   * @deprecated (3.1) testing backwards behavior
    */
  @Deprecated
  public void testBuggyTokenType30() throws Exception {
    assertAnalyzesTo(new ThaiAnalyzer(Version.LUCENE_30), "การที่ได้ต้องแสดงว่างานดี ๑๒๓",
                         new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี", "๑๒๓" },
                         new String[] { "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", 
                                        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", 
                                        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>" });
  }

  /** @deprecated (3.1) testing backwards behavior */
  @Deprecated
    public void testAnalyzer30() throws Exception {
        ThaiAnalyzer analyzer = new ThaiAnalyzer(Version.LUCENE_30);

    assertAnalyzesTo(analyzer, "", new String[] {});

    assertAnalyzesTo(
      analyzer,
      "การที่ได้ต้องแสดงว่างานดี",
      new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});

    assertAnalyzesTo(
      analyzer,
      "บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com",
      new String[] { "บริษัท", "ชื่อ", "xy&z", "คุย", "กับ", "xyz@demo.com" });

    // English stop words
    assertAnalyzesTo(
      analyzer,
      "ประโยคว่า The quick brown fox jumped over the lazy dogs",
      new String[] { "ประโยค", "ว่า", "quick", "brown", "fox", "jumped", "over", "lazy", "dogs" });
  }

  /*
   * Test that position increments are adjusted correctly for stopwords.
   */
  // note this test uses stopfilter's stopset
  public void testPositionIncrements() throws Exception {
    final ThaiAnalyzer analyzer = new ThaiAnalyzer(TEST_VERSION_CURRENT, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    assertAnalyzesTo(analyzer, "การที่ได้ต้อง the แสดงว่างานดี", 
        new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี" },
        new int[] { 0, 3, 6, 9, 18, 22, 25, 28 },
        new int[] { 3, 6, 9, 13, 22, 25, 28, 30 },
        new int[] { 1, 1, 1, 1, 2, 1, 1, 1 });

    // case that a stopword is adjacent to thai text, with no whitespace
    assertAnalyzesTo(analyzer, "การที่ได้ต้องthe แสดงว่างานดี", 
        new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี" },
        new int[] { 0, 3, 6, 9, 17, 21, 24, 27 },
        new int[] { 3, 6, 9, 13, 21, 24, 27, 29 },
        new int[] { 1, 1, 1, 1, 2, 1, 1, 1 });
  }

  public void testReusableTokenStream() throws Exception {
    ThaiAnalyzer analyzer = new ThaiAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET);
    assertAnalyzesToReuse(analyzer, "", new String[] {});

      assertAnalyzesToReuse(
          analyzer,
          "การที่ได้ต้องแสดงว่างานดี",
          new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});

      assertAnalyzesToReuse(
          analyzer,
          "บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com",
          new String[] { "บริษัท", "ชื่อ", "xy", "z", "คุย", "กับ", "xyz", "demo.com" });
  }

  /** @deprecated (3.1) for version back compat */
  @Deprecated
  public void testReusableTokenStream30() throws Exception {
      ThaiAnalyzer analyzer = new ThaiAnalyzer(Version.LUCENE_30);
      assertAnalyzesToReuse(analyzer, "", new String[] {});

      assertAnalyzesToReuse(
            analyzer,
            "การที่ได้ต้องแสดงว่างานดี",
            new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});

      assertAnalyzesToReuse(
            analyzer,
            "บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com",
            new String[] { "บริษัท", "ชื่อ", "xy&z", "คุย", "กับ", "xyz@demo.com" });
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new ThaiAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, new ThaiAnalyzer(TEST_VERSION_CURRENT), 100*RANDOM_MULTIPLIER, 8192);
  }
  
  // LUCENE-3044
  public void testAttributeReuse() throws Exception {
    ThaiAnalyzer analyzer = new ThaiAnalyzer(Version.LUCENE_30);
    // just consume
    TokenStream ts = analyzer.tokenStream("dummy", new StringReader("ภาษาไทย"));
    assertTokenStreamContents(ts, new String[] { "ภาษา", "ไทย" });
    // this consumer adds flagsAtt, which this analyzer does not use. 
    ts = analyzer.tokenStream("dummy", new StringReader("ภาษาไทย"));
    ts.addAttribute(FlagsAttribute.class);
    assertTokenStreamContents(ts, new String[] { "ภาษา", "ไทย" });
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new ThaiWordFilter(TEST_VERSION_CURRENT, tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
