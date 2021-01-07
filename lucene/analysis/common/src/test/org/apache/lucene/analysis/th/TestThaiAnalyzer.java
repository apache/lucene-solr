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
package org.apache.lucene.analysis.th;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;

/** Test case for ThaiAnalyzer, modified from TestFrenchAnalyzer */
public class TestThaiAnalyzer extends BaseTokenStreamTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue(
        "JRE does not support Thai dictionary-based BreakIterator", ThaiTokenizer.DBBI_AVAILABLE);
  }
  /*
   * testcase for offsets
   */
  public void testOffsets() throws Exception {
    Analyzer analyzer = new ThaiAnalyzer(CharArraySet.EMPTY_SET);
    assertAnalyzesTo(
        analyzer,
        "การที่ได้ต้องแสดงว่างานดี",
        new String[] {"การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"},
        new int[] {0, 3, 6, 9, 13, 17, 20, 23},
        new int[] {3, 6, 9, 13, 17, 20, 23, 25});
    analyzer.close();
  }

  public void testStopWords() throws Exception {
    Analyzer analyzer = new ThaiAnalyzer();
    assertAnalyzesTo(
        analyzer,
        "การที่ได้ต้องแสดงว่างานดี",
        new String[] {"แสดง", "งาน", "ดี"},
        new int[] {13, 20, 23},
        new int[] {17, 23, 25},
        new int[] {5, 2, 1});
    analyzer.close();
  }

  /*
   * Test that position increments are adjusted correctly for stopwords.
   */
  // note this test uses stopfilter's stopset
  public void testPositionIncrements() throws Exception {
    final ThaiAnalyzer analyzer = new ThaiAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
    assertAnalyzesTo(
        analyzer,
        "การที่ได้ต้อง the แสดงว่างานดี",
        new String[] {"การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"},
        new int[] {0, 3, 6, 9, 18, 22, 25, 28},
        new int[] {3, 6, 9, 13, 22, 25, 28, 30},
        new int[] {1, 1, 1, 1, 2, 1, 1, 1});

    // case that a stopword is adjacent to thai text, with no whitespace
    assertAnalyzesTo(
        analyzer,
        "การที่ได้ต้องthe แสดงว่างานดี",
        new String[] {"การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"},
        new int[] {0, 3, 6, 9, 17, 21, 24, 27},
        new int[] {3, 6, 9, 13, 21, 24, 27, 29},
        new int[] {1, 1, 1, 1, 2, 1, 1, 1});
    analyzer.close();
  }

  public void testReusableTokenStream() throws Exception {
    ThaiAnalyzer analyzer = new ThaiAnalyzer(CharArraySet.EMPTY_SET);
    assertAnalyzesTo(analyzer, "", new String[] {});

    assertAnalyzesTo(
        analyzer,
        "การที่ได้ต้องแสดงว่างานดี",
        new String[] {"การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});

    assertAnalyzesTo(
        analyzer,
        "บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com",
        new String[] {"บริษัท", "ชื่อ", "xy", "z", "คุย", "กับ", "xyz", "demo.com"});
    analyzer.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new ThaiAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }

  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Analyzer analyzer = new ThaiAnalyzer();
    checkRandomData(random(), analyzer, 3 * RANDOM_MULTIPLIER, 8192);
    analyzer.close();
  }

  // LUCENE-3044
  public void testAttributeReuse() throws Exception {
    ThaiAnalyzer analyzer = new ThaiAnalyzer();
    // just consume
    TokenStream ts = analyzer.tokenStream("dummy", "ภาษาไทย");
    assertTokenStreamContents(ts, new String[] {"ภาษา", "ไทย"});
    // this consumer adds flagsAtt, which this analyzer does not use.
    ts = analyzer.tokenStream("dummy", "ภาษาไทย");
    ts.addAttribute(FlagsAttribute.class);
    assertTokenStreamContents(ts, new String[] {"ภาษา", "ไทย"});
    analyzer.close();
  }

  /** test we fold digits to latin-1 */
  public void testDigits() throws Exception {
    ThaiAnalyzer a = new ThaiAnalyzer();
    checkOneTerm(a, "๑๒๓๔", "1234");
    a.close();
  }

  public void testTwoSentences() throws Exception {
    Analyzer analyzer = new ThaiAnalyzer(CharArraySet.EMPTY_SET);
    assertAnalyzesTo(
        analyzer,
        "This is a test. การที่ได้ต้องแสดงว่างานดี",
        new String[] {
          "this", "is", "a", "test", "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"
        },
        new int[] {0, 5, 8, 10, 16, 19, 22, 25, 29, 33, 36, 39},
        new int[] {4, 7, 9, 14, 19, 22, 25, 29, 33, 36, 39, 41});
    analyzer.close();
  }
}
