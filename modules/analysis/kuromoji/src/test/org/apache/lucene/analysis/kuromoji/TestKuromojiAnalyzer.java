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
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TestKuromojiAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new KuromojiAnalyzer(TEST_VERSION_CURRENT);
  }
  
  /**
   * An example sentence, test removal of particles, etc by POS,
   * lemmatization with the basic form, and that position increments
   * and offsets are correct.
   */
  public void testBasics() throws IOException {
    assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "多くの学生が試験に落ちた。",
        new String[] { "多く", "学生", "試験", "落ちる" },
        new int[] { 0, 3, 6,  9 },
        new int[] { 2, 5, 8, 11 },
        new int[] { 1, 2, 2,  2 }
      );
  }

  /**
   * Test that search mode is enabled and working by default
   */
  public void testDecomposition() throws IOException {

    // nocommit
    TokenStream ts = new KuromojiAnalyzer(TEST_VERSION_CURRENT)
        .tokenStream("foo",
                     new StringReader("マイケルジャクソン	マイケル ジャクソン"));
    while(ts.incrementToken());

    // Senior software engineer:
    if (KuromojiTokenizer2.DO_OUTPUT_COMPOUND) {
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "シニアソフトウェアエンジニア",
                       new String[] { "シニア",
                                      "シニアソフトウェアエンジニア",
                                      "ソフトウェア",
                                      "エンジニア" },
                       new int[] { 1, 0, 1, 1}
                       );

      // Kansai International Airport:
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "関西国際空港",
                       new String[] { "関西",
                                      "関西国際空港", // zero pos inc
                                      "国際",
                                      "空港" },
                       new int[] {1, 0, 1, 1}
                       );

      // Konika Minolta Holdings; not quite the right
      // segmentation (see LUCENE-3726):
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "コニカミノルタホールディングス",
                       new String[] { "コニカ",
                                      "コニカミノルタホールディングス", // zero pos inc
                                      "ミノルタ", 
                                      "ホールディングス"},
                       new int[] {1, 0, 1, 1}
                       );

      // Narita Airport
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "成田空港",
                       new String[] { "成田",
                                      "成田空港",
                                      "空港" },
                       new int[] {1, 0, 1});

      // Kyoto University Baseball Club
      // nocommit --segments differently but perhaps OK
      /*
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "京都大学硬式野球部",
                       new String[] { "京都",
                                      "京都大学硬式野球部",
                                      "大学",
                                      "硬式",
                                      "野球",
                                      "部" },
                       new int[] {1, 0, 1, 1, 1, 1});
      */
    } else {
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "シニアソフトウェアエンジニア",
                       new String[] { "シニア",
                                      "ソフトウェア",
                                      "エンジニア" },
                       new int[] { 1, 1, 1}
                       );
      // Kansai International Airport:
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "関西国際空港",
                       new String[] { "関西",
                                      "国際",
                                      "空港" },
                       new int[] {1, 1, 1}
                       );

      // Konika Minolta Holdings; not quite the right
      // segmentation (see LUCENE-3726):
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "コニカミノルタホールディングス",
                       new String[] { "コニカ",
                                      "ミノルタ", 
                                      "ホールディングス"},
                       new int[] {1, 1, 1}
                       );

      // Narita Airport
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "成田空港",
                       new String[] { "成田",
                                      "空港" },
                       new int[] {1, 1});

      // Kyoto University Baseball Club
      assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "京都大学硬式野球部",
                       new String[] { "京都",
                                      "大学",
                                      "硬式",
                                      "野球",
                                      "部" },
                       new int[] {1, 1, 1, 1, 1});
    }
  }

  
  /**
   * blast random strings against the analyzer
   */
  public void testRandom() throws IOException {
    checkRandomData(random, new KuromojiAnalyzer(TEST_VERSION_CURRENT), atLeast(10000));
  }
}
