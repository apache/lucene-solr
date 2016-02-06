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
package org.apache.lucene.analysis.icu.segmentation;


import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

/**
 * test ICUTokenizer with dictionary-based CJ segmentation
 */
public class TestICUTokenizerCJK extends BaseTokenStreamTestCase {
  Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new ICUTokenizer(newAttributeFactory(), new DefaultICUTokenizerConfig(true)));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }
  
  /**
   * test stolen from smartcn
   */
  public void testSimpleChinese() throws Exception {
    assertAnalyzesTo(a, "我购买了道具和服装。",
        new String[] { "我", "购买", "了", "道具", "和", "服装" }
    );
  }
  
  public void testChineseNumerics() throws Exception {
    assertAnalyzesTo(a, "９４８３", new String[] { "９４８３" });
    assertAnalyzesTo(a, "院內分機９４８３。",
        new String[] { "院", "內", "分機", "９４８３" });
    assertAnalyzesTo(a, "院內分機9483。",
        new String[] { "院", "內", "分機", "9483" });
  }
  
  /**
   * test stolen from kuromoji
   */
  public void testSimpleJapanese() throws Exception {
    assertAnalyzesTo(a, "それはまだ実験段階にあります",
        new String[] { "それ", "は", "まだ", "実験", "段階", "に", "あり", "ます"  }
    );
  }
  
  public void testJapaneseTypes() throws Exception {
    assertAnalyzesTo(a, "仮名遣い カタカナ",
        new String[] { "仮名遣い", "カタカナ" },
        new String[] { "<IDEOGRAPHIC>", "<IDEOGRAPHIC>" });
  }
  
  public void testKorean() throws Exception {
    // Korean words
    assertAnalyzesTo(a, "안녕하세요 한글입니다", new String[]{"안녕하세요", "한글입니다"});
  }
  
  /** make sure that we still tag korean as HANGUL (for further decomposition/ngram/whatever) */
  public void testKoreanTypes() throws Exception {
    assertAnalyzesTo(a, "훈민정음",
        new String[] { "훈민정음" },
        new String[] { "<HANGUL>" });
  }
  
  /** blast some random strings through the analyzer */
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-5575")
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), a, 10000*RANDOM_MULTIPLIER);
  }
  
  /** blast some random large strings through the analyzer */
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-5575")
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, a, 100*RANDOM_MULTIPLIER, 8192);
  }
}
