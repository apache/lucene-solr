package org.apache.lucene.analysis.ko;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import org.junit.Ignore;

public class TestKoreanAnalyzer extends BaseTokenStreamTestCase {


  public void testBasics() throws IOException {
    assertAnalyzesTo(new KoreanAnalyzer(TEST_VERSION_CURRENT), "자바로 전부 제작된 텍스트 검색 엔진 라이브러리",
        new String[]{"자바", "전부", "제작", "텍스트", "검색", "엔진", "라이브러리"},
        new int[]{0, 4, 7, 11, 15, 18, 21},
        new int[]{2, 6, 9, 14, 17, 20, 26},
        new int[]{1, 1, 1, 1, 1, 1, 1}
    );
  }
  
  // don't know why we have this, but it should at least do washington dc, not washington d.
  public void testAcronym() throws IOException {
    assertAnalyzesTo(new KoreanAnalyzer(TEST_VERSION_CURRENT), "Washington D.C.",
        new String[] { "washington", "dc" }
    );
  }

  public void testCompoundNoun() throws IOException {
    
    KoreanAnalyzer analyzer = new KoreanAnalyzer(TEST_VERSION_CURRENT);
    analyzer.setOriginCNoun(true);
    analyzer.setHasOrigin(false);
    
    assertAnalyzesTo(analyzer, "빅데이터",
        new String[]{"빅데이터","빅", "데이터"},
        new int[]{0,0, 1},
        new int[]{4,1, 4},
        new int[]{1,0, 1}
    );
    
  }
  
  @Ignore("TODO: Known issue for Soomyung to look into")
  public void testCompoundNoun1() throws IOException {
    
    KoreanAnalyzer analyzer = new KoreanAnalyzer(TEST_VERSION_CURRENT);
    analyzer.setOriginCNoun(false);
    
    assertAnalyzesTo(analyzer, "빅데이터",
        new String[]{"빅", "데이터"},
        new int[]{0, 1},
        new int[]{1, 4},
        new int[]{1, 1}
    );

  }
  
  /**
   * TEST FAIL: useCharFilter=false text='\u02ac0\ucb2c\u2606 '
   * 
   * NOTE: reproduce with: ant test  -Dtestcase=TestKoreanAnalyzer -Dtests.method=testRandom -Dtests.seed=3550FAE96FFD2DA6 -Dtests.locale=en_GB -Dtests.timezone=Mexico/BajaNorte -Dtests.file.encoding=UTF-8
   * 
   * java.lang.AssertionError: pos=0 posLen=1 token=ʬ0 expected:<3> but was:<2>
   * at __randomizedtesting.SeedInfo.seed([3550FAE96FFD2DA6:471CDFE6DE9D9BD5]:0)
   */  
//  public void testRandom() throws IOException {
//    Random random = random();
//    final Analyzer a = new KoreanAnalyzer(TEST_VERSION_CURRENT);    
//    checkRandomData(random, a, atLeast(10000));
//  }


  public void testOutput() throws IOException {
    String korean = "자바로 전부 제작된 텍스트 검색 엔진 라이브러리";
    Analyzer analyzer = new KoreanAnalyzer(TEST_VERSION_CURRENT);

    TokenStream stream = analyzer.tokenStream("dummy", korean);
    stream.reset();

    CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAttr = stream.addAttribute(OffsetAttribute.class);
    TypeAttribute typeAttr = stream.addAttribute(TypeAttribute.class);
    PositionIncrementAttribute positionAttr = stream.addAttribute(PositionIncrementAttribute.class);

    while (stream.incrementToken()) {

      System.out.println(
          "term: " + termAttr +
              "\ttype: " + typeAttr.type() +
              "\tstart offset: " + offsetAttr.startOffset() +
              "\tend offset: " + offsetAttr.endOffset() +
              "\tposition increment: " + positionAttr.getPositionIncrement()
      );
    }
  }
}
