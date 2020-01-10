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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

/**
 * Test Korean morphological analyzer
 */
public class TestKoreanAnalyzer extends BaseTokenStreamTestCase {
  public void testSentence() throws IOException {
    Analyzer a = new KoreanAnalyzer();
    assertAnalyzesTo(a, "한국은 대단한 나라입니다.",
        new String[]{"한국", "대단", "나라", "이"},
        new int[]{ 0, 4, 8, 10 },
        new int[]{ 2, 6, 10, 13 },
        new int[]{ 1, 2, 3, 1 }
    );
    a.close();
  }

  public void testStopTags() throws IOException {
    Set<POS.Tag> stopTags = new HashSet<>(Arrays.asList(POS.Tag.NNP, POS.Tag.NNG));
    Analyzer a = new KoreanAnalyzer(null, KoreanTokenizer.DecompoundMode.DISCARD, stopTags, false);
    assertAnalyzesTo(a, "한국은 대단한 나라입니다.",
        new String[]{"은", "대단", "하", "ᆫ", "이", "ᄇ니다"},
        new int[]{ 2, 4, 6, 6, 10, 10 },
        new int[]{ 3, 6, 7, 7, 13, 13 },
        new int[]{ 2, 1, 1, 1, 2, 1 }
    );
    a.close();
  }

  public void testUnknownWord() throws IOException {
    Analyzer a = new KoreanAnalyzer(null, KoreanTokenizer.DecompoundMode.DISCARD,
        KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS, true);

    assertAnalyzesTo(a,"2018 평창 동계올림픽대회",
        new String[]{"2", "0", "1", "8", "평창", "동계", "올림픽", "대회"},
        new int[]{0, 1, 2, 3, 5, 8, 10, 13},
        new int[]{1, 2, 3, 4, 7, 10, 13, 15},
        new int[]{1, 1, 1, 1, 1, 1, 1, 1});
    a.close();

    a = new KoreanAnalyzer(null, KoreanTokenizer.DecompoundMode.DISCARD,
        KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS, false);

    assertAnalyzesTo(a,"2018 평창 동계올림픽대회",
        new String[]{"2018", "평창", "동계", "올림픽", "대회"},
        new int[]{0, 5, 8, 10, 13},
        new int[]{4, 7, 10, 13, 15},
        new int[]{1, 1, 1, 1, 1});
    a.close();
  }

  /**
   * blast random strings against the analyzer
   */
  public void testRandom() throws IOException {
    Random random = random();
    final Analyzer a = new KoreanAnalyzer();
    checkRandomData(random, a, atLeast(1000));
    a.close();
  }

  /**
   * blast some random large strings through the analyzer
   */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    final Analyzer a = new KoreanAnalyzer();
    checkRandomData(random, a, 2 * RANDOM_MULTIPLIER, 8192);
    a.close();
  }

  // Copied from TestKoreanTokenizer, to make sure passing
  // user dict to analyzer works:
  public void testUserDict() throws IOException {
    final Analyzer analyzer = new KoreanAnalyzer(TestKoreanTokenizer.readDict(),
        KoreanTokenizer.DEFAULT_DECOMPOUND, KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS, false);
    assertAnalyzesTo(analyzer, "c++ 프로그래밍 언어",
        new String[]{"c++", "프로그래밍", "언어"},
        new int[]{0, 4, 10},
        new int[]{3, 9, 12},
        new int[]{1, 1, 1}
    );
  }
}
