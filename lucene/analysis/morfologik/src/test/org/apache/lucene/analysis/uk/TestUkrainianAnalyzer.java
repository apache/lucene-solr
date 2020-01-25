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
package org.apache.lucene.analysis.uk;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

/**
 * Test case for UkrainianAnalyzer.
 */

public class TestUkrainianAnalyzer extends BaseTokenStreamTestCase {

  /** Check that UkrainianAnalyzer doesn't discard any numbers */
  public void testDigitsInUkrainianCharset() throws IOException {
    UkrainianMorfologikAnalyzer ra = new UkrainianMorfologikAnalyzer();
    assertAnalyzesTo(ra, "text 1000", new String[] { "text", "1000" });
    ra.close();
  }

  public void testReusableTokenStream() throws Exception {
    Analyzer a = new UkrainianMorfologikAnalyzer();
    assertAnalyzesTo(a, "Ця п'єса, у свою чергу, рухається по емоційно-напруженому колу за ритм-енд-блюзом.",
                     new String[] { "п'єса", "черга", "рухатися", "емоційно", "напружений", "кола", "коло", "кіл", "ритм", "енд", "блюз" });
    a.close();
  }

  public void testSpecialCharsTokenStream() throws Exception {
    Analyzer a = new UkrainianMorfologikAnalyzer();
    assertAnalyzesTo(a, "м'яса м'я\u0301са м\u02BCяса м\u2019яса м\u2018яса м`яса",
                     new String[] { "м'ясо", "м'ясо", "м'ясо", "м'ясо", "м'ясо", "м'ясо"});
    a.close();
  }

  public void testCapsTokenStream() throws Exception {
    Analyzer a = new UkrainianMorfologikAnalyzer();
    assertAnalyzesTo(a, "Цих Чайковського і Ґете.",
                     new String[] { "Чайковське", "Чайковський", "Гете" });
    a.close();
  }

  public void testCharNormalization() throws Exception {
    Analyzer a = new UkrainianMorfologikAnalyzer();
    assertAnalyzesTo(a, "Ґюмрі та Гюмрі.",
                     new String[] { "Гюмрі", "Гюмрі" });
    a.close();
  }
  
  public void testSampleSentence() throws Exception {
    Analyzer a = new UkrainianMorfologikAnalyzer();
    assertAnalyzesTo(a, "Це — проект генерування словника з тегами частин мови для української мови.",
                     new String[] { "проект", "генерування", "словник", "тег", "частина", "мова", "українська", "український", "Українська", "мова" });
    a.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new UkrainianMorfologikAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
