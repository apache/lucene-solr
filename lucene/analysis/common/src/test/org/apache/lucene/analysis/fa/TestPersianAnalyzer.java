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
package org.apache.lucene.analysis.fa;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Test the Persian Analyzer
 * 
 */
public class TestPersianAnalyzer extends BaseTokenStreamTestCase {

  /**
   * This test fails with NPE when the stopwords file is missing in classpath
   */
  public void testResourcesAvailable() {
    new PersianAnalyzer().close();
  }

  /**
   * This test shows how the combination of tokenization (breaking on zero-width
   * non-joiner), normalization (such as treating arabic YEH and farsi YEH the
   * same), and stopwords creates a light-stemming effect for verbs.
   * 
   * These verb forms are from http://en.wikipedia.org/wiki/Persian_grammar
   */
  public void testBehaviorVerbs() throws Exception {
    Analyzer a = new PersianAnalyzer();
    // active present indicative
    assertAnalyzesTo(a, "می‌خورد", new String[] { "خورد" });
    // active preterite indicative
    assertAnalyzesTo(a, "خورد", new String[] { "خورد" });
    // active imperfective preterite indicative
    assertAnalyzesTo(a, "می‌خورد", new String[] { "خورد" });
    // active future indicative
    assertAnalyzesTo(a, "خواهد خورد", new String[] { "خورد" });
    // active present progressive indicative
    assertAnalyzesTo(a, "دارد می‌خورد", new String[] { "خورد" });
    // active preterite progressive indicative
    assertAnalyzesTo(a, "داشت می‌خورد", new String[] { "خورد" });

    // active perfect indicative
    assertAnalyzesTo(a, "خورده‌است", new String[] { "خورده" });
    // active imperfective perfect indicative
    assertAnalyzesTo(a, "می‌خورده‌است", new String[] { "خورده" });
    // active pluperfect indicative
    assertAnalyzesTo(a, "خورده بود", new String[] { "خورده" });
    // active imperfective pluperfect indicative
    assertAnalyzesTo(a, "می‌خورده بود", new String[] { "خورده" });
    // active preterite subjunctive
    assertAnalyzesTo(a, "خورده باشد", new String[] { "خورده" });
    // active imperfective preterite subjunctive
    assertAnalyzesTo(a, "می‌خورده باشد", new String[] { "خورده" });
    // active pluperfect subjunctive
    assertAnalyzesTo(a, "خورده بوده باشد", new String[] { "خورده" });
    // active imperfective pluperfect subjunctive
    assertAnalyzesTo(a, "می‌خورده بوده باشد", new String[] { "خورده" });
    // passive present indicative
    assertAnalyzesTo(a, "خورده می‌شود", new String[] { "خورده" });
    // passive preterite indicative
    assertAnalyzesTo(a, "خورده شد", new String[] { "خورده" });
    // passive imperfective preterite indicative
    assertAnalyzesTo(a, "خورده می‌شد", new String[] { "خورده" });
    // passive perfect indicative
    assertAnalyzesTo(a, "خورده شده‌است", new String[] { "خورده" });
    // passive imperfective perfect indicative
    assertAnalyzesTo(a, "خورده می‌شده‌است", new String[] { "خورده" });
    // passive pluperfect indicative
    assertAnalyzesTo(a, "خورده شده بود", new String[] { "خورده" });
    // passive imperfective pluperfect indicative
    assertAnalyzesTo(a, "خورده می‌شده بود", new String[] { "خورده" });
    // passive future indicative
    assertAnalyzesTo(a, "خورده خواهد شد", new String[] { "خورده" });
    // passive present progressive indicative
    assertAnalyzesTo(a, "دارد خورده می‌شود", new String[] { "خورده" });
    // passive preterite progressive indicative
    assertAnalyzesTo(a, "داشت خورده می‌شد", new String[] { "خورده" });
    // passive present subjunctive
    assertAnalyzesTo(a, "خورده شود", new String[] { "خورده" });
    // passive preterite subjunctive
    assertAnalyzesTo(a, "خورده شده باشد", new String[] { "خورده" });
    // passive imperfective preterite subjunctive
    assertAnalyzesTo(a, "خورده می‌شده باشد", new String[] { "خورده" });
    // passive pluperfect subjunctive
    assertAnalyzesTo(a, "خورده شده بوده باشد", new String[] { "خورده" });
    // passive imperfective pluperfect subjunctive
    assertAnalyzesTo(a, "خورده می‌شده بوده باشد", new String[] { "خورده" });

    // active present subjunctive
    assertAnalyzesTo(a, "بخورد", new String[] { "بخورد" });
    a.close();
  }

  /**
   * This test shows how the combination of tokenization and stopwords creates a
   * light-stemming effect for verbs.
   * 
   * In this case, these forms are presented with alternative orthography, using
   * arabic yeh and whitespace. This yeh phenomenon is common for legacy text
   * due to some previous bugs in Microsoft Windows.
   * 
   * These verb forms are from http://en.wikipedia.org/wiki/Persian_grammar
   */
  public void testBehaviorVerbsDefective() throws Exception {
    Analyzer a = new PersianAnalyzer();
    // active present indicative
    assertAnalyzesTo(a, "مي خورد", new String[] { "خورد" });
    // active preterite indicative
    assertAnalyzesTo(a, "خورد", new String[] { "خورد" });
    // active imperfective preterite indicative
    assertAnalyzesTo(a, "مي خورد", new String[] { "خورد" });
    // active future indicative
    assertAnalyzesTo(a, "خواهد خورد", new String[] { "خورد" });
    // active present progressive indicative
    assertAnalyzesTo(a, "دارد مي خورد", new String[] { "خورد" });
    // active preterite progressive indicative
    assertAnalyzesTo(a, "داشت مي خورد", new String[] { "خورد" });

    // active perfect indicative
    assertAnalyzesTo(a, "خورده است", new String[] { "خورده" });
    // active imperfective perfect indicative
    assertAnalyzesTo(a, "مي خورده است", new String[] { "خورده" });
    // active pluperfect indicative
    assertAnalyzesTo(a, "خورده بود", new String[] { "خورده" });
    // active imperfective pluperfect indicative
    assertAnalyzesTo(a, "مي خورده بود", new String[] { "خورده" });
    // active preterite subjunctive
    assertAnalyzesTo(a, "خورده باشد", new String[] { "خورده" });
    // active imperfective preterite subjunctive
    assertAnalyzesTo(a, "مي خورده باشد", new String[] { "خورده" });
    // active pluperfect subjunctive
    assertAnalyzesTo(a, "خورده بوده باشد", new String[] { "خورده" });
    // active imperfective pluperfect subjunctive
    assertAnalyzesTo(a, "مي خورده بوده باشد", new String[] { "خورده" });
    // passive present indicative
    assertAnalyzesTo(a, "خورده مي شود", new String[] { "خورده" });
    // passive preterite indicative
    assertAnalyzesTo(a, "خورده شد", new String[] { "خورده" });
    // passive imperfective preterite indicative
    assertAnalyzesTo(a, "خورده مي شد", new String[] { "خورده" });
    // passive perfect indicative
    assertAnalyzesTo(a, "خورده شده است", new String[] { "خورده" });
    // passive imperfective perfect indicative
    assertAnalyzesTo(a, "خورده مي شده است", new String[] { "خورده" });
    // passive pluperfect indicative
    assertAnalyzesTo(a, "خورده شده بود", new String[] { "خورده" });
    // passive imperfective pluperfect indicative
    assertAnalyzesTo(a, "خورده مي شده بود", new String[] { "خورده" });
    // passive future indicative
    assertAnalyzesTo(a, "خورده خواهد شد", new String[] { "خورده" });
    // passive present progressive indicative
    assertAnalyzesTo(a, "دارد خورده مي شود", new String[] { "خورده" });
    // passive preterite progressive indicative
    assertAnalyzesTo(a, "داشت خورده مي شد", new String[] { "خورده" });
    // passive present subjunctive
    assertAnalyzesTo(a, "خورده شود", new String[] { "خورده" });
    // passive preterite subjunctive
    assertAnalyzesTo(a, "خورده شده باشد", new String[] { "خورده" });
    // passive imperfective preterite subjunctive
    assertAnalyzesTo(a, "خورده مي شده باشد", new String[] { "خورده" });
    // passive pluperfect subjunctive
    assertAnalyzesTo(a, "خورده شده بوده باشد", new String[] { "خورده" });
    // passive imperfective pluperfect subjunctive
    assertAnalyzesTo(a, "خورده مي شده بوده باشد", new String[] { "خورده" });

    // active present subjunctive
    assertAnalyzesTo(a, "بخورد", new String[] { "بخورد" });
    a.close();
  }

  /**
   * This test shows how the combination of tokenization (breaking on zero-width
   * non-joiner or space) and stopwords creates a light-stemming effect for
   * nouns, removing the plural -ha.
   */
  public void testBehaviorNouns() throws Exception {
    Analyzer a = new PersianAnalyzer();
    assertAnalyzesTo(a, "برگ ها", new String[] { "برگ" });
    assertAnalyzesTo(a, "برگ‌ها", new String[] { "برگ" });
    a.close();
  }

  /**
   * Test showing that non-persian text is treated very much like SimpleAnalyzer
   * (lowercased, etc)
   */
  public void testBehaviorNonPersian() throws Exception {
    Analyzer a = new PersianAnalyzer();
    assertAnalyzesTo(a, "English test.", new String[] { "english", "test" });
    a.close();
  }
  
  /**
   * Basic test ensuring that tokenStream works correctly.
   */
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new PersianAnalyzer();
    assertAnalyzesTo(a, "خورده مي شده بوده باشد", new String[] { "خورده" });
    assertAnalyzesTo(a, "برگ‌ها", new String[] { "برگ" });
    a.close();
  }
  
  /**
   * Test that custom stopwords work, and are not case-sensitive.
   */
  public void testCustomStopwords() throws Exception {
    PersianAnalyzer a = new PersianAnalyzer( 
        new CharArraySet( asSet("the", "and", "a"), false));
    assertAnalyzesTo(a, "The quick brown fox.", new String[] { "quick",
        "brown", "fox" });
    a.close();
  }
  
  /**
   * test we fold digits to latin-1
   */
  public void testDigits() throws Exception {
    PersianAnalyzer a = new PersianAnalyzer();
    checkOneTerm(a, "۱۲۳۴", "1234");
    a.close();
  }
  
  /**
   * test that we don't fold digits for back compat behavior
   * @deprecated remove this test in lucene 7
   */
  @Deprecated
  public void testDigitsBackCompat() throws Exception {
    PersianAnalyzer a = new PersianAnalyzer();
    a.setVersion(Version.LUCENE_5_3_0);
    checkOneTerm(a, "۱۲۳۴", "۱۲۳۴");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    PersianAnalyzer a = new PersianAnalyzer();
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }

  public void testBackcompat40() throws IOException {
    PersianAnalyzer a = new PersianAnalyzer();
    a.setVersion(Version.LUCENE_4_6_1);
    // this is just a test to see the correct unicode version is being used, not actually testing hebrew
    assertAnalyzesTo(a, "א\"א", new String[] {"א", "א"});
  }
}
