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
package org.apache.lucene.analysis.ar;


import java.io.IOException;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Test the Arabic Analyzer
 *
 */
public class TestArabicAnalyzer extends BaseTokenStreamTestCase {
  
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new ArabicAnalyzer().close();
  }
  
  /**
   * Some simple tests showing some features of the analyzer, how some regular forms will conflate
   */
  public void testBasicFeatures() throws Exception {
    ArabicAnalyzer a = new ArabicAnalyzer();
    assertAnalyzesTo(a, "كبير", new String[] { "كبير" });
    assertAnalyzesTo(a, "كبيرة", new String[] { "كبير" }); // feminine marker
    
    assertAnalyzesTo(a, "مشروب", new String[] { "مشروب" });
    assertAnalyzesTo(a, "مشروبات", new String[] { "مشروب" }); // plural -at
    
    assertAnalyzesTo(a, "أمريكيين", new String[] { "امريك" }); // plural -in
    assertAnalyzesTo(a, "امريكي", new String[] { "امريك" }); // singular with bare alif
    
    assertAnalyzesTo(a, "كتاب", new String[] { "كتاب" }); 
    assertAnalyzesTo(a, "الكتاب", new String[] { "كتاب" }); // definite article
    
    assertAnalyzesTo(a, "ما ملكت أيمانكم", new String[] { "ملكت", "ايمانكم"});
    assertAnalyzesTo(a, "الذين ملكت أيمانكم", new String[] { "ملكت", "ايمانكم" }); // stopwords
    a.close();
  }
  
  /**
   * Simple tests to show things are getting reset correctly, etc.
   */
  public void testReusableTokenStream() throws Exception {
    ArabicAnalyzer a = new ArabicAnalyzer();
    assertAnalyzesTo(a, "كبير", new String[] { "كبير" });
    assertAnalyzesTo(a, "كبيرة", new String[] { "كبير" }); // feminine marker
    a.close();
  }

  /**
   * Non-arabic text gets treated in a similar way as SimpleAnalyzer.
   */
  public void testEnglishInput() throws Exception {
    ArabicAnalyzer a = new ArabicAnalyzer();
    assertAnalyzesTo(a, "English text.", new String[] {
        "english", "text" });
    a.close();
  }
  
  /**
   * Test that custom stopwords work, and are not case-sensitive.
   */
  public void testCustomStopwords() throws Exception {
    CharArraySet set = new CharArraySet(asSet("the", "and", "a"), false);
    ArabicAnalyzer a = new ArabicAnalyzer(set);
    assertAnalyzesTo(a, "The quick brown fox.", new String[] { "quick",
        "brown", "fox" });
    a.close();
  }
  
  public void testWithStemExclusionSet() throws IOException {
    CharArraySet set = new CharArraySet(asSet("ساهدهات"), false);
    ArabicAnalyzer a = new ArabicAnalyzer(CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(a, "كبيرة the quick ساهدهات", new String[] { "كبير","the", "quick", "ساهدهات" });
    assertAnalyzesTo(a, "كبيرة the quick ساهدهات", new String[] { "كبير","the", "quick", "ساهدهات" });
    a.close();
    
    a = new ArabicAnalyzer(CharArraySet.EMPTY_SET, CharArraySet.EMPTY_SET);
    assertAnalyzesTo(a, "كبيرة the quick ساهدهات", new String[] { "كبير","the", "quick", "ساهد" });
    assertAnalyzesTo(a, "كبيرة the quick ساهدهات", new String[] { "كبير","the", "quick", "ساهد" });
    a.close();
  }
  
  /**
   * test we fold digits to latin-1
   */
  public void testDigits() throws Exception {
    ArabicAnalyzer a = new ArabicAnalyzer();
    checkOneTerm(a, "١٢٣٤", "1234");
    a.close();
  }
  
  /**
   * test that we don't fold digits for back compat behavior
   * @deprecated remove this test in lucene 7
   */
  @Deprecated
  public void testDigitsBackCompat() throws Exception {
    ArabicAnalyzer a = new ArabicAnalyzer();
    a.setVersion(Version.LUCENE_5_3_0);
    checkOneTerm(a, "١٢٣٤", "١٢٣٤");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    ArabicAnalyzer a = new ArabicAnalyzer();
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }
}
