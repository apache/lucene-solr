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
package org.apache.lucene.analysis.hi;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Tests the HindiAnalyzer
 */
public class TestHindiAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new HindiAnalyzer().close();
  }
  
  public void testBasics() throws Exception {
    Analyzer a = new HindiAnalyzer();
    // two ways to write 'hindi' itself.
    checkOneTerm(a, "हिन्दी", "हिंद");
    checkOneTerm(a, "हिंदी", "हिंद");
    a.close();
  }
  
  public void testExclusionSet() throws Exception {
    CharArraySet exclusionSet = new CharArraySet( asSet("हिंदी"), false);
    Analyzer a = new HindiAnalyzer( 
        HindiAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTerm(a, "हिंदी", "हिंदी");
    a.close();
  }
  
  /**
   * test we fold digits to latin-1
   */
  public void testDigits() throws Exception {
    HindiAnalyzer a = new HindiAnalyzer();
    checkOneTerm(a, "१२३४", "1234");
    a.close();
  }
  
  /**
   * test that we don't fold digits for back compat behavior
   * @deprecated remove this test in lucene 7
   */
  @Deprecated
  public void testDigitsBackCompat() throws Exception {
    HindiAnalyzer a = new HindiAnalyzer();
    a.setVersion(Version.LUCENE_5_3_0);
    checkOneTerm(a, "१२३४", "१२३४");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new HindiAnalyzer();
    checkRandomData(random(), analyzer, 1000*RANDOM_MULTIPLIER);
    analyzer.close();
  }

  public void testBackcompat40() throws IOException {
    HindiAnalyzer a = new HindiAnalyzer();
    a.setVersion(Version.LUCENE_4_6_1);
    // this is just a test to see the correct unicode version is being used, not actually testing hebrew
    assertAnalyzesTo(a, "א\"א", new String[] {"א", "א"});
  }
}
