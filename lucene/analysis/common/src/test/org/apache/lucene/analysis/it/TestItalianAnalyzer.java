package org.apache.lucene.analysis.it;

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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

public class TestItalianAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new ItalianAnalyzer(TEST_VERSION_CURRENT);
  }
  
  /** test stopwords and stemming */
  public void testBasics() throws IOException {
    Analyzer a = new ItalianAnalyzer(TEST_VERSION_CURRENT);
    // stemming
    checkOneTermReuse(a, "abbandonata", "abbandonat");
    checkOneTermReuse(a, "abbandonati", "abbandonat");
    // stopword
    assertAnalyzesTo(a, "dallo", new String[] {});
  }
  
  /** test use of exclusion set */
  public void testExclude() throws IOException {
    CharArraySet exclusionSet = new CharArraySet(TEST_VERSION_CURRENT, asSet("abbandonata"), false);
    Analyzer a = new ItalianAnalyzer(TEST_VERSION_CURRENT, 
        ItalianAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTermReuse(a, "abbandonata", "abbandonata");
    checkOneTermReuse(a, "abbandonati", "abbandonat");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new ItalianAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
  
  /** test that the elisionfilter is working */
  public void testContractions() throws IOException {
    Analyzer a = new ItalianAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesTo(a, "dell'Italia", new String[] { "ital" });
    assertAnalyzesTo(a, "l'Italiano", new String[] { "italian" });
  }
  
  /** test that we don't enable this before 3.2*/
  public void testContractionsBackwards() throws IOException {
    Analyzer a = new ItalianAnalyzer(Version.LUCENE_31);
    assertAnalyzesTo(a, "dell'Italia", new String[] { "dell'ital" });
    assertAnalyzesTo(a, "l'Italiano", new String[] { "l'ital" });
  }
}
