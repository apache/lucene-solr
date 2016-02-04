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
package org.apache.lucene.analysis.en;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

public class TestEnglishAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new EnglishAnalyzer().close();
  }
  
  /** test stopwords and stemming */
  public void testBasics() throws IOException {
    Analyzer a = new EnglishAnalyzer();
    // stemming
    checkOneTerm(a, "books", "book");
    checkOneTerm(a, "book", "book");
    // stopword
    assertAnalyzesTo(a, "the", new String[] {});
    // possessive removal
    checkOneTerm(a, "steven's", "steven");
    checkOneTerm(a, "steven\u2019s", "steven");
    checkOneTerm(a, "steven\uFF07s", "steven");
    a.close();
  }
  
  /** test use of exclusion set */
  public void testExclude() throws IOException {
    CharArraySet exclusionSet = new CharArraySet( asSet("books"), false);
    Analyzer a = new EnglishAnalyzer( 
        EnglishAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTerm(a, "books", "books");
    checkOneTerm(a, "book", "book");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new EnglishAnalyzer();
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }

  public void testBackcompat40() throws IOException {
    EnglishAnalyzer a = new EnglishAnalyzer();
    a.setVersion(Version.LUCENE_4_6_1);
    // this is just a test to see the correct unicode version is being used, not actually testing hebrew
    assertAnalyzesTo(a, "א\"א", new String[] {"א", "א"});
  }
}
