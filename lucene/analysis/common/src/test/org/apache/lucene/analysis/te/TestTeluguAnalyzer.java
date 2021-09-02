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
package org.apache.lucene.analysis.te;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;

public class TestTeluguAnalyzer extends BaseTokenStreamTestCase {

  /** This test fails with NPE when the stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new TeluguAnalyzer().close();
  }

  public void testBasics() throws Exception {
    Analyzer a = new TeluguAnalyzer();
    // two ways to write oo letter.
    checkOneTerm(a, "ఒౕనమాల", "ఓనమాల");
    a.close();
  }

  public void testExclusionSet() throws Exception {
    CharArraySet exclusionSet = new CharArraySet(asSet("వస్తువులు"), false);
    Analyzer a = new TeluguAnalyzer(TeluguAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTerm(a, "వస్తువులు", "వస్తుమలు");
    a.close();
  }

  /** test we fold digits to latin-1 */
  public void testDigits() throws Exception {
    TeluguAnalyzer a = new TeluguAnalyzer();
    checkOneTerm(a, "౧౨౩౪", "1234");
    a.close();
  }

  /** Send some random strings to the analyzer */
  public void testRandomStrings() throws Exception {
    TeluguAnalyzer analyzer = new TeluguAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
