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
package org.apache.lucene.analysis.eu;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;

public class TestBasqueAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new BasqueAnalyzer().close();
  }

  /** test stopwords and stemming */
  public void testBasics() throws IOException {
    Analyzer a = new BasqueAnalyzer();
    // stemming
    checkOneTerm(a, "zaldi", "zaldi");
    checkOneTerm(a, "zaldiak", "zaldi");
    // stopword
    assertAnalyzesTo(a, "izan", new String[] {});
    a.close();
  }

  /** test use of exclusion set */
  public void testExclude() throws IOException {
    CharArraySet exclusionSet = new CharArraySet(asSet("zaldiak"), false);
    Analyzer a = new BasqueAnalyzer(BasqueAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTerm(a, "zaldiak", "zaldiak");
    checkOneTerm(a, "mendiari", "mendi");
    a.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new BasqueAnalyzer();
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }
}
