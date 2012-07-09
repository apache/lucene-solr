package org.apache.lucene.analysis.hi;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.util.CharArraySet;

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

/**
 * Tests the HindiAnalyzer
 */
public class TestHindiAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new HindiAnalyzer(TEST_VERSION_CURRENT);
  }
  
  public void testBasics() throws Exception {
    Analyzer a = new HindiAnalyzer(TEST_VERSION_CURRENT);
    // two ways to write 'hindi' itself.
    checkOneTermReuse(a, "हिन्दी", "हिंद");
    checkOneTermReuse(a, "हिंदी", "हिंद");
  }
  
  public void testExclusionSet() throws Exception {
    CharArraySet exclusionSet = new CharArraySet(TEST_VERSION_CURRENT, asSet("हिंदी"), false);
    Analyzer a = new HindiAnalyzer(TEST_VERSION_CURRENT, 
        HindiAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTermReuse(a, "हिंदी", "हिंदी");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new HindiAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
}
