package org.apache.lucene.analysis.de;

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
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

public class TestGermanAnalyzer extends BaseTokenStreamTestCase {
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new GermanAnalyzer(TEST_VERSION_CURRENT);
    checkOneTermReuse(a, "Tisch", "tisch");
    checkOneTermReuse(a, "Tische", "tisch");
    checkOneTermReuse(a, "Tischen", "tisch");
  }
  
  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
    set.add("fischen");
    GermanStemFilter filter = new GermanStemFilter(
        new SetKeywordMarkerFilter(new LowerCaseTokenizer(TEST_VERSION_CURRENT, new StringReader( 
            "Fischen Trinken")), set));
    assertTokenStreamContents(filter, new String[] { "fischen", "trink" });
  }

  public void testStemExclusionTable() throws Exception {
    GermanAnalyzer a = new GermanAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET, 
        new CharArraySet(TEST_VERSION_CURRENT, asSet("tischen"), false));
    checkOneTermReuse(a, "tischen", "tischen");
  }
  
  /** test some features of the new snowball filter
   * these only pass with LUCENE_CURRENT, not if you use o.a.l.a.de.GermanStemmer
   */
  public void testGermanSpecials() throws Exception {
    GermanAnalyzer a = new GermanAnalyzer(TEST_VERSION_CURRENT);
    // a/o/u + e is equivalent to the umlaut form
    checkOneTermReuse(a, "Schaltflächen", "schaltflach");
    checkOneTermReuse(a, "Schaltflaechen", "schaltflach");
    // here they are with the old stemmer
    a = new GermanAnalyzer(Version.LUCENE_30);
    checkOneTermReuse(a, "Schaltflächen", "schaltflach");
    checkOneTermReuse(a, "Schaltflaechen", "schaltflaech");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new GermanAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
}
