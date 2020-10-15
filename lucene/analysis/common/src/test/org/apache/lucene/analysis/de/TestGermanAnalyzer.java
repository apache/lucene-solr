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
package org.apache.lucene.analysis.de;


import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;

public class TestGermanAnalyzer extends BaseTokenStreamTestCase {
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new GermanAnalyzer();
    checkOneTerm(a, "Tisch", "tisch");
    checkOneTerm(a, "Tische", "tisch");
    checkOneTerm(a, "Tischen", "tisch");
    a.close();
  }
  
  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet( 1, true);
    set.add("fischen");
    final Tokenizer in = new LetterTokenizer();
    in.setReader(new StringReader("Fischen Trinken"));
    GermanStemFilter filter = new GermanStemFilter(
        new SetKeywordMarkerFilter(new LowerCaseFilter(in), set));
    assertTokenStreamContents(filter, new String[] { "fischen", "trink" });
  }

  public void testStemExclusionTable() throws Exception {
    GermanAnalyzer a = new GermanAnalyzer( CharArraySet.EMPTY_SET, 
        new CharArraySet( asSet("tischen"), false));
    checkOneTerm(a, "tischen", "tischen");
    a.close();
  }
  
  /** test some features of the new snowball filter
   * these only pass with LATEST, not if you use o.a.l.a.de.GermanStemmer
   */
  public void testGermanSpecials() throws Exception {
    GermanAnalyzer a = new GermanAnalyzer();
    // a/o/u + e is equivalent to the umlaut form
    checkOneTerm(a, "Schaltflächen", "schaltflach");
    checkOneTerm(a, "Schaltflaechen", "schaltflach");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    GermanAnalyzer a = new GermanAnalyzer();
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }
}
