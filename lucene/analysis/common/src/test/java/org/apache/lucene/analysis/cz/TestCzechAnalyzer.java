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
package org.apache.lucene.analysis.cz;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;

/**
 * Test the CzechAnalyzer
 * 
 * Before Lucene 3.1, CzechAnalyzer was a StandardAnalyzer with a custom 
 * stopword list. As of 3.1 it also includes a stemmer.
 *
 */
public class TestCzechAnalyzer extends BaseTokenStreamTestCase {
  
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new CzechAnalyzer().close();
  }
  
  public void testStopWord() throws Exception {
    Analyzer analyzer = new CzechAnalyzer();
    assertAnalyzesTo(analyzer, "Pokud mluvime o volnem", 
        new String[] { "mluvim", "voln" });
    analyzer.close();
  }
  
  public void testReusableTokenStream() throws Exception {
    Analyzer analyzer = new CzechAnalyzer();
    assertAnalyzesTo(analyzer, "Pokud mluvime o volnem", new String[] { "mluvim", "voln" });
    assertAnalyzesTo(analyzer, "Česká Republika", new String[] { "česk", "republik" });
    analyzer.close();
  }

  public void testWithStemExclusionSet() throws IOException{
    CharArraySet set = new CharArraySet(1, true);
    set.add("hole");
    CzechAnalyzer cz = new CzechAnalyzer(CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(cz, "hole desek", new String[] {"hole", "desk"});
    cz.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new CzechAnalyzer();
    checkRandomData(random(), analyzer, 1000*RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
