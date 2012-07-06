package org.apache.lucene.analysis.cz;

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
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Test the CzechAnalyzer
 * 
 * Before Lucene 3.1, CzechAnalyzer was a StandardAnalyzer with a custom 
 * stopword list. As of 3.1 it also includes a stemmer.
 *
 */
public class TestCzechAnalyzer extends BaseTokenStreamTestCase {
  /**
   * @deprecated (3.1) Remove this test when support for 3.0 indexes is no longer needed.
   */
  @Deprecated
  public void testStopWordLegacy() throws Exception {
    assertAnalyzesTo(new CzechAnalyzer(Version.LUCENE_30), "Pokud mluvime o volnem", 
        new String[] { "mluvime", "volnem" });
  }
  
  public void testStopWord() throws Exception {
    assertAnalyzesTo(new CzechAnalyzer(TEST_VERSION_CURRENT), "Pokud mluvime o volnem", 
        new String[] { "mluvim", "voln" });
  }
  
  /**
   * @deprecated (3.1) Remove this test when support for 3.0 indexes is no longer needed.
   */
  @Deprecated
  public void testReusableTokenStreamLegacy() throws Exception {
    Analyzer analyzer = new CzechAnalyzer(Version.LUCENE_30);
    assertAnalyzesToReuse(analyzer, "Pokud mluvime o volnem", new String[] { "mluvime", "volnem" });
    assertAnalyzesToReuse(analyzer, "Česká Republika", new String[] { "česká", "republika" });
  }
  
  public void testReusableTokenStream() throws Exception {
    Analyzer analyzer = new CzechAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesToReuse(analyzer, "Pokud mluvime o volnem", new String[] { "mluvim", "voln" });
    assertAnalyzesToReuse(analyzer, "Česká Republika", new String[] { "česk", "republik" });
  }

  public void testWithStemExclusionSet() throws IOException{
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
    set.add("hole");
    CzechAnalyzer cz = new CzechAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(cz, "hole desek", new String[] {"hole", "desk"});
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new CzechAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
}
