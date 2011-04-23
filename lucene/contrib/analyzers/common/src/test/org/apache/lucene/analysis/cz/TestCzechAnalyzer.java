package org.apache.lucene.analysis.cz;

/**
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
import java.io.InputStream;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
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
   * @deprecated Remove this test when support for 3.0 indexes is no longer needed.
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
   * @deprecated Remove this test when support for 3.0 indexes is no longer needed.
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

  /**
   * An input stream that always throws IOException for testing.
   * @deprecated Remove this class when the loadStopWords method is removed.
   */
  @Deprecated
  private class UnreliableInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      throw new IOException();
    }
  }
  
  /**
   * The loadStopWords method does not throw IOException on error,
   * instead previously it set the stoptable to null (versus empty)
   * this would cause a NPE when it is time to create the StopFilter.
   * @deprecated Remove this test when the loadStopWords method is removed.
   */
  @Deprecated
  public void testInvalidStopWordFile() throws Exception {
    CzechAnalyzer cz = new CzechAnalyzer(Version.LUCENE_30);
    cz.loadStopWords(new UnreliableInputStream(), "UTF-8");
    assertAnalyzesTo(cz, "Pokud mluvime o volnem",
        new String[] { "pokud", "mluvime", "o", "volnem" });
  }
  
  /** 
   * Test that changes to the stop table via loadStopWords are applied immediately
   * when using reusable token streams.
   * @deprecated Remove this test when the loadStopWords method is removed.
   */
  @Deprecated
  public void testStopWordFileReuse() throws Exception {
    CzechAnalyzer cz = new CzechAnalyzer(Version.LUCENE_30);
    assertAnalyzesToReuse(cz, "Česká Republika", 
      new String[] { "česká", "republika" });
    
    InputStream stopwords = getClass().getResourceAsStream("customStopWordFile.txt");
    cz.loadStopWords(stopwords, "UTF-8");
    
    assertAnalyzesToReuse(cz, "Česká Republika", new String[] { "česká" });
  }
  
  public void testWithStemExclusionSet() throws IOException{
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
    set.add("hole");
    CzechAnalyzer cz = new CzechAnalyzer(TEST_VERSION_CURRENT, CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(cz, "hole desek", new String[] {"hole", "desk"});
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random, new CzechAnalyzer(TEST_VERSION_CURRENT), 10000*RANDOM_MULTIPLIER);
  }
}
