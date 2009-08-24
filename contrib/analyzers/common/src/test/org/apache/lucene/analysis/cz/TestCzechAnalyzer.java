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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

/**
 * Test the CzechAnalyzer
 * 
 * CzechAnalyzer is like a StandardAnalyzer with a custom stopword list.
 *
 */
public class TestCzechAnalyzer extends BaseTokenStreamTestCase {
  File dataDir = new File(System.getProperty("dataDir", "./bin"));
  File customStopFile = new File(dataDir, "org/apache/lucene/analysis/cz/customStopWordFile.txt");
  
  public void testStopWord() throws Exception {
    assertAnalyzesTo(new CzechAnalyzer(), "Pokud mluvime o volnem", new String[] { "mluvime", "volnem" });
  }
    
  public void testReusableTokenStream() throws Exception {
    Analyzer analyzer = new CzechAnalyzer();
    assertAnalyzesToReuse(analyzer, "Pokud mluvime o volnem", new String[] { "mluvime", "volnem" });
    assertAnalyzesToReuse(analyzer, "Česká Republika", new String[] { "česká", "republika" });
  }

  /*
   * An input stream that always throws IOException for testing.
   */
  private class UnreliableInputStream extends InputStream {
    public int read() throws IOException {
      throw new IOException();
    }
  }
  
  /*
   * The loadStopWords method does not throw IOException on error,
   * instead previously it set the stoptable to null (versus empty)
   * this would cause a NPE when it is time to create the StopFilter.
   */
  public void testInvalidStopWordFile() throws Exception {
    CzechAnalyzer cz = new CzechAnalyzer();
    cz.loadStopWords(new UnreliableInputStream(), "UTF-8");
    assertAnalyzesTo(cz, "Pokud mluvime o volnem",
        new String[] { "pokud", "mluvime", "o", "volnem" });
  }
  
  /* 
   * Test that changes to the stop table via loadStopWords are applied immediately
   * when using reusable token streams.
   */
  public void testStopWordFileReuse() throws Exception {
    CzechAnalyzer cz = new CzechAnalyzer();
    assertAnalyzesToReuse(cz, "Česká Republika", 
      new String[] { "česká", "republika" });
    
    InputStream stopwords = new FileInputStream(customStopFile);
    cz.loadStopWords(stopwords, "UTF-8");
    
    assertAnalyzesToReuse(cz, "Česká Republika", new String[] { "česká" });
  }

}
