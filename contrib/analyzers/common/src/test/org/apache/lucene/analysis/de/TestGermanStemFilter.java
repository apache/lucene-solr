package org.apache.lucene.analysis.de;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Test the German stemmer. The stemming algorithm is known to work less 
 * than perfect, as it doesn't use any word lists with exceptions. We 
 * also check some of the cases where the algorithm is wrong.
 *
 */
public class TestGermanStemFilter extends BaseTokenStreamTestCase {

  public void testStemming() throws Exception {
    // read test cases from external file:
    File dataDir = new File(System.getProperty("dataDir", "./bin"));
    File testFile = new File(dataDir, "org/apache/lucene/analysis/de/data.txt");
    FileInputStream fis = new FileInputStream(testFile);
    InputStreamReader isr = new InputStreamReader(fis, "iso-8859-1");
    BufferedReader breader = new BufferedReader(isr);
    while(true) {
      String line = breader.readLine();
      if (line == null)
        break;
      line = line.trim();
      if (line.startsWith("#") || line.equals(""))
        continue;    // ignore comments and empty lines
      String[] parts = line.split(";");
      //System.out.println(parts[0] + " -- " + parts[1]);
      check(parts[0], parts[1]);
    }
    breader.close();
    isr.close();
    fis.close();
  }
  
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new GermanAnalyzer();
    checkReuse(a, "Tisch", "tisch");
    checkReuse(a, "Tische", "tisch");
    checkReuse(a, "Tischen", "tisch");
  }
  
  /**
   * subclass that acts just like whitespace analyzer for testing
   */
  private class GermanSubclassAnalyzer extends GermanAnalyzer {
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new WhitespaceTokenizer(reader);
    }
  }
  
  public void testLUCENE1678BWComp() throws Exception {
    checkReuse(new GermanSubclassAnalyzer(), "Tischen", "Tischen");
  }

  /* 
   * Test that changes to the exclusion table are applied immediately
   * when using reusable token streams.
   */
  public void testExclusionTableReuse() throws Exception {
    GermanAnalyzer a = new GermanAnalyzer();
    checkReuse(a, "tischen", "tisch");
    a.setStemExclusionTable(new String[] { "tischen" });
    checkReuse(a, "tischen", "tischen");
  }
  
  private void check(final String input, final String expected) throws Exception {
    checkOneTerm(new GermanAnalyzer(), input, expected);
  }
  
  private void checkReuse(Analyzer a, String input, String expected) throws Exception {
    checkOneTermReuse(a, input, expected);
  }
}
