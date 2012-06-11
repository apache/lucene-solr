package org.apache.lucene.analysis.synonym;

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

import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.junit.Test;

/**
 * Tests parser for the Solr synonyms format
 * @lucene.experimental
 */
public class TestSolrSynonymParser extends BaseTokenStreamTestCase {
  
  /** Tests some simple examples from the solr wiki */
  public void testSimple() throws Exception {
    String testFile = 
    "i-pod, ipod, ipoooood\n" + 
    "foo => foo bar\n" +
    "foo => baz\n" +
    "this test, that testing";
    
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new MockAnalyzer(random()));
    parser.add(new StringReader(testFile));
    final SynonymMap map = parser.build();
    
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, true);
        return new TokenStreamComponents(tokenizer, new SynonymFilter(tokenizer, map, true));
      }
    };
    
    assertAnalyzesTo(analyzer, "ball", 
        new String[] { "ball" },
        new int[] { 1 });
    
    assertAnalyzesTo(analyzer, "i-pod",
        new String[] { "i-pod", "ipod", "ipoooood" },
        new int[] { 1, 0, 0 });
    
    assertAnalyzesTo(analyzer, "foo",
        new String[] { "foo", "baz", "bar" },
        new int[] { 1, 0, 1 });
    
    assertAnalyzesTo(analyzer, "this test",
        new String[] { "this", "that", "test", "testing" },
        new int[] { 1, 0, 1, 0 });
  }
  
  /** parse a syn file with bad syntax */
  @Test(expected=ParseException.class)
  public void testInvalidDoubleMap() throws Exception {
    String testFile = "a => b => c"; 
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new MockAnalyzer(random()));
    parser.add(new StringReader(testFile));
  }
  
  /** parse a syn file with bad syntax */
  @Test(expected=ParseException.class)
  public void testInvalidAnalyzesToNothingOutput() throws Exception {
    String testFile = "a => 1"; 
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new MockAnalyzer(random(), MockTokenizer.SIMPLE, false));
    parser.add(new StringReader(testFile));
  }
  
  /** parse a syn file with bad syntax */
  @Test(expected=ParseException.class)
  public void testInvalidAnalyzesToNothingInput() throws Exception {
    String testFile = "1 => a"; 
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new MockAnalyzer(random(), MockTokenizer.SIMPLE, false));
    parser.add(new StringReader(testFile));
  }
  
  /** parse a syn file with bad syntax */
  @Test(expected=ParseException.class)
  public void testInvalidPositionsInput() throws Exception {
    String testFile = "testola => the test";
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new EnglishAnalyzer(TEST_VERSION_CURRENT));
    parser.add(new StringReader(testFile));
  }
  
  /** parse a syn file with bad syntax */
  @Test(expected=ParseException.class)
  public void testInvalidPositionsOutput() throws Exception {
    String testFile = "the test => testola";
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new EnglishAnalyzer(TEST_VERSION_CURRENT));
    parser.add(new StringReader(testFile));
  }
  
  /** parse a syn file with some escaped syntax chars */
  public void testEscapedStuff() throws Exception {
    String testFile = 
      "a\\=>a => b\\=>b\n" +
      "a\\,a => b\\,b";
    SolrSynonymParser parser = new SolrSynonymParser(true, true, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    parser.add(new StringReader(testFile));
    final SynonymMap map = parser.build();
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
        return new TokenStreamComponents(tokenizer, new SynonymFilter(tokenizer, map, false));
      }
    };
    
    assertAnalyzesTo(analyzer, "ball", 
        new String[] { "ball" },
        new int[] { 1 });
    
    assertAnalyzesTo(analyzer, "a=>a",
        new String[] { "b=>b" },
        new int[] { 1 });
    
    assertAnalyzesTo(analyzer, "a,a",
        new String[] { "b,b" },
        new int[] { 1 });
  }
}
