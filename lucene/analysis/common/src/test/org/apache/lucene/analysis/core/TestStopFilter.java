package org.apache.lucene.analysis.core;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.English;
import org.apache.lucene.util.Version;


public class TestStopFilter extends BaseTokenStreamTestCase {
  
  // other StopFilter functionality is already tested by TestStopAnalyzer

  public void testExactCase() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    CharArraySet stopWords = new CharArraySet(TEST_VERSION_CURRENT, asSet("is", "the", "Time"), false);
    TokenStream stream = new StopFilter(TEST_VERSION_CURRENT, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false), stopWords);
    assertTokenStreamContents(stream, new String[] { "Now", "The" });
  }

  public void testStopFilt() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    String[] stopWords = new String[] { "is", "the", "Time" };
    CharArraySet stopSet = StopFilter.makeStopSet(TEST_VERSION_CURRENT, stopWords);
    TokenStream stream = new StopFilter(TEST_VERSION_CURRENT, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false), stopSet);
    assertTokenStreamContents(stream, new String[] { "Now", "The" });
  }

  /**
   * Test Position increments applied by StopFilter with and without enabling this option.
   */
  public void testStopPositons() throws IOException {
    StringBuilder sb = new StringBuilder();
    ArrayList<String> a = new ArrayList<String>();
    for (int i=0; i<20; i++) {
      String w = English.intToEnglish(i).trim();
      sb.append(w).append(" ");
      if (i%3 != 0) a.add(w);
    }
    log(sb.toString());
    String stopWords[] = a.toArray(new String[0]);
    for (int i=0; i<a.size(); i++) log("Stop: "+stopWords[i]);
    CharArraySet stopSet = StopFilter.makeStopSet(TEST_VERSION_CURRENT, stopWords);
    // with increments
    StringReader reader = new StringReader(sb.toString());
    StopFilter stpf = new StopFilter(Version.LUCENE_40, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false), stopSet);
    doTestStopPositons(stpf,true);
    // without increments
    reader = new StringReader(sb.toString());
    stpf = new StopFilter(TEST_VERSION_CURRENT, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false), stopSet);
    doTestStopPositons(stpf,false);
    // with increments, concatenating two stop filters
    ArrayList<String> a0 = new ArrayList<String>();
    ArrayList<String> a1 = new ArrayList<String>();
    for (int i=0; i<a.size(); i++) {
      if (i%2==0) { 
        a0.add(a.get(i));
      } else {
        a1.add(a.get(i));
      }
    }
    String stopWords0[] =  a0.toArray(new String[0]);
    for (int i=0; i<a0.size(); i++) log("Stop0: "+stopWords0[i]);
    String stopWords1[] =  a1.toArray(new String[0]);
    for (int i=0; i<a1.size(); i++) log("Stop1: "+stopWords1[i]);
    CharArraySet stopSet0 = StopFilter.makeStopSet(TEST_VERSION_CURRENT, stopWords0);
    CharArraySet stopSet1 = StopFilter.makeStopSet(TEST_VERSION_CURRENT, stopWords1);
    reader = new StringReader(sb.toString());
    StopFilter stpf0 = new StopFilter(TEST_VERSION_CURRENT, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false), stopSet0); // first part of the set
    stpf0.setEnablePositionIncrements(true);
    StopFilter stpf01 = new StopFilter(TEST_VERSION_CURRENT, stpf0, stopSet1); // two stop filters concatenated!
    doTestStopPositons(stpf01,true);
  }
  
  private void doTestStopPositons(StopFilter stpf, boolean enableIcrements) throws IOException {
    log("---> test with enable-increments-"+(enableIcrements?"enabled":"disabled"));
    stpf.setEnablePositionIncrements(enableIcrements);
    CharTermAttribute termAtt = stpf.getAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncrAtt = stpf.getAttribute(PositionIncrementAttribute.class);
    stpf.reset();
    for (int i=0; i<20; i+=3) {
      assertTrue(stpf.incrementToken());
      log("Token "+i+": "+stpf);
      String w = English.intToEnglish(i).trim();
      assertEquals("expecting token "+i+" to be "+w,w,termAtt.toString());
      assertEquals("all but first token must have position increment of 3",enableIcrements?(i==0?1:3):1,posIncrAtt.getPositionIncrement());
    }
    assertFalse(stpf.incrementToken());
    stpf.end();
    stpf.close();
  }
  
  // print debug info depending on VERBOSE
  private static void log(String s) {
    if (VERBOSE) {
      System.out.println(s);
    }
  }
  
  // stupid filter that inserts synonym of 'hte' for 'the'
  private class MockSynonymFilter extends TokenFilter {
    State bufferedState;
    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

    MockSynonymFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (bufferedState != null) {
        restoreState(bufferedState);
        posIncAtt.setPositionIncrement(0);
        termAtt.setEmpty().append("hte");
        bufferedState = null;
        return true;
      } else if (input.incrementToken()) {
        if (termAtt.toString().equals("the")) {
          bufferedState = captureState();
        }
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      bufferedState = null;
    }
  }
  
  public void testFirstPosInc() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        TokenFilter filter = new MockSynonymFilter(tokenizer);
        StopFilter stopfilter = new StopFilter(TEST_VERSION_CURRENT, filter, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        stopfilter.setEnablePositionIncrements(false);
        return new TokenStreamComponents(tokenizer, stopfilter);
      }
    };
    
    assertAnalyzesTo(analyzer, "the quick brown fox",
        new String[] { "hte", "quick", "brown", "fox" },
        new int[] { 1, 1, 1, 1} );
  }
}
