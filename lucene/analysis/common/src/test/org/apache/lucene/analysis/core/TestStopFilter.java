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
package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.English;

public class TestStopFilter extends BaseTokenStreamTestCase {
  
  // other StopFilter functionality is already tested by TestStopAnalyzer

  public void testExactCase() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    CharArraySet stopWords = new CharArraySet(asSet("is", "the", "Time"), false);
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(reader);
    TokenStream stream = new StopFilter(in, stopWords);
    assertTokenStreamContents(stream, new String[] { "Now", "The" });
  }

  public void testStopFilt() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    String[] stopWords = new String[] { "is", "the", "Time" };
    CharArraySet stopSet = StopFilter.makeStopSet(stopWords);
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(reader);
    TokenStream stream = new StopFilter(in, stopSet);
    assertTokenStreamContents(stream, new String[] { "Now", "The" });
  }

  /**
   * Test Position increments applied by StopFilter with and without enabling this option.
   */
  public void testStopPositons() throws IOException {
    StringBuilder sb = new StringBuilder();
    ArrayList<String> a = new ArrayList<>();
    for (int i=0; i<20; i++) {
      String w = English.intToEnglish(i).trim();
      sb.append(w).append(" ");
      if (i%3 != 0) a.add(w);
    }
    log(sb.toString());
    String stopWords[] = a.toArray(new String[0]);
    for (int i=0; i<a.size(); i++) log("Stop: "+stopWords[i]);
    CharArraySet stopSet = StopFilter.makeStopSet(stopWords);
    // with increments
    StringReader reader = new StringReader(sb.toString());
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(reader);
    StopFilter stpf = new StopFilter(in, stopSet);
    doTestStopPositons(stpf);
    // with increments, concatenating two stop filters
    ArrayList<String> a0 = new ArrayList<>();
    ArrayList<String> a1 = new ArrayList<>();
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
    CharArraySet stopSet0 = StopFilter.makeStopSet(stopWords0);
    CharArraySet stopSet1 = StopFilter.makeStopSet(stopWords1);
    reader = new StringReader(sb.toString());
    final MockTokenizer in1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in1.setReader(reader);
    StopFilter stpf0 = new StopFilter(in1, stopSet0); // first part of the set
    StopFilter stpf01 = new StopFilter(stpf0, stopSet1); // two stop filters concatenated!
    doTestStopPositons(stpf01);
  }

  // LUCENE-3849: make sure after .end() we see the "ending" posInc
  public void testEndStopword() throws Exception {
    CharArraySet stopSet = StopFilter.makeStopSet("of");
    final MockTokenizer in = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    in.setReader(new StringReader("test of"));
    StopFilter stpf = new StopFilter(in, stopSet);
    assertTokenStreamContents(stpf, new String[] { "test" },
                              new int[] {0},
                              new int[] {4},
                              null,
                              new int[] {1},
                              null,
                              7,
                              1,
                              null,
                              true);    
  }
  
  private void doTestStopPositons(StopFilter stpf) throws IOException {
    CharTermAttribute termAtt = stpf.getAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncrAtt = stpf.getAttribute(PositionIncrementAttribute.class);
    stpf.reset();
    for (int i=0; i<20; i+=3) {
      assertTrue(stpf.incrementToken());
      log("Token "+i+": "+stpf);
      String w = English.intToEnglish(i).trim();
      assertEquals("expecting token "+i+" to be "+w,w,termAtt.toString());
      assertEquals("all but first token must have position increment of 3",i==0?1:3,posIncrAtt.getPositionIncrement());
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

}
