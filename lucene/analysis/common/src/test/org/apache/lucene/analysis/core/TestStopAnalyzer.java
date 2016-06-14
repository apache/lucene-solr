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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

public class TestStopAnalyzer extends BaseTokenStreamTestCase {
  
  private StopAnalyzer stop;
  private Set<Object> inValidTokens = new HashSet<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    Iterator<?> it = StopAnalyzer.ENGLISH_STOP_WORDS_SET.iterator();
    while(it.hasNext()) {
      inValidTokens.add(it.next());
    }
    stop = new StopAnalyzer();
  }
  
  @Override
  public void tearDown() throws Exception {
    stop.close();
    super.tearDown();
  }

  public void testDefaults() throws IOException {
    assertTrue(stop != null);
    try (TokenStream stream = stop.tokenStream("test", "This is a test of the english stop analyzer")) {
      assertTrue(stream != null);
      CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
      stream.reset();
    
      while (stream.incrementToken()) {
        assertFalse(inValidTokens.contains(termAtt.toString()));
      }
      stream.end();
    }
  }

  public void testStopList() throws IOException {
    CharArraySet stopWordsSet = new CharArraySet(asSet("good", "test", "analyzer"), false);
    StopAnalyzer newStop = new StopAnalyzer(stopWordsSet);
    try (TokenStream stream = newStop.tokenStream("test", "This is a good test of the english stop analyzer")) {
      assertNotNull(stream);
      CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    
      stream.reset();
      while (stream.incrementToken()) {
        String text = termAtt.toString();
        assertFalse(stopWordsSet.contains(text));
      }
      stream.end();
    }
    newStop.close();
  }

  public void testStopListPositions() throws IOException {
    CharArraySet stopWordsSet = new CharArraySet(asSet("good", "test", "analyzer"), false);
    StopAnalyzer newStop = new StopAnalyzer(stopWordsSet);
    String s =             "This is a good test of the english stop analyzer with positions";
    int expectedIncr[] =  { 1,   1, 1,          3, 1,  1,      1,            2,   1};
    try (TokenStream stream = newStop.tokenStream("test", s)) {
      assertNotNull(stream);
      int i = 0;
      CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
      PositionIncrementAttribute posIncrAtt = stream.addAttribute(PositionIncrementAttribute.class);

      stream.reset();
      while (stream.incrementToken()) {
        String text = termAtt.toString();
        assertFalse(stopWordsSet.contains(text));
        assertEquals(expectedIncr[i++],posIncrAtt.getPositionIncrement());
      }
      stream.end();
    }
    newStop.close();
  }

}
