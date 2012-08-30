package org.apache.lucene.analysis.core;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import java.io.StringReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

public class TestStopAnalyzer extends BaseTokenStreamTestCase {
  
  private StopAnalyzer stop = new StopAnalyzer(TEST_VERSION_CURRENT);
  private Set<Object> inValidTokens = new HashSet<Object>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    Iterator<?> it = StopAnalyzer.ENGLISH_STOP_WORDS_SET.iterator();
    while(it.hasNext()) {
      inValidTokens.add(it.next());
    }
  }

  public void testDefaults() throws IOException {
    assertTrue(stop != null);
    StringReader reader = new StringReader("This is a test of the english stop analyzer");
    TokenStream stream = stop.tokenStream("test", reader);
    assertTrue(stream != null);
    CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    stream.reset();
    
    while (stream.incrementToken()) {
      assertFalse(inValidTokens.contains(termAtt.toString()));
    }
  }

  public void testStopList() throws IOException {
    CharArraySet stopWordsSet = new CharArraySet(TEST_VERSION_CURRENT, asSet("good", "test", "analyzer"), false);
    StopAnalyzer newStop = new StopAnalyzer(Version.LUCENE_40, stopWordsSet);
    StringReader reader = new StringReader("This is a good test of the english stop analyzer");
    TokenStream stream = newStop.tokenStream("test", reader);
    assertNotNull(stream);
    CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    
    stream.reset();
    while (stream.incrementToken()) {
      String text = termAtt.toString();
      assertFalse(stopWordsSet.contains(text));
    }
  }

  public void testStopListPositions() throws IOException {
    CharArraySet stopWordsSet = new CharArraySet(TEST_VERSION_CURRENT, asSet("good", "test", "analyzer"), false);
    StopAnalyzer newStop = new StopAnalyzer(TEST_VERSION_CURRENT, stopWordsSet);
    StringReader reader = new StringReader("This is a good test of the english stop analyzer with positions");
    int expectedIncr[] =                  { 1,   1, 1,          3, 1,  1,      1,            2,   1};
    TokenStream stream = newStop.tokenStream("test", reader);
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
  }

}
