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
package org.apache.lucene.analysis.util;


import java.text.BreakIterator;
import java.text.CharacterIterator;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestCharArrayIterator extends LuceneTestCase {
  
  public void testWordInstance() {
    doTests(CharArrayIterator.newWordInstance());
  }
  
  public void testConsumeWordInstance() {
    // we use the default locale, as it's randomized by LuceneTestCase
    BreakIterator bi = BreakIterator.getWordInstance(Locale.getDefault());
    CharArrayIterator ci = CharArrayIterator.newWordInstance();
    for (int i = 0; i < 10000; i++) {
      char text[] = TestUtil.randomUnicodeString(random()).toCharArray();
      ci.setText(text, 0, text.length);
      consume(bi, ci);
    }
  }
  
  /* run this to test if your JRE is buggy
  public void testWordInstanceJREBUG() {
    // we use the default locale, as it's randomized by LuceneTestCase
    BreakIterator bi = BreakIterator.getWordInstance(Locale.getDefault());
    Segment ci = new Segment();
    for (int i = 0; i < 10000; i++) {
      char text[] = _TestUtil.randomUnicodeString(random).toCharArray();
      ci.array = text;
      ci.offset = 0;
      ci.count = text.length;
      consume(bi, ci);
    }
  }
  */
  
  public void testSentenceInstance() {
    doTests(CharArrayIterator.newSentenceInstance());
  }
  
  public void testConsumeSentenceInstance() {
    // we use the default locale, as it's randomized by LuceneTestCase
    BreakIterator bi = BreakIterator.getSentenceInstance(Locale.getDefault());
    CharArrayIterator ci = CharArrayIterator.newSentenceInstance();
    for (int i = 0; i < 10000; i++) {
      char text[] = TestUtil.randomUnicodeString(random()).toCharArray();
      ci.setText(text, 0, text.length);
      consume(bi, ci);
    }
  }
  
  /* run this to test if your JRE is buggy
  public void testSentenceInstanceJREBUG() {
    // we use the default locale, as it's randomized by LuceneTestCase
    BreakIterator bi = BreakIterator.getSentenceInstance(Locale.getDefault());
    Segment ci = new Segment();
    for (int i = 0; i < 10000; i++) {
      char text[] = _TestUtil.randomUnicodeString(random).toCharArray();
      ci.array = text;
      ci.offset = 0;
      ci.count = text.length;
      consume(bi, ci);
    }
  }
  */
  
  private void doTests(CharArrayIterator ci) {
    // basics
    ci.setText("testing".toCharArray(), 0, "testing".length());
    assertEquals(0, ci.getBeginIndex());
    assertEquals(7, ci.getEndIndex());
    assertEquals(0, ci.getIndex());
    assertEquals('t', ci.current());
    assertEquals('e', ci.next());
    assertEquals('g', ci.last());
    assertEquals('n', ci.previous());
    assertEquals('t', ci.first());
    assertEquals(CharacterIterator.DONE, ci.previous());
    
    // first()
    ci.setText("testing".toCharArray(), 0, "testing".length());
    ci.next();
    // Sets the position to getBeginIndex() and returns the character at that position. 
    assertEquals('t', ci.first());
    assertEquals(ci.getBeginIndex(), ci.getIndex());
    // or DONE if the text is empty
    ci.setText(new char[] {}, 0, 0);
    assertEquals(CharacterIterator.DONE, ci.first());
    
    // last()
    ci.setText("testing".toCharArray(), 0, "testing".length());
    // Sets the position to getEndIndex()-1 (getEndIndex() if the text is empty) 
    // and returns the character at that position. 
    assertEquals('g', ci.last());
    assertEquals(ci.getIndex(), ci.getEndIndex() - 1);
    // or DONE if the text is empty
    ci.setText(new char[] {}, 0, 0);
    assertEquals(CharacterIterator.DONE, ci.last());
    assertEquals(ci.getEndIndex(), ci.getIndex());
    
    // current()
    // Gets the character at the current position (as returned by getIndex()). 
    ci.setText("testing".toCharArray(), 0, "testing".length());
    assertEquals('t', ci.current());
    ci.last();
    ci.next();
    // or DONE if the current position is off the end of the text.
    assertEquals(CharacterIterator.DONE, ci.current());
    
    // next()
    ci.setText("te".toCharArray(), 0, 2);
    // Increments the iterator's index by one and returns the character at the new index.
    assertEquals('e', ci.next());
    assertEquals(1, ci.getIndex());
    // or DONE if the new position is off the end of the text range.
    assertEquals(CharacterIterator.DONE, ci.next());
    assertEquals(ci.getEndIndex(), ci.getIndex());
    
    // setIndex()
    ci.setText("test".toCharArray(), 0, "test".length());
    expectThrows(IllegalArgumentException.class, () -> {
      ci.setIndex(5);
    });
    
    // clone()
    char text[] = "testing".toCharArray();
    ci.setText(text, 0, text.length);
    ci.next();
    CharArrayIterator ci2 = ci.clone();
    assertEquals(ci.getIndex(), ci2.getIndex());
    assertEquals(ci.next(), ci2.next());
    assertEquals(ci.last(), ci2.last());
  }
  
  private void consume(BreakIterator bi, CharacterIterator ci) {
    bi.setText(ci);
    while (bi.next() != BreakIterator.DONE)
      ;
  }
}
