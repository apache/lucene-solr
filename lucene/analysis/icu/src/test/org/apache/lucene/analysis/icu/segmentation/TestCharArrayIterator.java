package org.apache.lucene.analysis.icu.segmentation;

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

import java.text.CharacterIterator;

import org.apache.lucene.util.LuceneTestCase;

public class TestCharArrayIterator extends LuceneTestCase {
  public void testBasicUsage() {
    CharArrayIterator ci = new CharArrayIterator();
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
  }
  
  public void testFirst() {
    CharArrayIterator ci = new CharArrayIterator();
    ci.setText("testing".toCharArray(), 0, "testing".length());
    ci.next();
    // Sets the position to getBeginIndex() and returns the character at that position. 
    assertEquals('t', ci.first());
    assertEquals(ci.getBeginIndex(), ci.getIndex());
    // or DONE if the text is empty
    ci.setText(new char[] {}, 0, 0);
    assertEquals(CharacterIterator.DONE, ci.first());
  }
  
  public void testLast() {
    CharArrayIterator ci = new CharArrayIterator();
    ci.setText("testing".toCharArray(), 0, "testing".length());
    // Sets the position to getEndIndex()-1 (getEndIndex() if the text is empty) 
    // and returns the character at that position. 
    assertEquals('g', ci.last());
    assertEquals(ci.getIndex(), ci.getEndIndex() - 1);
    // or DONE if the text is empty
    ci.setText(new char[] {}, 0, 0);
    assertEquals(CharacterIterator.DONE, ci.last());
    assertEquals(ci.getEndIndex(), ci.getIndex());
  }
  
  public void testCurrent() {
    CharArrayIterator ci = new CharArrayIterator();
    // Gets the character at the current position (as returned by getIndex()). 
    ci.setText("testing".toCharArray(), 0, "testing".length());
    assertEquals('t', ci.current());
    ci.last();
    ci.next();
    // or DONE if the current position is off the end of the text.
    assertEquals(CharacterIterator.DONE, ci.current());
  }
  
  public void testNext() {
    CharArrayIterator ci = new CharArrayIterator();
    ci.setText("te".toCharArray(), 0, 2);
    // Increments the iterator's index by one and returns the character at the new index.
    assertEquals('e', ci.next());
    assertEquals(1, ci.getIndex());
    // or DONE if the new position is off the end of the text range.
    assertEquals(CharacterIterator.DONE, ci.next());
    assertEquals(ci.getEndIndex(), ci.getIndex());
  }
  
  public void testSetIndex() {
    CharArrayIterator ci = new CharArrayIterator();
    ci.setText("test".toCharArray(), 0, "test".length());
    try {
      ci.setIndex(5);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }
  }
  
  public void testClone() {
    char text[] = "testing".toCharArray();
    CharArrayIterator ci = new CharArrayIterator();
    ci.setText(text, 0, text.length);
    ci.next();
    CharArrayIterator ci2 = ci.clone();
    assertEquals(ci.getIndex(), ci2.getIndex());
    assertEquals(ci.next(), ci2.next());
    assertEquals(ci.last(), ci2.last());
  }
  

}
