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
package org.apache.lucene.search.postingshighlight;

import org.apache.lucene.util.LuceneTestCase;

import java.text.BreakIterator;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Locale;

public class TestWholeBreakIterator extends LuceneTestCase {
  
  /** For single sentences, we know WholeBreakIterator should break the same as a sentence iterator */
  public void testSingleSentences() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new WholeBreakIterator();
    assertSameBreaks("a", expected, actual);
    assertSameBreaks("ab", expected, actual);
    assertSameBreaks("abc", expected, actual);
    assertSameBreaks("", expected, actual);
  }
  
  public void testSliceEnd() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new WholeBreakIterator();
    assertSameBreaks("a000", 0, 1, expected, actual);
    assertSameBreaks("ab000", 0, 1, expected, actual);
    assertSameBreaks("abc000", 0, 1, expected, actual);
    assertSameBreaks("000", 0, 0, expected, actual);
  }
  
  public void testSliceStart() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new WholeBreakIterator();
    assertSameBreaks("000a", 3, 1, expected, actual);
    assertSameBreaks("000ab", 3, 2, expected, actual);
    assertSameBreaks("000abc", 3, 3, expected, actual);
    assertSameBreaks("000", 3, 0, expected, actual);
  }
  
  public void testSliceMiddle() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new WholeBreakIterator();
    assertSameBreaks("000a000", 3, 1, expected, actual);
    assertSameBreaks("000ab000", 3, 2, expected, actual);
    assertSameBreaks("000abc000", 3, 3, expected, actual);
    assertSameBreaks("000000", 3, 0, expected, actual);
  }
  
  /** the current position must be ignored, initial position is always first() */
  public void testFirstPosition() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new WholeBreakIterator();
    assertSameBreaks("000ab000", 3, 2, 4, expected, actual);
  }

  public static void assertSameBreaks(String text, BreakIterator expected, BreakIterator actual) {
    assertSameBreaks(new StringCharacterIterator(text), 
                     new StringCharacterIterator(text), 
                     expected, 
                     actual);
  }
  
  public static void assertSameBreaks(String text, int offset, int length, BreakIterator expected, BreakIterator actual) {
    assertSameBreaks(text, offset, length, offset, expected, actual);
  }
  
  public static void assertSameBreaks(String text, int offset, int length, int current, BreakIterator expected, BreakIterator actual) {
    assertSameBreaks(new StringCharacterIterator(text, offset, offset+length, current), 
                     new StringCharacterIterator(text, offset, offset+length, current), 
                     expected, 
                     actual);
  }

  /** Asserts that two breakiterators break the text the same way */
  public static void assertSameBreaks(CharacterIterator one, CharacterIterator two, BreakIterator expected, BreakIterator actual) {
    expected.setText(one);
    actual.setText(two);

    assertEquals(expected.current(), actual.current());

    // next()
    int v = expected.current();
    while (v != BreakIterator.DONE) {
      assertEquals(v = expected.next(), actual.next());
      assertEquals(expected.current(), actual.current());
    }
    
    // first()
    assertEquals(expected.first(), actual.first());
    assertEquals(expected.current(), actual.current());
    // last()
    assertEquals(expected.last(), actual.last());
    assertEquals(expected.current(), actual.current());
    
    // previous()
    v = expected.current();
    while (v != BreakIterator.DONE) {
      assertEquals(v = expected.previous(), actual.previous());
      assertEquals(expected.current(), actual.current());
    }
    
    // following()
    for (int i = one.getBeginIndex(); i <= one.getEndIndex(); i++) {
      expected.first();
      actual.first();
      assertEquals(expected.following(i), actual.following(i));
      assertEquals(expected.current(), actual.current());
    }
    
    // preceding()
    for (int i = one.getBeginIndex(); i <= one.getEndIndex(); i++) {
      expected.last();
      actual.last();
      assertEquals(expected.preceding(i), actual.preceding(i));
      assertEquals(expected.current(), actual.current());
    }
  }
}
