package org.apache.lucene.search.postingshighlight;

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

import java.text.BreakIterator;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;

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

  /** Asserts that two breakiterators break the text the same way */
  // TODO: change this to use offsets with non-zero start/end
  public void assertSameBreaks(String text, BreakIterator expected, BreakIterator actual) {
    expected.setText(text);
    actual.setText(text);

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
    for (int i = 0; i <= text.length(); i++) {
      expected.first();
      actual.first();
      assertEquals(expected.following(i), actual.following(i));
      assertEquals(expected.current(), actual.current());
    }
    
    // preceding()
    for (int i = 0; i <= text.length(); i++) {
      expected.last();
      actual.last();
      assertEquals(expected.preceding(i), actual.preceding(i));
      assertEquals(expected.current(), actual.current());
    }
  }
}
