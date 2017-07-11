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
package org.apache.lucene.search.uhighlight;

import java.text.BreakIterator;
import java.util.Locale;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.uhighlight.TestWholeBreakIterator.assertSameBreaks;
import static org.hamcrest.CoreMatchers.equalTo;

public class TestCustomSeparatorBreakIterator extends LuceneTestCase {

  private static final Character[] SEPARATORS = new Character[]{' ', '\u0000', 8233};

  public void testBreakOnCustomSeparator() throws Exception {
    Character separator = randomSeparator();
    BreakIterator bi = new CustomSeparatorBreakIterator(separator);
    String source = "this" + separator + "is" + separator + "the" + separator + "first" + separator + "sentence";
    bi.setText(source);
    assertThat(bi.current(), equalTo(0));
    assertThat(bi.first(), equalTo(0));
    assertThat(source.substring(bi.current(), bi.next()), equalTo("this" + separator));
    assertThat(source.substring(bi.current(), bi.next()), equalTo("is" + separator));
    assertThat(source.substring(bi.current(), bi.next()), equalTo("the" + separator));
    assertThat(source.substring(bi.current(), bi.next()), equalTo("first" + separator));
    assertThat(source.substring(bi.current(), bi.next()), equalTo("sentence"));
    assertThat(bi.next(), equalTo(BreakIterator.DONE));

    assertThat(bi.last(), equalTo(source.length()));
    int current = bi.current();
    assertThat(source.substring(bi.previous(), current), equalTo("sentence"));
    current = bi.current();
    assertThat(source.substring(bi.previous(), current), equalTo("first" + separator));
    current = bi.current();
    assertThat(source.substring(bi.previous(), current), equalTo("the" + separator));
    current = bi.current();
    assertThat(source.substring(bi.previous(), current), equalTo("is" + separator));
    current = bi.current();
    assertThat(source.substring(bi.previous(), current), equalTo("this" + separator));
    assertThat(bi.previous(), equalTo(BreakIterator.DONE));
    assertThat(bi.current(), equalTo(0));

    assertThat(source.substring(0, bi.following(9)), equalTo("this" + separator + "is" + separator + "the" + separator));

    assertThat(source.substring(0, bi.preceding(9)), equalTo("this" + separator + "is" + separator));

    assertThat(bi.first(), equalTo(0));
    assertThat(source.substring(0, bi.next(3)), equalTo("this" + separator + "is" + separator + "the" + separator));
  }

  public void testSingleSentences() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("a", expected, actual);
    assertSameBreaks("ab", expected, actual);
    assertSameBreaks("abc", expected, actual);
    assertSameBreaks("", expected, actual);
  }

  public void testSliceEnd() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("a000", 0, 1, expected, actual);
    assertSameBreaks("ab000", 0, 1, expected, actual);
    assertSameBreaks("abc000", 0, 1, expected, actual);
    assertSameBreaks("000", 0, 0, expected, actual);
  }

  public void testSliceStart() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("000a", 3, 1, expected, actual);
    assertSameBreaks("000ab", 3, 2, expected, actual);
    assertSameBreaks("000abc", 3, 3, expected, actual);
    assertSameBreaks("000", 3, 0, expected, actual);
  }

  public void testSliceMiddle() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("000a000", 3, 1, expected, actual);
    assertSameBreaks("000ab000", 3, 2, expected, actual);
    assertSameBreaks("000abc000", 3, 3, expected, actual);
    assertSameBreaks("000000", 3, 0, expected, actual);
  }

  /** the current position must be ignored, initial position is always first() */
  public void testFirstPosition() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("000ab000", 3, 2, 4, expected, actual);
  }

  private static char randomSeparator() {
    return RandomPicks.randomFrom(random(), SEPARATORS);
  }
}
