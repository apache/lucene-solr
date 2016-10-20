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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")//Gradle interferes with this Lucene test rule
public class TestSplittingBreakIterator extends LuceneTestCase {


  private static final BreakIterator LINE_BI = BreakIterator.getLineInstance(Locale.ROOT);
  private static final BreakIterator SPLIT_BI = new SplittingBreakIterator(LINE_BI, '|');

  public void testLineBreakIterator() {
    testWithoutSplits(LINE_BI);
  }

  private void testWithoutSplits(BreakIterator bi) {
    // these tests have no '|'
    testBreakIterator(bi,
        " a",
        "^^^");
    testBreakIterator(bi,
        "aa",
        "^ ^");
    testBreakIterator(bi,
        "aa a",
        "^  ^^");
  }

  public void testWithoutSplits() {
    testWithoutSplits(SPLIT_BI);
  }

  public void testOnlySingleSplitChar() {
    testBreakIterator(SPLIT_BI,
        "|",
        "^^");
  }

  public void testSplitThenValue() {
    testBreakIterator(SPLIT_BI,
        "|a",
        "^^^");
  }

  public void testValueThenSplit() {
    testBreakIterator(SPLIT_BI,
        "a|",
        "^^^");
  }

  public void testValueThenSplitThenValue() {
    testBreakIterator(SPLIT_BI,
        "aa|aa",
        "^ ^^ ^");
  }

  public void testValueThenDoubleSplitThenValue() {
    testBreakIterator(SPLIT_BI,
        "aa||aa",
        "^ ^^^ ^");
  }

  public void testValueThenSplitThenDoubleValueThenSplitThenValue() {
    testBreakIterator(SPLIT_BI,
        "a|bb cc|d",
        "^^^  ^ ^^^");
  }

  private void testBreakIterator(BreakIterator bi, String text, String boundaries) {
    bi.setText(text);

    //Test first & last
    testFirstAndLast(bi, text, boundaries);

    //Test if expected boundaries are consistent with reading them from next() in a loop:
    assertEquals(boundaries, readBoundariesToString(bi, text));

    //Test following() and preceding():
    // get each index, randomized in case their is a sequencing bug:
    List<Integer> indexes = randomIntsBetweenInclusive(text.length() + 1);
    testFollowing(bi, text, boundaries, indexes);
    testPreceding(bi, text, boundaries, indexes);

    //Test previous():
    testPrevious(bi, text, boundaries);
  }

  private void testFirstAndLast(BreakIterator bi, String text, String boundaries) {
    String message = "Text: " + text;
    int current = bi.current();
    assertEquals(message, boundaries.indexOf('^'), current);
    assertEquals(message, current, bi.first());
    assertEquals(message, current, bi.current());
    current = bi.last();
    assertEquals(boundaries.lastIndexOf('^'), current);
    assertEquals(message, current, bi.current());
  }

  private void testFollowing(BreakIterator bi, String text, String boundaries, List<Integer> indexes) {
    String message = "Text: " + text;
    for (Integer index : indexes) {
      int got = bi.following(index);
      if (index == boundaries.length()) {
        assertEquals(message, BreakIterator.DONE, got);
        assertEquals(boundaries.lastIndexOf('^'), bi.current());
        continue;
      }
      assertEquals(message + " index:" + index, boundaries.indexOf('^', index + 1), got);
    }
  }

  private void testPreceding(BreakIterator bi, String text, String boundaries, List<Integer> indexes) {
    String message = "Text: " + text;
    for (Integer index : indexes) {
      int got = bi.preceding(index);
      if (index == 0) {
        assertEquals(message, BreakIterator.DONE, got);
        assertEquals(boundaries.indexOf('^'), bi.current());
        continue;
      }
//            if (index == text.length() && got == BreakIterator.DONE) {
//                continue;//hack to accept faulty default impl of BreakIterator.preceding()
//            }
      assertEquals(message + " index:" + index, boundaries.lastIndexOf('^', index - 1), got);
    }
  }

  private List<Integer> randomIntsBetweenInclusive(int end) {
    List<Integer> indexes = new ArrayList<>(end);
    for (int i = 0; i < end; i++) {
      indexes.add(i);
    }
    Collections.shuffle(indexes, random());
    return indexes;
  }

  private void testPrevious(BreakIterator bi, String text, String boundaries) {
    String message = "Text: " + text;

    bi.setText(text);
    int idx = bi.last();//position at the end
    while (true) {
      idx = boundaries.lastIndexOf('^', idx - 1);
      if (idx == -1) {
        assertEquals(message, BreakIterator.DONE, bi.previous());
        break;
      }
      assertEquals(message, idx, bi.previous());
    }
    assertEquals(message, boundaries.indexOf('^'), bi.current());//finishes at first
  }

  /**
   * Returns a string comprised of spaces and '^' only at the boundaries.
   */
  private String readBoundariesToString(BreakIterator bi, String text) {
    // init markers to spaces
    StringBuilder markers = new StringBuilder();
    markers.setLength(text.length() + 1);
    for (int k = 0; k < markers.length(); k++) {
      markers.setCharAt(k, ' ');
    }

    bi.setText(text);
    for (int boundary = bi.current(); boundary != BreakIterator.DONE; boundary = bi.next()) {
      markers.setCharAt(boundary, '^');
    }
    return markers.toString();
  }
}
