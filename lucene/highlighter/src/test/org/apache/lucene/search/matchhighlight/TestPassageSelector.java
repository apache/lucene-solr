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
package org.apache.lucene.search.matchhighlight;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.util.LuceneTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

public class TestPassageSelector extends LuceneTestCase {
  @Test
  public void checkEmptyExtra() {
    checkPassages(
        "foo >>bar<< baz abc",
        "foo bar baz abc",
        300,
        100,
        new OffsetRange(4, 7),
        new OffsetRange(4, 7));

    checkPassages(
        ">foo >bar< >baz<< abc",
        "foo bar baz abc",
        300,
        100,
        new OffsetRange(0, 11),
        new OffsetRange(4, 7),
        new OffsetRange(8, 11));

    checkPassages(
        ">>foo< bar >baz<< abc",
        "foo bar baz abc",
        300,
        100,
        new OffsetRange(0, 11),
        new OffsetRange(0, 3),
        new OffsetRange(8, 11));
  }

  @Test
  public void oneMarker() {
    checkPassages(">0<123456789a", "0123456789a", 300, 1, new OffsetRange(0, 1));
    checkPassages("0123456789>a<", "0123456789a", 300, 1, new OffsetRange(10, 11));
    checkPassages(">0123456789a<", "0123456789a", 300, 1, new OffsetRange(0, 11));
  }

  @Test
  public void noHighlights() {
    checkPassages("0123456789a", "0123456789a", 300, 1);
    checkPassages("01234...", "0123456789a", 5, 1);
    checkPassages(
        "0123|45678",
        "0123456789a",
        15,
        2,
        new OffsetRange[0],
        new OffsetRange[] {new OffsetRange(0, 4), new OffsetRange(4, 9)});
  }

  @Test
  public void oneMarkerTruncated() {
    checkPassages(">0<12...", "0123456789a", 4, 1, new OffsetRange(0, 1));
    checkPassages("...789>a<", "0123456789a", 4, 1, new OffsetRange(10, 11));
    checkPassages("...>3456<...", "0123456789a", 4, 1, new OffsetRange(3, 7));
    checkPassages("...3>45<6...", "0123456789a", 4, 1, new OffsetRange(4, 6));
  }

  @Test
  public void highlightLargerThanWindow() {
    String value = "0123456789a";
    checkPassages("0123...", value, 4, 1, new OffsetRange(0, value.length()));
  }

  @Test
  public void twoMarkers() {
    checkPassages(
        "0>12<3>45<6789a", "0123456789a", 300, 1, new OffsetRange(1, 3), new OffsetRange(4, 6));
    checkPassages(
        "0>123<>45<6789a", "0123456789a", 300, 1, new OffsetRange(1, 4), new OffsetRange(4, 6));
  }

  @Test
  public void noMarkers() {
    checkPassages("0123456789a", "0123456789a", 300, 1);
    checkPassages("0123...", "0123456789a", 4, 1);
  }

  @Test
  public void markersOutsideValue() {
    checkPassages("0123456789a", "0123456789a", 300, 1, new OffsetRange(100, 200));
  }

  @Test
  public void twoPassages() {
    checkPassages(
        "0>12<3...|...6>78<9...",
        "0123456789a",
        4,
        2,
        new OffsetRange(1, 3),
        new OffsetRange(7, 9));
  }

  @Test
  public void emptyRanges() {
    // Empty ranges cover the highlight, so it is omitted.
    // Instead, the first non-empty range is taken as the default.
    checkPassages(
        "6789...",
        "0123456789a",
        4,
        2,
        ranges(new OffsetRange(0, 1)),
        ranges(new OffsetRange(0, 0), new OffsetRange(2, 2), new OffsetRange(6, 11)));
  }

  @Test
  public void defaultPassages() {
    // No highlights, multiple value ranges.
    checkPassages(
        "01|23|4567",
        "0123456789",
        100,
        100,
        ranges(),
        ranges(new OffsetRange(0, 2), new OffsetRange(2, 4), new OffsetRange(4, 8)));

    // No highlights, multiple value ranges, maxPassages = 1
    checkPassages(
        "01",
        "0123456789",
        100,
        1,
        ranges(),
        ranges(new OffsetRange(0, 2), new OffsetRange(2, 4), new OffsetRange(4, 8)));

    // No highlights, multiple value ranges, maxWindows size too short.
    checkPassages(
        "0123...|5678...",
        "0123456789",
        4,
        2,
        ranges(),
        ranges(new OffsetRange(0, 5), new OffsetRange(5, 10)));
  }

  @Test
  public void passageScoring() {
    // More highlights per passage -> better passage
    checkPassages(
        ">01<>23<...",
        "0123456789a",
        4,
        1,
        new OffsetRange(0, 2),
        new OffsetRange(2, 4),
        new OffsetRange(8, 10));

    checkPassages(
        "...>01<23>45<67>89<...",
        "__________0123456789a__________",
        10,
        1,
        new OffsetRange(10, 12),
        new OffsetRange(14, 16),
        new OffsetRange(18, 20));

    // ...if tied, the one with longer highlight length overall.
    checkPassages(
        "...6>789<...", "0123456789a", 4, 1, new OffsetRange(0, 2), new OffsetRange(7, 10));

    // ...if tied, the first one in order.
    checkPassages(">01<23...", "0123456789a", 4, 1, new OffsetRange(0, 2), new OffsetRange(8, 10));
  }

  @Test
  public void rangeWindows() {
    // Add constraint windows to split the three highlights.
    checkPassages(
        "..._______>01<2",
        "__________0123456789a__________",
        10,
        3,
        ranges(new OffsetRange(10, 12), new OffsetRange(14, 16), new OffsetRange(18, 20)),
        ranges(new OffsetRange(0, 13)));

    checkPassages(
        ">89<a_______...",
        "__________0123456789a__________",
        10,
        3,
        ranges(new OffsetRange(10, 12), new OffsetRange(14, 16), new OffsetRange(18, 20)),
        ranges(new OffsetRange(18, Integer.MAX_VALUE)));

    checkPassages(
        "...________>01<|23>45<67|>89<a_______...",
        "__________0123456789a__________",
        10,
        3,
        ranges(new OffsetRange(10, 12), new OffsetRange(14, 16), new OffsetRange(18, 20)),
        ranges(
            new OffsetRange(0, 12),
            new OffsetRange(12, 18),
            new OffsetRange(18, Integer.MAX_VALUE)));
  }

  @Test
  public void testHighlightAcrossAllowedValueRange() {
    checkPassages(
        "012>34<|>56<789",
        "0123456789",
        100,
        10,
        ranges(new OffsetRange(3, 7)),
        ranges(new OffsetRange(0, 5), new OffsetRange(5, 10)));
  }

  @Test
  public void randomizedSanityCheck() {
    PassageSelector selector = new PassageSelector();
    PassageFormatter formatter = new PassageFormatter("...", ">", "<");
    ArrayList<OffsetRange> highlights = new ArrayList<>();
    ArrayList<OffsetRange> permittedRanges = new ArrayList<>();

    for (int i = 0; i < 5000; i++) {
      String value =
          randomBoolean()
              ? randomAsciiLettersOfLengthBetween(0, 100)
              : randomRealisticUnicodeOfCodepointLengthBetween(0, 1000);
      int maxLength = value.length();

      permittedRanges.clear();
      highlights.clear();
      for (int j = randomIntBetween(0, 10); --j >= 0; ) {
        int from = randomIntBetween(0, value.length());
        int to = Math.min(from + randomIntBetween(1, 10), maxLength);
        if (from < to) {
          highlights.add(new OffsetRange(from, to));
        }
      }

      int charWindow = randomIntBetween(1, 100);
      int maxPassages = randomIntBetween(1, 10);

      if (randomIntBetween(0, 5) == 0) {
        int increment = value.length() / 10;
        for (int c = randomIntBetween(0, 20), start = 0; --c >= 0; ) {
          int step = randomIntBetween(0, increment);
          int to = Math.min(start + step, maxLength);
          if (start < to) {
            permittedRanges.add(new OffsetRange(start, to));
          }
          start += step + randomIntBetween(0, 3);
        }
      } else {
        permittedRanges.add(new OffsetRange(0, value.length()));
      }

      // Just make sure there are no exceptions.
      List<Passage> passages =
          selector.pickBest(value, highlights, charWindow, maxPassages, permittedRanges);
      formatter.format(value, passages, permittedRanges);
    }
  }

  private void checkPassages(
      String expected, String value, int charWindow, int maxPassages, OffsetRange... highlights) {
    checkPassages(
        expected,
        value,
        charWindow,
        maxPassages,
        highlights,
        ranges(new OffsetRange(0, value.length())));
  }

  private void checkPassages(
      String expected,
      String value,
      int charWindow,
      int maxPassages,
      OffsetRange[] highlights,
      OffsetRange[] ranges) {
    String result = getPassages(value, charWindow, maxPassages, highlights, ranges);
    if (!Objects.equals(result, expected)) {
      System.out.println("Value:  " + value);
      System.out.println("Result: " + result);
      System.out.println("Expect: " + expected);
    }
    assertThat(result, Matchers.equalTo(expected));
  }

  protected String getPassages(
      String value,
      int charWindow,
      int maxPassages,
      OffsetRange[] highlights,
      OffsetRange[] ranges) {
    PassageFormatter passageFormatter = new PassageFormatter("...", ">", "<");
    PassageSelector selector = new PassageSelector();
    List<OffsetRange> rangeList = Arrays.asList(ranges);
    List<OffsetRange> hlist = Arrays.asList(highlights);
    List<Passage> passages = selector.pickBest(value, hlist, charWindow, maxPassages, rangeList);
    return String.join("|", passageFormatter.format(value, passages, rangeList));
  }

  protected OffsetRange[] ranges(OffsetRange... offsets) {
    return offsets;
  }
}
