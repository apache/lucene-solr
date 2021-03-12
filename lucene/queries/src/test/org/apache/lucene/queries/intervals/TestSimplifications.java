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

package org.apache.lucene.queries.intervals;

import org.apache.lucene.util.LuceneTestCase;

public class TestSimplifications extends LuceneTestCase {

  public void testStringPhrases() {
    // BLOCK(term) => term
    IntervalsSource actual = Intervals.phrase("term");
    assertEquals(Intervals.term("term"), actual);
  }

  public void testSourcePhrases() {
    IntervalsSource actual = Intervals.phrase(Intervals.term("term"));
    assertEquals(Intervals.term("term"), actual);
  }

  public void testOrdered() {
    // ORDERED(term) => term
    IntervalsSource actual = Intervals.ordered(Intervals.term("term"));
    assertEquals(Intervals.term("term"), actual);
  }

  public void testOrderedWithDuplicates() {
    IntervalsSource actual = Intervals.ordered(Intervals.term("term"), Intervals.term("term"));
    assertEquals("ORDERED(term,term)", actual.toString());
    actual = Intervals.ordered(Intervals.term("term"), Intervals.term("term"), Intervals.term("bar"));
    assertEquals("ORDERED(term,term,bar)", actual.toString());
  }

  public void testUnordered() {
    // UNORDERED(term) => term
    IntervalsSource actual = Intervals.unordered(Intervals.term("term"));
    assertEquals(Intervals.term("term"), actual);
  }

  public void testUnorderedWithDuplicates() {
    IntervalsSource actual = Intervals.unordered(Intervals.term("term"), Intervals.term("term"));
    assertEquals("UNORDERED(term,term)", actual.toString());
    actual = Intervals.unordered(Intervals.term("term"), Intervals.term("term"), Intervals.term("bar"));
    assertEquals("UNORDERED(term,term,bar)", actual.toString());
  }

  public void testUnorderedOverlaps() {
    // UNORDERED_NO_OVERLAPS(term, term) => ORDERED(term, term)
    IntervalsSource actual = Intervals.unorderedNoOverlaps(Intervals.term("term"), Intervals.term("term"));
    assertEquals(Intervals.ordered(Intervals.term("term"), Intervals.term("term")), actual);
  }

  public void testDisjunctionSingleton() {
    IntervalsSource actual = Intervals.or(Intervals.term("a"));
    assertEquals(Intervals.term("a"), actual);
  }

  public void testDisjunctionRemovesDuplicates() {
    // or(a, b, a) => or(a, b)
    IntervalsSource actual = Intervals.or(Intervals.term("a"), Intervals.term("b"), Intervals.term("a"));
    assertEquals(Intervals.or(Intervals.term("a"), Intervals.term("b")), actual);
  }

  public void testPhraseSimplification() {
    // BLOCK(BLOCK(a, b), c) => BLOCK(a, b, c)
    IntervalsSource actual = Intervals.phrase(Intervals.phrase(Intervals.term("a"), Intervals.term("b")), Intervals.term("c"));
    assertEquals(Intervals.phrase(Intervals.term("a"), Intervals.term("b"), Intervals.term("c")), actual);

    // BLOCK(a, BLOCK(b, BLOCK(c, d))) => BLOCK(a, b, c, d)
    actual = Intervals.phrase(Intervals.term("a"), Intervals.phrase(Intervals.term("b"),
        Intervals.phrase(Intervals.term("c"), Intervals.term("d"))));
    assertEquals(Intervals.phrase(Intervals.term("a"), Intervals.term("b"), Intervals.term("c"), Intervals.term("d")), actual);
  }

  public void testDisjunctionSimplification() {
    // or(a, or(b, or(c, d))) => or(a, b, c, d)
    IntervalsSource actual = Intervals.or(Intervals.term("a"), Intervals.or(Intervals.term("b"),
        Intervals.or(Intervals.term("c"), Intervals.term("d"))));
    assertEquals(Intervals.or(Intervals.term("a"), Intervals.term("b"), Intervals.term("c"), Intervals.term("d")), actual);
  }

  public void testMinShouldMatchSimplifications() {
    IntervalsSource expected = Intervals.unordered(Intervals.term("a"), Intervals.term("b"));
    assertEquals(expected, Intervals.atLeast(2, Intervals.term("a"), Intervals.term("b")));

    assertEquals(
        "NOMATCH(Too few sources to match minimum of [3]: [a, b])",
        Intervals.atLeast(3, Intervals.term("a"), Intervals.term("b")).toString());
  }
}
