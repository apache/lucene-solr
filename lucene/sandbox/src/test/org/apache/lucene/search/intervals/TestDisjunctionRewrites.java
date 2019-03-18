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

package org.apache.lucene.search.intervals;

import org.apache.lucene.util.LuceneTestCase;

public class TestDisjunctionRewrites extends LuceneTestCase {

  public void testPhraseDisjunctionWithDifferentLengthClauses() {

    // BLOCK(a, or(b, BLOCK(b, c)), d) => BLOCK(a, or(BLOCK(b, d), BLOCK(b, c, d)))

    IntervalsSource actual = Intervals.phrase(
        Intervals.term("a"),
        Intervals.or(Intervals.term("b"), Intervals.phrase(Intervals.term("b"), Intervals.term("c"))),
        Intervals.term("d"));

    IntervalsSource expected = Intervals.phrase(
        Intervals.term("a"),
        Intervals.or(Intervals.phrase(Intervals.term("b"), Intervals.term("d")),
            Intervals.phrase(Intervals.term("b"), Intervals.term("c"), Intervals.term("d"))));

    assertEquals(expected, actual);

  }

  public void testPhraseDisjunctionWithNestedDifferentLengthClauses() {

    // BLOCK(a, or(ORDERED(or(b, c), d), b, p, q), f, g)
    //  => BLOCK(a, or(BLOCK(ordered(or(b, c), d), f, g), BLOCK(or(b, p, q), f, g)))

    IntervalsSource actual = Intervals.phrase(
        Intervals.term("a"),
        Intervals.or(
            Intervals.ordered(Intervals.or(Intervals.term("b"), Intervals.term("c")), Intervals.term("d")),
            Intervals.term("b"),
            Intervals.term("p"),
            Intervals.term("q")
        ),
        Intervals.term("f"),
        Intervals.term("g")
    );

    IntervalsSource expected = Intervals.phrase(
        Intervals.term("a"),
        Intervals.or(
            Intervals.phrase(
                Intervals.ordered(Intervals.or(Intervals.term("b"), Intervals.term("c")), Intervals.term("d")),
                Intervals.term("f")),
            Intervals.phrase(Intervals.or(Intervals.term("p"), Intervals.term("q"), Intervals.term("b")), Intervals.term("f"))
        ),
        Intervals.term("g")
    );

    assertEquals(expected, actual);
  }

  public void testDisjunctionRewritePreservesFilters() {

    // BLOCK(a, MAXGAPS/3(OR(BLOCK(a, b), BLOCK(c, d))), c
    // => BLOCK(a, OR(BLOCK(MAXGAPS/3(BLOCK(a, b)), c), BLOCK(MAXGAPS/3(BLOCK(c, d)), c)))

    IntervalsSource actual = Intervals.phrase(
        Intervals.term("a"),
        Intervals.maxgaps(3, Intervals.or(
            Intervals.phrase("a", "b"),
            Intervals.phrase("c", "d")
        )),
        Intervals.term("c")
    );

    IntervalsSource expected = Intervals.phrase(
        Intervals.term("a"),
        Intervals.or(
            Intervals.phrase(Intervals.maxgaps(3, Intervals.phrase("a", "b")), Intervals.term("c")),
            Intervals.phrase(Intervals.maxgaps(3, Intervals.phrase("c", "d")), Intervals.term("c"))));

    assertEquals(expected, actual);

  }

}
