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

import static org.apache.lucene.queries.intervals.Intervals.*;

public class TestDisjunctionRewrites extends LuceneTestCase {

  public void testDisjunctionSuffix() {

    // BLOCK(a,or(b, BLOCK(b, c))) => or(BLOCK(a, b), BLOCK(a, b, c))
    IntervalsSource actual = Intervals.phrase(
        Intervals.term("a"),
        Intervals.or(Intervals.term("b"), Intervals.phrase("b", "c")));
    IntervalsSource expected = Intervals.or(
        Intervals.phrase("a", "b"),
        Intervals.phrase("a", "b", "c"));
    assertEquals(expected, actual);

  }

  public void testPhraseDisjunctionWithDifferentLengthClauses() {

    // BLOCK(a, or(b, BLOCK(b, c)), d) => or(BLOCK(a, b, d), BLOCK(a, b, c, d))

    IntervalsSource actual = Intervals.phrase(
        Intervals.term("a"),
        Intervals.or(Intervals.term("b"), Intervals.phrase(Intervals.term("b"), Intervals.term("c"))),
        Intervals.term("d"));

    IntervalsSource expected = Intervals.or(
        Intervals.phrase("a", "b", "d"),
        Intervals.phrase("a", "b", "c", "d")
    );

    assertEquals(expected, actual);

  }

  public void testPhraseDisjunctionWithNestedDifferentLengthClauses() {

    // BLOCK(a, or(ORDERED(or(b, c), d), b, p, q), f, g)
    //  => or(BLOCK(a, or(b, p, q), f, g), BLOCK(a, ORDERED(or(b, c), d), f, g))

    IntervalsSource expected = Intervals.or(
        Intervals.phrase(
            Intervals.term("a"),
            Intervals.or(Intervals.term("b"), Intervals.term("p"), Intervals.term("q")),
            Intervals.term("f"),
            Intervals.term("g")),
        Intervals.phrase(
            Intervals.term("a"),
            Intervals.ordered(Intervals.or(Intervals.term("b"), Intervals.term("c")), Intervals.term("d")),
            Intervals.term("f"),
            Intervals.term("g")
        )
    );

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

    assertEquals(expected, actual);
  }

  public void testDisjunctionRewritePreservesFilters() {

    // BLOCK(a, MAXGAPS/3(OR(BLOCK(a, b), BLOCK(c, d))), c)
    // => or(BLOCK(a, MAXGAPS/3(BLOCK(a, b)), c), BLOCK(a, MAXGAPS/3(BLOCK(c, d)), c))

    IntervalsSource actual = Intervals.phrase(
        Intervals.term("a"),
        Intervals.maxgaps(3, Intervals.or(
            Intervals.phrase("a", "b"),
            Intervals.phrase("c", "d")
        )),
        Intervals.term("c")
    );

    IntervalsSource expected = Intervals.or(
        Intervals.phrase(
            Intervals.term("a"),
            Intervals.maxgaps(3, Intervals.phrase("a", "b")),
            Intervals.term("c")
        ),
        Intervals.phrase(
            Intervals.term("a"),
            Intervals.maxgaps(3, Intervals.phrase("c", "d")),
            Intervals.term("c")
        ));

    assertEquals(expected, actual);

  }

  public void testNestedMaxGaps() {
    // MAXGAPS/3(ORDERED(MAXGAPS/4(ORDERED(a, or(b, BLOCK(c, d)))), e)
    // => or(MAXGAPS/3(ORDERED(MAXGAPS/4(ORDERED(a, b)), e)), MAXGAPS/3(ORDERED(MAXGAPS/4(ORDERED(a, BLOCK(c, d))), e)))
    IntervalsSource actual = maxgaps(3, ordered(
        maxgaps(4, ordered(term("a"), or(term("b"), phrase("c", "d")))), term("e")));
    IntervalsSource expected = or(
        maxgaps(3, ordered(maxgaps(4, ordered(term("a"), term("b"))), term("e"))),
        maxgaps(3, ordered(maxgaps(4, ordered(term("a"), phrase("c", "d"))), term("e")))
    );
    assertEquals(expected, actual);
  }

  public void testNestedMaxWidth() {
    // maxwidth does not automatically pull up disjunctions at construction time, so we need to check
    // that it does the right thing if wrapped by something that does need exact internal accounting
    // PHRASE(a, MAXWIDTH(4, OR(ORDERED(b, c), ORDERED(d, e))), f)
    // => or(PHRASE(a, MAXWIDTH(4, ORDERED(b, c)), f), PHRASE(a, MAXWIDTH(4, ORDERED(d, e)), f))
    IntervalsSource actual = phrase(term("a"), maxwidth(4, or(ordered(term("b"), term("c")), ordered(term("d"), term("e")))), term("f"));
    IntervalsSource expected = or(
        phrase(term("a"), maxwidth(4, ordered(term("b"), term("c"))), term("f")),
        phrase(term("a"), maxwidth(4, ordered(term("d"), term("e"))), term("f"))
    );
    assertEquals(expected, actual);
  }

  public void testNestedFixField() {
    // PHRASE(a, FIXFIELD(field, or(PHRASE(a, b), b)), c)
    // => or(PHRASE(a, FIXFIELD(PHRASE(a, b)), c), PHRASE(a, FIXFIELD(b), c))
    IntervalsSource actual = phrase(term("a"), fixField("field", or(phrase("a", "b"), term("b"))), term("c"));
    IntervalsSource expected = or(
        phrase(term("a"), fixField("field", phrase("a", "b")), term("c")),
        phrase(term("a"), fixField("field", term("b")), term("c"))
    );
    assertEquals(expected, actual);
  }

  public void testContainedBy() {
    // the 'big' interval should not be minimized, the 'small' one should be
    // CONTAINED_BY(or("s", BLOCK("s", "t")), MAXGAPS/4(or(ORDERED("a", "b"), ORDERED("c", "d"))))
    // => or(CONTAINED_BY(or("s", BLOCK("s", "t")), MAXGAPS/4(ORDERED("a", "b"))),
    //       CONTAINED_BY(or("s", BLOCK("s", "t")), MAXGAPS/4(ORDERED("c", "d"))))
    IntervalsSource actual = containedBy(or(term("s"), phrase("s", "t")),
                                         maxgaps(4, or(ordered(term("a"), term("b")), ordered(term("c"), term("d")))));
    IntervalsSource expected = or(
        containedBy(or(term("s"), phrase("s", "t")), maxgaps(4, ordered(term("a"), term("b")))),
        containedBy(or(term("s"), phrase("s", "t")), maxgaps(4, ordered(term("c"), term("d"))))
    );
    assertEquals(expected, actual);
  }

  public void testContaining() {
    // the 'big' interval should not be minimized, the 'small' one should be
    // CONTAINING(MAXGAPS/4(or(ORDERED("a", "b"), ORDERED("c", "d"))), or("s", BLOCK("s", "t")))
    // => or(CONTAINING(MAXGAPS/4(ORDERED("a", "b")), or("s", BLOCK("s", "t"))),
    //       CONTAINING(MAXGAPS/4(ORDERED("c", "d")), or("s", BLOCK("s", "t"))))
    IntervalsSource actual = containing(maxgaps(4, or(ordered(term("a"), term("b")), ordered(term("c"), term("d")))),
                                        or(term("s"), phrase("s", "t")));
    IntervalsSource expected = or(
        containing(maxgaps(4, ordered(term("a"), term("b"))), or(term("s"), phrase("s", "t"))),
        containing(maxgaps(4, ordered(term("c"), term("d"))), or(term("s"), phrase("s", "t")))
    );
    assertEquals(expected, actual);
  }

  public void testNotContainedBy() {
    // the 'big' interval should not be minimized, the 'small' one should be
    // NOT_CONTAINED_BY(or(BLOCK("a", "b"), "a"), or(BLOCK("c", "d"), "d"))
    // => or(NOT_CONTAINED_BY(or(BLOCK("a", "b"), "a"), BLOCK("c", "d"))), NOT_CONTAINED_BY(or(BLOCK("a", "b"), "a"), "d"))
    IntervalsSource actual = notContainedBy(or(phrase("a", "b"), term("a")), or(phrase("c", "d"), term("d")));
    IntervalsSource expected = or(
        notContainedBy(or(phrase("a", "b"), term("a")), phrase("c", "d")),
        notContainedBy(or(phrase("a", "b"), term("a")), term("d"))
    );
    assertEquals(expected, actual);
  }

  public void testNotContaining() {
    // the 'big' interval should not be minimized, the 'small' one should be
    // NOT_CONTAINING(or(BLOCK("a", "b"), "a"), or(BLOCK("c", "d"), "d"))
    // => or(NOT_CONTAINING(BLOCK("a", "b"), or(BLOCK("c", "d"), "d")), NOT_CONTAINING("a", or(BLOCK("c", "d"), "d")))
    IntervalsSource actual = notContaining(or(phrase("a", "b"), term("a")), or(phrase("c", "d"), term("d")));
    IntervalsSource expected = or(
        notContaining(phrase("a", "b"), or(phrase("c", "d"), term("d"))),
        notContaining(term("a"), or(phrase("c", "d"), term("d")))
    );
    assertEquals(expected, actual);
  }

  public void testBlockedRewrites() {
    IntervalsSource actual = phrase(term("a"), or(false, phrase("b", "c"), term("c")));
    IntervalsSource ifRewritten = or(phrase("a", "b", "c"), phrase("a", "c"));
    assertNotEquals(ifRewritten, actual);
  }

}
