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

package org.apache.lucene.analysis.shingle;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.graph.GraphTokenStreamFiniteStrings;

public class FixedShingleFilterTest extends BaseTokenStreamTestCase {

  public void testBiGramFilter() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("please", 0, 6),
        new Token("divide", 7, 13),
        new Token("this", 14, 18),
        new Token("sentence", 19, 27),
        new Token("into", 28, 32),
        new Token("shingles", 33, 41)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 2),
        new String[]{"please divide", "divide this", "this sentence", "sentence into", "into shingles"},
        new int[]{0, 7, 14, 19, 28,},
        new int[]{13, 18, 27, 32, 41,},
        new String[]{"shingle", "shingle", "shingle", "shingle", "shingle",},
        new int[]{1, 1, 1, 1, 1,},
        new int[]{1, 1, 1, 1, 1});

  }

  public void testBiGramFilterWithAltSeparator() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("please", 0, 6),
        new Token("divide", 7, 13),
        new Token("this", 14, 18),
        new Token("sentence", 19, 27),
        new Token("into", 28, 32),
        new Token("shingles", 33, 41)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 2, "<SEP>", "_"),
        new String[]{"please<SEP>divide", "divide<SEP>this", "this<SEP>sentence", "sentence<SEP>into", "into<SEP>shingles"},
        new int[]{0, 7, 14, 19, 28},
        new int[]{13, 18, 27, 32, 41},
        new String[]{"shingle", "shingle", "shingle", "shingle", "shingle"},
        new int[]{1, 1, 1, 1, 1});

  }

  public void testTriGramFilter() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("please", 0, 6),
        new Token("divide", 7, 13),
        new Token("this", 14, 18),
        new Token("sentence", 19, 27),
        new Token("into", 28, 32),
        new Token("shingles", 33, 41)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 3),
        new String[]{"please divide this", "divide this sentence", "this sentence into", "sentence into shingles"});
  }

  public void testShingleSizeGreaterThanTokenstreamLength() throws IOException {

    TokenStream ts = new FixedShingleFilter(new CannedTokenStream(
        new Token("please", 0, 6),
        new Token("divide", 7, 13)
    ), 3);

    ts.reset();
    assertFalse(ts.incrementToken());

  }

  public void testWithStopwords() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("please", 0, 6),
        new Token("divide", 7, 13),
        new Token("sentence", 2, 19, 27),
        new Token("shingles", 2, 33, 41)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 3),
        new String[]{"please divide _", "divide _ sentence", "sentence _ shingles"},
        new int[]{0, 7, 19,},
        new int[]{13, 27, 41,},
        new String[]{"shingle", "shingle", "shingle",},
        new int[]{1, 1, 2,});

  }

  public void testConsecutiveStopwords() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("b", 2, 2, 3),
        new Token("c", 4, 5),
        new Token("d", 6, 7),
        new Token("b", 3, 12, 13),
        new Token("c", 14, 15)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 4),
        new String[]{"b c d _", "c d _ _", "d _ _ b"},
        new int[]{2, 4, 6,},
        new int[]{7, 7, 13,},
        new int[]{2, 1, 1,});
  }

  public void testTrailingStopwords() throws IOException {

    TokenStream ts = new CannedTokenStream(1, 7,
        new Token("b", 0, 1),
        new Token("c", 2, 3),
        new Token("d", 4, 5)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 3),
          new String[] { "b c d", "c d _" },
          new int[] {    0,         2,    },
          new int[] {    5,         5,    },
          new int[] {    1,         1,    });


  }

  public void testMultipleTrailingStopwords() throws IOException {

    TokenStream ts = new CannedTokenStream(2, 9,
        new Token("b", 0, 1),
        new Token("c", 2, 3),
        new Token("d", 4, 5)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 3),
          new String[] { "b c d", "c d _", "d _ _" },
          new int[] {    0,         2,      4 },
          new int[] {    5,         5,      5 },
          new int[] {    1,         1,      1 });
  }

  public void testIncomingGraphs() throws IOException {

    // b/a c b/a d

    TokenStream ts = new CannedTokenStream(
        new Token("b", 0, 1),
        new Token("a", 0, 0, 1),
        new Token("c", 2, 3),
        new Token("b", 4, 5),
        new Token("a", 0, 4, 5),
        new Token("d", 6, 7)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 2),
          new String[] { "b c", "a c", "c b", "c a", "b d", "a d" },
          new int[] {    0,     0,     2,     2,     4,     4 },
          new int[] {    3,     3,     5,     5,     7,     7 },
          new int[] {    1,     0,     1,     0,     1,     0 });
  }

  public void testShinglesSpanningGraphs() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("b", 0, 1),
        new Token("a", 0, 0, 1),
        new Token("c", 2, 3),
        new Token("b", 4, 5),
        new Token("a", 0, 4, 5),
        new Token("d", 6, 7)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 3),
          new String[] { "b c b", "b c a", "a c b", "a c a", "c b d", "c a d" },
          new int[] {    0,        0,      0,       0,       2,        2,     },
          new int[] {    5,        5,      5,       5,       7,        7,     },
          new int[] {    1,        0,      0,       0,       1,        0,     });
  }

  public void testTrailingGraphsOfDifferingLengths() throws IOException {

    // a b:3/c d e f
    TokenStream ts = new CannedTokenStream(
        new Token("a", 0, 1),
        new Token("b", 1, 2, 3, 3),
        new Token("c", 0, 2, 3),
        new Token("d", 2, 3),
        new Token("e", 2, 3),
        new Token("f", 4, 5)
    );

    assertTokenStreamContents(new FixedShingleFilter(ts, 3),
        new String[]{ "a b f", "a c d", "c d e", "d e f"});

  }

  public void testParameterLimits() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      new FixedShingleFilter(new CannedTokenStream(), 1);
    });
    assertEquals("Shingle size must be between 2 and 4, got 1", e.getMessage());
    IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> {
      new FixedShingleFilter(new CannedTokenStream(), 5);
    });
    assertEquals("Shingle size must be between 2 and 4, got 5", e2.getMessage());
  }

  public void testWithGraphInput() throws IOException {

    TokenStream ts = new CannedTokenStream(
        new Token("fuz", 0, 3),
        new Token("foo", 1, 4, 6, 2),
        new Token("bar", 0, 4, 6),
        new Token("baz", 1, 4, 6)
    );
    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);
    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTokenStreamContents(new FixedShingleFilter(it.next(), 2), new String[]{ "fuz foo"});
    assertTokenStreamContents(new FixedShingleFilter(it.next(), 2), new String[]{ "fuz bar", "bar baz"});

  }

}
