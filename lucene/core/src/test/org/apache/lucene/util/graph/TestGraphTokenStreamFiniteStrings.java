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
package org.apache.lucene.util.graph;

import java.util.Iterator;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;

/**
 * {@link GraphTokenStreamFiniteStrings} tests.
 */
public class TestGraphTokenStreamFiniteStrings extends LuceneTestCase {

  private static Token token(String term, int posInc, int posLength) {
    final Token t = new Token(term, 0, term.length());
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
  }

  private void assertTokenStream(TokenStream ts, String[] terms, int[] increments) throws Exception {
    // verify no nulls and arrays same length
    assertNotNull(ts);
    assertNotNull(terms);
    assertNotNull(increments);
    assertEquals(terms.length, increments.length);
    CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
    PositionIncrementAttribute incrAtt = ts.getAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute lenAtt = ts.getAttribute(PositionLengthAttribute.class);
    int offset = 0;
    while (ts.incrementToken()) {
      // verify term and increment
      assert offset < terms.length;
      assertEquals(terms[offset], termAtt.toString());
      assertEquals(increments[offset], incrAtt.getPositionIncrement());
      assertEquals(1, lenAtt.getPositionLength());  // we always output linear token streams
      offset++;
    }

    // make sure we processed all items
    assertEquals(offset, terms.length);
  }

  public void testIllegalState() throws Exception {
    expectThrows(IllegalStateException.class, () -> {
      TokenStream ts = new CannedTokenStream(
          token("a", 0, 1),
          token("b", 1, 1)
      );

      new GraphTokenStreamFiniteStrings(ts).getFiniteStrings();
    });
  }

  public void testEmpty() throws Exception {
    TokenStream ts = new CannedTokenStream(
    );
    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);
    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertFalse(it.hasNext());
    assertArrayEquals(new int[0], graph.articulationPoints());
  }

  public void testSingleGraph() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("fast", 1, 1),
        token("wi", 1, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 3});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "fast")});

    assertTrue(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 3);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wi", "fi"}, new int[]{1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wifi"}, new int[]{1});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(3));
    it = graph.getFiniteStrings(3, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 3);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }

  public void testSingleGraphWithGap() throws Exception {
    // "hey the fast wifi network", where "the" removed
    TokenStream ts = new CannedTokenStream(
        token("hey", 1, 1),
        token("fast", 2, 1),
        token("wi", 1, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(),
        new String[]{"hey", "fast", "wi", "fi", "network"}, new int[]{1, 2, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(),
        new String[]{"hey", "fast", "wifi", "network"}, new int[]{1, 2, 1, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 2, 4});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"hey"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "hey")});

    assertFalse(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 2);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast"}, new int[] {2});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 1);
    assertArrayEquals(terms, new Term[] {new Term("field", "fast")});

    assertTrue(graph.hasSidePath(2));
    it = graph.getFiniteStrings(2, 4);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wi", "fi"}, new int[]{1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wifi"}, new int[]{1});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(4));
    it = graph.getFiniteStrings(4, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 4);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }


  public void testGraphAndGapSameToken() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("fast", 1, 1),
        token("wi", 2, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 2, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wifi", "network"}, new int[]{1, 2, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 3});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "fast")});

    assertTrue(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 3);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wi", "fi"}, new int[]{2, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wifi"}, new int[]{2});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(3));
    it = graph.getFiniteStrings(3, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 3);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }

  public void testGraphAndGapSameTokenTerm() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("a", 1, 1),
        token("b", 1, 1),
        token("c", 2, 1),
        token("a", 0, 2),
        token("d", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"a", "b", "c", "d"}, new int[]{1, 1, 2, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"a", "b", "a"}, new int[]{1, 1, 2});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 2});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"a"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "a")});

    assertFalse(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 2);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"b"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 1);
    assertArrayEquals(terms, new Term[] {new Term("field", "b")});

    assertTrue(graph.hasSidePath(2));
    it = graph.getFiniteStrings(2, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"c", "d"}, new int[] {2, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"a"}, new int[] {2});
    assertFalse(it.hasNext());
  }

  public void testStackedGraph() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("fast", 1, 1),
        token("wi", 1, 1),
        token("wifi", 0, 2),
        token("wireless", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wireless", "network"}, new int[]{1, 1, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 3});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "fast")});

    assertTrue(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 3);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wi", "fi"}, new int[]{1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wifi"}, new int[]{1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wireless"}, new int[]{1});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(3));
    it = graph.getFiniteStrings(3, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 3);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }

  public void testStackedGraphWithGap() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("fast", 1, 1),
        token("wi", 2, 1),
        token("wifi", 0, 2),
        token("wireless", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 2, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wifi", "network"}, new int[]{1, 2, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wireless", "network"}, new int[]{1, 2, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 3});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "fast")});

    assertTrue(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 3);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wi", "fi"}, new int[]{2, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wifi"}, new int[]{2});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wireless"}, new int[]{2});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(3));
    it = graph.getFiniteStrings(3, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 3);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }

  public void testStackedGraphWithRepeat() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("ny", 1, 4),
        token("new", 0, 1),
        token("new", 0, 3),
        token("york", 1, 1),
        token("city", 1, 2),
        token("york", 1, 1),
        token("is", 1, 1),
        token("great", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"ny", "is", "great"}, new int[]{1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"new", "york", "city", "is", "great"}, new int[]{1, 1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"new", "york", "is", "great"}, new int[]{1, 1, 1, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {4, 5});

    assertTrue(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 4);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"ny"}, new int[]{1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"new", "york", "city"}, new int[]{1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"new", "york"}, new int[]{1, 1});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(4));
    it = graph.getFiniteStrings(4, 5);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"is"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 4);
    assertArrayEquals(terms, new Term[] {new Term("field", "is")});

    assertFalse(graph.hasSidePath(5));
    it = graph.getFiniteStrings(5, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"great"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 5);
    assertArrayEquals(terms, new Term[] {new Term("field", "great")});
  }

  public void testGraphWithRegularSynonym() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("fast", 1, 1),
        token("speedy", 0, 1),
        token("wi", 1, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"speedy", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"speedy", "wifi", "network"}, new int[]{1, 1, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 3});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast"}, new int[] {1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"speedy"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "fast"), new Term("field", "speedy")});

    assertTrue(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 3);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wi", "fi"}, new int[]{1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wifi"}, new int[]{1});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(3));
    it = graph.getFiniteStrings(3, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[] {1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 3);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }

  public void testMultiGraph() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("turbo", 1, 1),
        token("fast", 0, 2),
        token("charged", 1, 1),
        token("wi", 1, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(),
        new String[]{"turbo", "charged", "wi", "fi", "network"}, new int[]{1, 1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(),
        new String[]{"turbo", "charged", "wifi", "network"}, new int[]{1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {2, 4});

    assertTrue(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 2);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"turbo", "charged"}, new int[]{1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"fast"}, new int[]{1});
    assertFalse(it.hasNext());

    assertTrue(graph.hasSidePath(2));
    it = graph.getFiniteStrings(2, 4);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wi", "fi"}, new int[]{1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"wifi"}, new int[]{1});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(4));
    it = graph.getFiniteStrings(4, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[] {1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 4);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }

  public void testMultipleSidePaths() throws Exception {
    // 0   1        2    3         4    5  6         7  8
    // the ny:4/new york wifi:5/wi fi:4 [] wifi:2/wi fi network
    TokenStream ts = new CannedTokenStream(
        token("the", 1, 1),
        token("ny", 1, 4),
        token("new", 0, 1),
        token("york", 1, 1),
        token("wifi", 1, 5),
        token("wi", 0, 1),
        token("fi", 1, 4),
        token("wifi", 2, 2),
        token("wi", 0, 1),
        token("fi", 1, 1),
        token("network", 1, 1)
    );
    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(ts);

    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"the", "ny", "wifi", "network"}, new int[]{1, 1, 2, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"the", "ny", "wi", "fi", "network"}, new int[]{1, 1, 2, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"the", "new", "york", "wifi", "network"}, new int[]{1, 1, 1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"the", "new", "york", "wi", "fi", "network"}, new int[]{1, 1, 1, 1, 1, 1});
    assertFalse(it.hasNext());

    int[] points = graph.articulationPoints();
    assertArrayEquals(points, new int[] {1, 7});

    assertFalse(graph.hasSidePath(0));
    it = graph.getFiniteStrings(0, 1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"the"}, new int[]{1});
    assertFalse(it.hasNext());
    Term[] terms = graph.getTerms("field", 0);
    assertArrayEquals(terms, new Term[] {new Term("field", "the")});

    assertTrue(graph.hasSidePath(1));
    it = graph.getFiniteStrings(1, 7);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"ny", "wifi"}, new int[]{1, 2});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"ny", "wi", "fi"}, new int[]{1, 2, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"new", "york", "wifi"}, new int[]{1, 1, 1});
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"new", "york", "wi", "fi"}, new int[]{1, 1, 1, 1});
    assertFalse(it.hasNext());

    assertFalse(graph.hasSidePath(7));
    it = graph.getFiniteStrings(7, -1);
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{"network"}, new int[]{1});
    assertFalse(it.hasNext());
    terms = graph.getTerms("field", 7);
    assertArrayEquals(terms, new Term[] {new Term("field", "network")});
  }

  public void testSidePathWithGap() throws Exception {
    // 0    1               2  3  4             5
    // king alfred:3/alfred [] [] great/awesome ruled
    CannedTokenStream cts = new CannedTokenStream(
        token("king", 1, 1),
        token("alfred", 1, 4),
        token("alfred", 0, 1),
        token("great", 3, 1),
        token("awesome", 0, 1),
        token("ruled", 1, 1)
    );
    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(cts);
    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{ "king", "alfred", "ruled" }, new int[]{ 1, 1, 1 });
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{ "king", "alfred", "great", "ruled"}, new int[]{ 1, 1, 3, 1 });
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{ "king", "alfred", "awesome", "ruled"}, new int[]{ 1, 1, 3, 1 });
    assertFalse(it.hasNext());
  }

  public void testMultipleSidePathsWithGaps() throws Exception {
    // king alfred:4/alfred [] [] saxons:3 [] wessex ruled
    CannedTokenStream cts = new CannedTokenStream(
        token("king", 1, 1),
        token("alfred", 1, 4),
        token("alfred", 0, 1),
        token("saxons", 3, 3),
        token("wessex", 2, 1),
        token("ruled", 1, 1)
    );
    GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(cts);
    Iterator<TokenStream> it = graph.getFiniteStrings();
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{ "king", "alfred", "wessex", "ruled" }, new int[]{ 1, 1, 2, 1 });
    assertTrue(it.hasNext());
    assertTokenStream(it.next(), new String[]{ "king", "alfred", "saxons", "ruled" }, new int[]{ 1, 1, 3, 1 });
    assertFalse(it.hasNext());
  }

}
