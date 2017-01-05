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

import java.util.List;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
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
    BytesTermAttribute termAtt = ts.getAttribute(BytesTermAttribute.class);
    PositionIncrementAttribute incrAtt = ts.getAttribute(PositionIncrementAttribute.class);
    int offset = 0;
    while (ts.incrementToken()) {
      // verify term and increment
      assert offset < terms.length;
      assertEquals(terms[offset], termAtt.getBytesRef().utf8ToString());
      assertEquals(increments[offset], incrAtt.getPositionIncrement());
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

      GraphTokenStreamFiniteStrings.getTokenStreams(ts);
    });
  }

  public void testSingleGraph() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("fast", 1, 1),
        token("wi", 1, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(2, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(1), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
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

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(2, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0),
        new String[]{"hey", "fast", "wi", "fi", "network"}, new int[]{1, 2, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(1),
        new String[]{"hey", "fast", "wifi", "network"}, new int[]{1, 2, 1, 1});
  }


  public void testGraphAndGapSameToken() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("fast", 1, 1),
        token("wi", 2, 1),
        token("wifi", 0, 2),
        token("fi", 1, 1),
        token("network", 1, 1)
    );

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(2, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 2, 1, 1});
    assertTokenStream(finiteTokenStreams.get(1), new String[]{"fast", "wifi", "network"}, new int[]{1, 2, 1});
  }

  public void testGraphAndGapSameTokenTerm() throws Exception {
    TokenStream ts = new CannedTokenStream(
        token("a", 1, 1),
        token("b", 1, 1),
        token("c", 2, 1),
        token("a", 0, 2),
        token("d", 1, 1)
    );

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(2, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0), new String[]{"a", "b", "c", "d"}, new int[]{1, 1, 2, 1});
    assertTokenStream(finiteTokenStreams.get(1), new String[]{"a", "b", "a"}, new int[]{1, 1, 2});
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

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(3, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(1), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(2), new String[]{"fast", "wireless", "network"}, new int[]{1, 1, 1});
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

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(3, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 2, 1, 1});
    assertTokenStream(finiteTokenStreams.get(1), new String[]{"fast", "wifi", "network"}, new int[]{1, 2, 1});
    assertTokenStream(finiteTokenStreams.get(2), new String[]{"fast", "wireless", "network"}, new int[]{1, 2, 1});
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

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(4, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(1), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(2), new String[]{"speedy", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(3), new String[]{"speedy", "wifi", "network"}, new int[]{1, 1, 1});
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

    List<TokenStream> finiteTokenStreams = GraphTokenStreamFiniteStrings.getTokenStreams(ts);

    assertEquals(4, finiteTokenStreams.size());
    assertTokenStream(finiteTokenStreams.get(0),
        new String[]{"turbo", "charged", "wi", "fi", "network"}, new int[]{1, 1, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(1),
        new String[]{"turbo", "charged", "wifi", "network"}, new int[]{1, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(2), new String[]{"fast", "wi", "fi", "network"}, new int[]{1, 1, 1, 1});
    assertTokenStream(finiteTokenStreams.get(3), new String[]{"fast", "wifi", "network"}, new int[]{1, 1, 1});
  }
}
