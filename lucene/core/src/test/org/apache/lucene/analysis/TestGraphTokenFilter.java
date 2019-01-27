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

package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

public class TestGraphTokenFilter extends BaseTokenStreamTestCase {

  static class TestFilter extends GraphTokenFilter {

    public TestFilter(TokenStream input) {
      super(input);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      return incrementBaseToken();
    }
  }

  public void testGraphTokenStream() throws IOException {

    TestGraphTokenizers.GraphTokenizer tok = new TestGraphTokenizers.GraphTokenizer();
    GraphTokenFilter graph = new TestFilter(tok);

    CharTermAttribute termAtt = graph.addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncAtt = graph.addAttribute(PositionIncrementAttribute.class);

    tok.setReader(new StringReader("a b/c d e/f:3 g/h i j k"));
    tok.reset();

    assertFalse(graph.incrementGraph());
    assertEquals(0, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertEquals("a", termAtt.toString());
    assertEquals(1, posIncAtt.getPositionIncrement());
    assertTrue(graph.incrementGraphToken());
    assertEquals("b", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("d", termAtt.toString());
    assertTrue(graph.incrementGraph());
    assertEquals("a", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("c", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("d", termAtt.toString());
    assertFalse(graph.incrementGraph());
    assertEquals(5, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertEquals("b", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("d", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("e", termAtt.toString());
    assertTrue(graph.incrementGraph());
    assertEquals("b", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("d", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("f", termAtt.toString());
    assertFalse(graph.incrementGraph());
    assertEquals(6, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertEquals("c", termAtt.toString());
    assertEquals(0, posIncAtt.getPositionIncrement());
    assertTrue(graph.incrementGraphToken());
    assertEquals("d", termAtt.toString());
    assertFalse(graph.incrementGraph());
    assertEquals(6, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertEquals("d", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("e", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("g", termAtt.toString());
    assertTrue(graph.incrementGraph());
    assertEquals("d", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("e", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("h", termAtt.toString());
    assertTrue(graph.incrementGraph());
    assertEquals("d", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("f", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("j", termAtt.toString());
    assertFalse(graph.incrementGraph());
    assertEquals(8, graph.cachedTokenCount());

    //tok.setReader(new StringReader("a b/c d e/f:3 g/h i j k"));

    assertTrue(graph.incrementBaseToken());
    assertEquals("e", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("g", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("i", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("j", termAtt.toString());
    assertTrue(graph.incrementGraph());
    assertEquals("e", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("h", termAtt.toString());
    assertFalse(graph.incrementGraph());
    assertEquals(8, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertEquals("f", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("j", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("k", termAtt.toString());
    assertFalse(graph.incrementGraphToken());
    assertFalse(graph.incrementGraph());
    assertEquals(8, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertEquals("g", termAtt.toString());
    assertTrue(graph.incrementGraphToken());
    assertEquals("i", termAtt.toString());
    assertFalse(graph.incrementGraph());
    assertEquals(8, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertEquals("h", termAtt.toString());
    assertFalse(graph.incrementGraph());
    assertEquals(8, graph.cachedTokenCount());

    assertTrue(graph.incrementBaseToken());
    assertTrue(graph.incrementBaseToken());
    assertTrue(graph.incrementBaseToken());
    assertEquals("k", termAtt.toString());
    assertFalse(graph.incrementGraphToken());
    assertEquals(0, graph.getTrailingPositions());
    assertFalse(graph.incrementGraph());
    assertFalse(graph.incrementBaseToken());
    assertEquals(8, graph.cachedTokenCount());

  }

  public void testTrailingPositions() throws IOException {

    // a/b:2 c _
    CannedTokenStream cts = new CannedTokenStream(1, 5,
        new Token("a", 0, 1),
        new Token("b", 0, 0, 1, 2),
        new Token("c", 1, 2, 3)
    );

    GraphTokenFilter gts = new TestFilter(cts);
    assertFalse(gts.incrementGraph());
    assertTrue(gts.incrementBaseToken());
    assertTrue(gts.incrementGraphToken());
    assertFalse(gts.incrementGraphToken());
    assertEquals(1, gts.getTrailingPositions());
    assertFalse(gts.incrementGraph());
    assertTrue(gts.incrementBaseToken());
    assertFalse(gts.incrementGraphToken());
    assertEquals(1, gts.getTrailingPositions());
    assertFalse(gts.incrementGraph());
  }

  public void testMaximumGraphCacheSize() throws IOException {

    Token[] tokens = new Token[GraphTokenFilter.MAX_TOKEN_CACHE_SIZE + 5];
    for (int i = 0; i < GraphTokenFilter.MAX_TOKEN_CACHE_SIZE + 5; i++) {
      tokens[i] = new Token("a", 1, i * 2, i * 2 + 1);
    }

    GraphTokenFilter gts = new TestFilter(new CannedTokenStream(tokens));
    Exception e = expectThrows(IllegalStateException.class, () -> {
      gts.reset();
      gts.incrementBaseToken();
      while (true) {
        gts.incrementGraphToken();
      }
    });
    assertEquals("Too many cached tokens (> 100)", e.getMessage());

    gts.reset();
    // after reset, the cache should be cleared and so we can read ahead once more
    gts.incrementBaseToken();
    gts.incrementGraphToken();

  }

  public void testGraphPathCountLimits() {

    Token[] tokens = new Token[50];
    tokens[0] = new Token("term", 1, 0, 1);
    tokens[1] = new Token("term1", 1, 2, 3);
    for (int i = 2; i < 50; i++) {
      tokens[i] = new Token("term" + i, i % 2, 2, 3);
    }

    Exception e = expectThrows(IllegalStateException.class, () -> {
      GraphTokenFilter graph = new TestFilter(new CannedTokenStream(tokens));
      graph.reset();
      graph.incrementBaseToken();
      for (int i = 0; i < 10; i++) {
        graph.incrementGraphToken();
      }
      while (graph.incrementGraph()) {
        for (int i = 0; i < 10; i++) {
          graph.incrementGraphToken();
        }
      }
    });
    assertEquals("Too many graph paths (> 1000)", e.getMessage());
  }

}
