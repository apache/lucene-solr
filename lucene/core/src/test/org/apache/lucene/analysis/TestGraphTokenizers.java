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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.fst.Util;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

public class TestGraphTokenizers extends BaseTokenStreamTestCase {

  // Makes a graph TokenStream from the string; separate
  // positions with single space, multiple tokens at the same
  // position with /, and add optional position length with
  // :.  EG "a b c" is a simple chain, "a/x b c" adds 'x'
  // over 'a' at position 0 with posLen=1, "a/x:3 b c" adds
  // 'x' over a with posLen=3.  Tokens are in normal-form!
  // So, offsets are computed based on the first token at a
  // given position.  NOTE: each token must be a single
  // character!  We assume this when computing offsets...
  
  // NOTE: all input tokens must be length 1!!!  This means
  // you cannot turn on MockCharFilter when random
  // testing...

  public static final class GraphTokenizer extends Tokenizer {
    private List<Token> tokens;
    private int upto;
    private int inputLength;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);

    @Override
    public void reset() throws IOException {
      super.reset();
      tokens = null;
      upto = 0;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (tokens == null) {
        fillTokens();
      }
      //System.out.println("graphTokenizer: incr upto=" + upto + " vs " + tokens.size());
      if (upto == tokens.size()) {
        //System.out.println("  END @ " + tokens.size());
        return false;
      } 
      final Token t = tokens.get(upto++);
      //System.out.println("  return token=" + t);
      clearAttributes();
      termAtt.append(t.toString());
      offsetAtt.setOffset(t.startOffset(), t.endOffset());
      posIncrAtt.setPositionIncrement(t.getPositionIncrement());
      posLengthAtt.setPositionLength(t.getPositionLength());
      return true;
    }

    @Override
    public void end() throws IOException {
      super.end();
      // NOTE: somewhat... hackish, but we need this to
      // satisfy BTSTC:
      final int lastOffset;
      if (tokens != null && !tokens.isEmpty()) {
        lastOffset = tokens.get(tokens.size()-1).endOffset();
      } else {
        lastOffset = 0;
      }
      offsetAtt.setOffset(correctOffset(lastOffset),
                          correctOffset(inputLength));
    }

    private void fillTokens() throws IOException {
      final StringBuilder sb = new StringBuilder();
      final char[] buffer = new char[256];
      while (true) {
        final int count = input.read(buffer);
        if (count == -1) {
          break;
        }
        sb.append(buffer, 0, count);
        //System.out.println("got count=" + count);
      }
      //System.out.println("fillTokens: " + sb);

      inputLength = sb.length();

      final String[] parts = sb.toString().split(" ");

      tokens = new ArrayList<>();
      int pos = 0;
      int maxPos = -1;
      int offset = 0;
      //System.out.println("again");
      for(String part : parts) {
        final String[] overlapped = part.split("/");
        boolean firstAtPos = true;
        int minPosLength = Integer.MAX_VALUE;
        for(String part2 : overlapped) {
          final int colonIndex = part2.indexOf(':');
          final String token;
          final int posLength;
          if (colonIndex != -1) {
            token = part2.substring(0, colonIndex);
            posLength = Integer.parseInt(part2.substring(1+colonIndex));
          } else {
            token = part2;
            posLength = 1;
          }
          maxPos = Math.max(maxPos, pos + posLength);
          minPosLength = Math.min(minPosLength, posLength);
          final Token t = new Token(token, offset, offset + 2*posLength - 1);
          t.setPositionLength(posLength);
          t.setPositionIncrement(firstAtPos ? 1:0);
          firstAtPos = false;
          //System.out.println("  add token=" + t + " startOff=" + t.startOffset() + " endOff=" + t.endOffset());
          tokens.add(t);
        }
        pos += minPosLength;
        offset = 2 * pos;
      }
      assert maxPos <= pos: "input string mal-formed: posLength>1 tokens hang over the end";
    }
  }

  public void testMockGraphTokenFilterBasic() throws Exception {

    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            final TokenStream t2 = new MockGraphTokenFilter(random(), t);
            return new TokenStreamComponents(t, t2);
          }
        };
      
      checkAnalysisConsistency(random(), a, false, "a b c d e f g h i j k");
    }
  }

  public void testMockGraphTokenFilterOnGraphInput() throws Exception {
    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new GraphTokenizer();
            final TokenStream t2 = new MockGraphTokenFilter(random(), t);
            return new TokenStreamComponents(t, t2);
          }
        };
      
      checkAnalysisConsistency(random(), a, false, "a/x:3 c/y:2 d e f/z:4 g h i j k");
    }
  }

  // Just deletes (leaving hole) token 'a':
  private final static class RemoveATokens extends TokenFilter {
    private int pendingPosInc;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

    public RemoveATokens(TokenStream in) {
      super(in);
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      pendingPosInc = 0;
    }

    @Override
    public void end() throws IOException {
      super.end();
      posIncAtt.setPositionIncrement(pendingPosInc + posIncAtt.getPositionIncrement());
    }

    @Override
    public boolean incrementToken() throws IOException {
      while (true) {
        final boolean gotOne = input.incrementToken();
        if (!gotOne) {
          return false;
        } else if (termAtt.toString().equals("a")) {
          pendingPosInc += posIncAtt.getPositionIncrement();
        } else {
          posIncAtt.setPositionIncrement(pendingPosInc + posIncAtt.getPositionIncrement());
          pendingPosInc = 0;
          return true;
        }
      }
    }
  }

  public void testMockGraphTokenFilterBeforeHoles() throws Exception {
    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            final TokenStream t2 = new MockGraphTokenFilter(random(), t);
            final TokenStream t3 = new RemoveATokens(t2);
            return new TokenStreamComponents(t, t3);
          }
        };

      Random random = random();
      checkAnalysisConsistency(random, a, false, "a b c d e f g h i j k");
      checkAnalysisConsistency(random, a, false, "x y a b c d e f g h i j k");
      checkAnalysisConsistency(random, a, false, "a b c d e f g h i j k a");
      checkAnalysisConsistency(random, a, false, "a b c d e f g h i j k a x y");
    }
  }

  public void testMockGraphTokenFilterAfterHoles() throws Exception {
    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            final TokenStream t2 = new RemoveATokens(t);
            final TokenStream t3 = new MockGraphTokenFilter(random(), t2);
            return new TokenStreamComponents(t, t3);
          }
        };

      Random random = random();
      checkAnalysisConsistency(random, a, false, "a b c d e f g h i j k");
      checkAnalysisConsistency(random, a, false, "x y a b c d e f g h i j k");
      checkAnalysisConsistency(random, a, false, "a b c d e f g h i j k a");
      checkAnalysisConsistency(random, a, false, "a b c d e f g h i j k a x y");
    }
  }

  public void testMockGraphTokenFilterRandom() throws Exception {
    for(int iter=0;iter<3*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            final TokenStream t2 = new MockGraphTokenFilter(random(), t);
            return new TokenStreamComponents(t, t2);
          }
        };
      
      Random random = random();
      checkRandomData(random, a, 5, atLeast(100));
    }
  }

  // Two MockGraphTokenFilters
  public void testDoubleMockGraphTokenFilterRandom() throws Exception {
    for(int iter=0;iter<3*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            final TokenStream t1 = new MockGraphTokenFilter(random(), t);
            final TokenStream t2 = new MockGraphTokenFilter(random(), t1);
            return new TokenStreamComponents(t, t2);
          }
        };
      
      Random random = random();
      checkRandomData(random, a, 5, atLeast(100));
    }
  }

  public void testMockGraphTokenFilterBeforeHolesRandom() throws Exception {
    for(int iter=0;iter<3*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            final TokenStream t1 = new MockGraphTokenFilter(random(), t);
            final TokenStream t2 = new MockHoleInjectingTokenFilter(random(), t1);
            return new TokenStreamComponents(t, t2);
          }
        };
      
      Random random = random();
      checkRandomData(random, a, 5, atLeast(100));
    }
  }

  public void testMockGraphTokenFilterAfterHolesRandom() throws Exception {
    for(int iter=0;iter<3*RANDOM_MULTIPLIER;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }

      // Make new analyzer each time, because MGTF has fixed
      // seed:
      final Analyzer a = new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            final TokenStream t1 = new MockHoleInjectingTokenFilter(random(), t);
            final TokenStream t2 = new MockGraphTokenFilter(random(), t1);
            return new TokenStreamComponents(t, t2);
          }
        };
      
      Random random = random();
      checkRandomData(random, a, 5, atLeast(100));
    }
  }

  private static Token token(String term, int posInc, int posLength) {
    final Token t = new Token(term, 0, 0);
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
  }

  private static Token token(String term, int posInc, int posLength, int startOffset, int endOffset) {
    final Token t = new Token(term, startOffset, endOffset);
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
  }

  public void testSingleToken() throws Exception {
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 1, 1),
      });
    assertSameLanguage(s2a("abc"), ts);
  }

  public void testMultipleHoles() throws Exception {
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("a", 1, 1),
        token("b", 3, 1),
      });
    assertSameLanguage(join(s2a("a"), SEP_A, HOLE_A, SEP_A, HOLE_A, SEP_A, s2a("b")), ts);
  }

  public void testSynOverMultipleHoles() throws Exception {
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("a", 1, 1),
        token("x", 0, 3),
        token("b", 3, 1),
      });
    final Automaton a1 = join(s2a("a"), SEP_A, HOLE_A, SEP_A, HOLE_A, SEP_A, s2a("b")); 
    final Automaton a2 = join(s2a("x"), SEP_A, s2a("b")); 
    assertSameLanguage(Operations.union(a1, a2), ts);
  }

  // for debugging!
  /*
  private static void toDot(Automaton a) throws IOException {
    final String s = a.toDot();
    Writer w = new OutputStreamWriter(new FileOutputStream("/x/tmp/out.dot"));
    w.write(s);
    w.close();
    System.out.println("TEST: saved to /x/tmp/out.dot");
  }
  */

  private static final Automaton SEP_A = Automata.makeChar(TokenStreamToAutomaton.POS_SEP);
  private static final Automaton HOLE_A = Automata.makeChar(TokenStreamToAutomaton.HOLE);

  private Automaton join(String ... strings) {
    List<Automaton> as = new ArrayList<>();
    for(String s : strings) {
      as.add(s2a(s));
      as.add(SEP_A);
    }
    as.remove(as.size()-1);
    return Operations.concatenate(as);
  }

  private Automaton join(Automaton ... as) {
    return Operations.concatenate(Arrays.asList(as));
  }

  private Automaton s2a(String s) {
    return Automata.makeString(s);
  }

  public void testTwoTokens() throws Exception {
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 1, 1),
        token("def", 1, 1),
      });
    assertSameLanguage(join("abc", "def"), ts);
  }

  public void testHole() throws Exception {

    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 1, 1),
        token("def", 2, 1),
      });
    assertSameLanguage(join(s2a("abc"), SEP_A, HOLE_A, SEP_A, s2a("def")), ts);
  }

  public void testOverlappedTokensSausage() throws Exception {

    // Two tokens on top of each other (sausage):
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 1, 1),
        token("xyz", 0, 1)
      });
    final Automaton a1 = s2a("abc");
    final Automaton a2 = s2a("xyz");
    assertSameLanguage(Operations.union(a1, a2), ts);
  }

  public void testOverlappedTokensLattice() throws Exception {

    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 1, 1),
        token("xyz", 0, 2),
        token("def", 1, 1),
      });
    final Automaton a1 = s2a("xyz");
    final Automaton a2 = join("abc", "def");
    assertSameLanguage(Operations.union(a1, a2), ts);
  }

  public void testSynOverHole() throws Exception {

    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("a", 1, 1),
        token("X", 0, 2),
        token("b", 2, 1),
      });
    final Automaton a1 = Operations.union(join(s2a("a"), SEP_A, HOLE_A), s2a("X"));
    final Automaton expected = Operations.concatenate(a1, join(SEP_A, s2a("b")));
    assertSameLanguage(expected, ts);
  }

  public void testSynOverHole2() throws Exception {

    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("xyz", 1, 1),
        token("abc", 0, 3),
        token("def", 2, 1),
      });
    final Automaton expected = Operations.union(
      join(s2a("xyz"), SEP_A, HOLE_A, SEP_A, s2a("def")), s2a("abc"));
    assertSameLanguage(expected, ts);
  }

  public void testOverlappedTokensLattice2() throws Exception {

    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 1, 1),
        token("xyz", 0, 3),
        token("def", 1, 1),
        token("ghi", 1, 1),
      });
    final Automaton a1 = s2a("xyz");
    final Automaton a2 = join("abc", "def", "ghi");
    assertSameLanguage(Operations.union(a1, a2), ts);
  }

  public void testToDot() throws Exception {
    final TokenStream ts = new CannedTokenStream(new Token[] {token("abc", 1, 1, 0, 4)});
    StringWriter w = new StringWriter();
    new TokenStreamToDot("abcd", ts, new PrintWriter(w)).toDot();
    assertTrue(w.toString().indexOf("abc / abcd") != -1);
  }

  public void testStartsWithHole() throws Exception {
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 2, 1),
      });
    assertSameLanguage(join(HOLE_A, SEP_A, s2a("abc")), ts);
  }

  public void testEndsWithHole() throws Exception {
    final TokenStream ts = new CannedTokenStream(1, 0,
                                                 new Token[] {
                                                   token("abc", 2, 1),
                                                 });
    assertSameLanguage(join(HOLE_A, SEP_A, s2a("abc"), SEP_A, HOLE_A), ts);
  }

  public void testSynHangingOverEnd() throws Exception {
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("a", 1, 1),
        token("X", 0, 10),
      });
    assertSameLanguage(Operations.union(s2a("a"), s2a("X")), ts);
  }

  /** Returns all paths */
  private Set<String> toPathStrings(Automaton a) {
    BytesRefBuilder scratchBytesRefBuilder = new BytesRefBuilder();
    Set<String> paths = new HashSet<>();
    for (IntsRef ir: AutomatonTestUtil.getFiniteStringsRecursive(a, -1)) {
      paths.add(Util.toBytesRef(ir, scratchBytesRefBuilder).utf8ToString().replace((char) TokenStreamToAutomaton.POS_SEP, ' '));
    }
    return paths;
  }

  private void assertSameLanguage(Automaton expected, TokenStream ts) throws IOException {
    assertSameLanguage(expected, new TokenStreamToAutomaton().toAutomaton(ts));
  }

  private void assertSameLanguage(Automaton expected, Automaton actual) {
    Automaton expectedDet = Operations.determinize(Operations.removeDeadStates(expected), DEFAULT_DETERMINIZE_WORK_LIMIT);
    Automaton actualDet = Operations.determinize(Operations.removeDeadStates(actual), DEFAULT_DETERMINIZE_WORK_LIMIT);
    if (Operations.sameLanguage(expectedDet, actualDet) == false) {
      Set<String> expectedPaths = toPathStrings(expectedDet);
      Set<String> actualPaths = toPathStrings(actualDet);
      StringBuilder b = new StringBuilder();
      b.append("expected:\n");
      for(String path : expectedPaths) {
        b.append("  ");
        b.append(path);
        if (actualPaths.contains(path) == false) {
          b.append(" [missing!]");
        }
        b.append('\n');
      }
      b.append("actual:\n");
      for(String path : actualPaths) {
        b.append("  ");
        b.append(path);
        if (expectedPaths.contains(path) == false) {
          b.append(" [unexpected!]");
        }
        b.append('\n');
      }
      fail("accepted language is different:\n\n" + b.toString());
    }
  }

  public void testTokenStreamGraphWithHoles() throws Exception {
    final TokenStream ts = new CannedTokenStream(
      new Token[] {
        token("abc", 1, 1),
        token("xyz", 1, 8),
        token("def", 1, 1),
        token("ghi", 1, 1),
      });
    assertSameLanguage(Operations.union(join(s2a("abc"), SEP_A, s2a("xyz")),
                                        join(s2a("abc"), SEP_A, HOLE_A, SEP_A, s2a("def"), SEP_A, s2a("ghi"))), ts);
  }
}
