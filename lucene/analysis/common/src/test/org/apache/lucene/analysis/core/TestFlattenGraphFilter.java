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

package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AutomatonToTokenStream;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenStreamToAutomaton;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;

public class TestFlattenGraphFilter extends BaseTokenStreamTestCase {
  
  private static Token token(String term, int posInc, int posLength, int startOffset, int endOffset) {
    final Token t = new Token(term, startOffset, endOffset);
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
  }

  public void testSimpleMock() throws Exception {
    Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
          TokenStream ts = new FlattenGraphFilter(tokenizer);
          return new TokenStreamComponents(tokenizer, ts);
        }
      };

    assertAnalyzesTo(a, "wtf happened",
                     new String[] {"wtf", "happened"},
                     new int[]    {    0,          4},
                     new int[]    {    3,         12},
                     null,
                     new int[]    {    1,          1},
                     new int[]    {    1,          1},
                     true);
  }

  // Make sure graph is unchanged if it's already flat
  public void testAlreadyFlatten() throws Exception {
    TokenStream in = new CannedTokenStream(0, 12, new Token[] {
        token("wtf", 1, 1, 0, 3),
        token("what", 0, 1, 0, 3),
        token("wow", 0, 1, 0, 3),
        token("the", 1, 1, 0, 3),
        token("that's", 0, 1, 0, 3),
        token("fudge", 1, 1, 0, 3),
        token("funny", 0, 1, 0, 3),
        token("happened", 1, 1, 4, 12)
      });

    TokenStream out = new FlattenGraphFilter(in);

    // ... but on output, it's flattened to wtf/what/wow that's/the fudge/funny happened:
    assertTokenStreamContents(out,
                              new String[] {"wtf", "what", "wow", "the", "that's", "fudge", "funny", "happened"},
                              new int[] {0, 0, 0, 0, 0, 0, 0, 4},
                              new int[] {3, 3, 3, 3, 3, 3, 3, 12},
                              new int[] {1, 0, 0, 1, 0, 1, 0, 1},
                              new int[] {1, 1, 1, 1, 1, 1, 1, 1},
                              12);
  }

  public void testWTF1() throws Exception {

    // "wow that's funny" and "what the fudge" are separate side paths, in parallel with "wtf", on input:
    TokenStream in = new CannedTokenStream(0, 12, new Token[] {
        token("wtf", 1, 5, 0, 3),
        token("what", 0, 1, 0, 3),
        token("wow", 0, 3, 0, 3),
        token("the", 1, 1, 0, 3),
        token("fudge", 1, 3, 0, 3),
        token("that's", 1, 1, 0, 3),
        token("funny", 1, 1, 0, 3),
        token("happened", 1, 1, 4, 12)
      });


    TokenStream out = new FlattenGraphFilter(in);

    // ... but on output, it's flattened to wtf/what/wow that's/the fudge/funny happened:
    assertTokenStreamContents(out,
                              new String[] {"wtf", "what", "wow", "the", "that's", "fudge", "funny", "happened"},
                              new int[] {0, 0, 0, 0, 0, 0, 0, 4},
                              new int[] {3, 3, 3, 3, 3, 3, 3, 12},
                              new int[] {1, 0, 0, 1, 0, 1, 0, 1},
                              new int[] {3, 1, 1, 1, 1, 1, 1, 1},
                              12);
    
  }

  /** Same as testWTF1 except the "wtf" token comes out later */
  public void testWTF2() throws Exception {

    // "wow that's funny" and "what the fudge" are separate side paths, in parallel with "wtf", on input:
    TokenStream in = new CannedTokenStream(0, 12, new Token[] {
        token("what", 1, 1, 0, 3),
        token("wow", 0, 3, 0, 3),
        token("wtf", 0, 5, 0, 3),
        token("the", 1, 1, 0, 3),
        token("fudge", 1, 3, 0, 3),
        token("that's", 1, 1, 0, 3),
        token("funny", 1, 1, 0, 3),
        token("happened", 1, 1, 4, 12)
      });


    TokenStream out = new FlattenGraphFilter(in);

    // ... but on output, it's flattened to wtf/what/wow that's/the fudge/funny happened:
    assertTokenStreamContents(out,
                              new String[] {"what", "wow", "wtf", "the", "that's", "fudge", "funny", "happened"},
                              new int[] {0, 0, 0, 0, 0, 0, 0, 4},
                              new int[] {3, 3, 3, 3, 3, 3, 3, 12},
                              new int[] {1, 0, 0, 1, 0, 1, 0, 1},
                              new int[] {1, 1, 3, 1, 1, 1, 1, 1},
                              12);
    
  }

  public void testNonGreedySynonyms() throws Exception {
    // This is just "hypothetical" for Lucene today, because SynFilter is
    // greedy: when two syn rules match on overlapping tokens, only one
    // (greedily) wins.  This test pretends all syn matches could match:

    TokenStream in = new CannedTokenStream(0, 20, new Token[] {
        token("wizard", 1, 1, 0, 6),
        token("wizard_of_oz", 0, 3, 0, 12),
        token("of", 1, 1, 7, 9),
        token("oz", 1, 1, 10, 12),
        token("oz_screams", 0, 2, 10, 20),
        token("screams", 1, 1, 13, 20),
      });


    TokenStream out = new FlattenGraphFilter(in);

    // ... but on output, it's flattened to wtf/what/wow that's/the fudge/funny happened:
    assertTokenStreamContents(out,
                              new String[] {"wizard", "wizard_of_oz", "of", "oz", "oz_screams", "screams"},
                              new int[] {0, 0, 7, 10, 10, 13},
                              new int[] {6, 12, 9, 12, 20, 20},
                              new int[] {1, 0, 1, 1, 0, 1},
                              new int[] {1, 3, 1, 1, 2, 1},
                              20);
    
  }

  public void testNonGraph() throws Exception {
    TokenStream in = new CannedTokenStream(0, 22, new Token[] {
        token("hello", 1, 1, 0, 5),
        token("pseudo", 1, 1, 6, 12),
        token("world", 1, 1, 13, 18),
        token("fun", 1, 1, 19, 22),
      });


    TokenStream out = new FlattenGraphFilter(in);

    // ... but on output, it's flattened to wtf/what/wow that's/the fudge/funny happened:
    assertTokenStreamContents(out,
                              new String[] {"hello", "pseudo", "world", "fun"},
                              new int[] {0, 6, 13, 19},
                              new int[] {5, 12, 18, 22},
                              new int[] {1, 1, 1, 1},
                              new int[] {1, 1, 1, 1},
                              22);
  }

  public void testSimpleHole() throws Exception {
    TokenStream in = new CannedTokenStream(0, 13, new Token[] {
        token("hello", 1, 1, 0, 5),
        token("hole", 2, 1, 6, 10),
        token("fun", 1, 1, 11, 13),
      });


    TokenStream out = new FlattenGraphFilter(in);

    // ... but on output, it's flattened to wtf/what/wow that's/the fudge/funny happened:
    assertTokenStreamContents(out,
                              new String[] {"hello", "hole", "fun"},
                              new int[] {0, 6, 11},
                              new int[] {5, 10, 13},
                              new int[] {1, 2, 1},
                              new int[] {1, 1, 1},
                              13);
  }

  public void testHoleUnderSyn() throws Exception {
    // Tests a StopFilter after SynFilter where a stopword in a syn is removed
    //
    //   wizard of oz -> woz syn, but then "of" becomes a hole

    TokenStream in = new CannedTokenStream(0, 12, new Token[] {
        token("wizard", 1, 1, 0, 6),
        token("woz", 0, 3, 0, 12),
        token("oz", 2, 1, 10, 12),
      });


    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(out,
                              new String[] {"wizard", "woz", "oz"},
                              new int[] {0, 0, 10},
                              new int[] {6, 12, 12},
                              new int[] {1, 0, 2},
                              new int[] {1, 3, 1},
                              12);
  }

  public void testStrangelyNumberedNodes() throws Exception {

    // Uses only nodes 0, 2, 3, i.e. 1 is just never used (it is not a hole!!)
    TokenStream in = new CannedTokenStream(0, 27, new Token[] {
        token("dog", 1, 3, 0, 5),
        token("puppy", 0, 3, 0, 5),
        token("flies", 3, 1, 6, 11),
      });

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(out,
                              new String[] {"dog", "puppy", "flies"},
                              new int[] {0, 0, 6},
                              new int[] {5, 5, 11},
                              new int[] {1, 0, 1},
                              new int[] {1, 1, 1},
                              27);
  }

  public void testTwoLongParallelPaths() throws Exception {

    // "a a a a a a" in parallel with "b b b b b b"
    TokenStream in = new CannedTokenStream(0, 11, new Token[] {
        token("a", 1, 1, 0, 1),
        token("b", 0, 2, 0, 1),
        token("a", 1, 2, 2, 3),
        token("b", 1, 2, 2, 3),
        token("a", 1, 2, 4, 5),
        token("b", 1, 2, 4, 5),
        token("a", 1, 2, 6, 7),
        token("b", 1, 2, 6, 7),
        token("a", 1, 2, 8, 9),
        token("b", 1, 2, 8, 9),
        token("a", 1, 2, 10, 11),
        token("b", 1, 2, 10, 11),
      });


    TokenStream out = new FlattenGraphFilter(in);
    
    // ... becomes flattened to a single path with overlapping a/b token between each node:
    assertTokenStreamContents(out,
                              new String[] {"a", "b", "a", "b", "a", "b", "a", "b", "a", "b", "a", "b"},
                              new int[] {0, 0, 2, 2, 4, 4, 6, 6, 8, 8, 10, 10},
                              new int[] {1, 1, 3, 3, 5, 5, 7, 7, 9, 9, 11, 11},
                              new int[] {1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0},
                              new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
                              11);
    
  }

  // b has a posInc of 1, which is correct, but no edge ever visited that node.
  // After hole recovery 'b' and 'c' should still be under 'abc'
  // assert disabled = pos length of abc = 4
  // assert enabled = AssertionError: outputEndNode=3 vs inputTo=2
  public void testAltPathFirstStepHole() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            3,
            new Token[] {token("abc", 1, 3, 0, 3), token("b", 1, 1, 1, 2), token("c", 1, 1, 2, 3)});

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"abc", "b", "c"},
        new int[] {0, 1, 2},
        new int[] {3, 2, 3},
        new int[] {1, 1, 1},
        new int[] {3, 1, 1},
        3);
  }

  // Last node in an alt path fixes outputnode of long path. In this graph the follow up node fixes
  // that.
  // incorrect pos length of abc = 1
  public void testAltPathLastStepHole() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            4,
            new Token[] {
              token("abc", 1, 3, 0, 3),
              token("a", 0, 1, 0, 1),
              token("b", 1, 1, 1, 2),
              token("d", 2, 1, 3, 4)
            });

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"abc", "a", "b", "d"},
        new int[] {0, 0, 1, 3},
        new int[] {1, 1, 2, 4},
        new int[] {1, 0, 1, 2},
        new int[] {3, 1, 1, 1},
        4);
  }

  // Check to see how multiple holes in a row are preserved.
  public void testLongHole() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            28,
            new Token[] {
              token("hello", 1, 1, 0, 5), token("hole", 5, 1, 20, 24), token("fun", 1, 1, 25, 28),
            });

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"hello", "hole", "fun"},
        new int[] {0, 20, 25},
        new int[] {5, 24, 28},
        new int[] {1, 2, 1},
        new int[] {1, 1, 1},
        28);
  }

  // multiple nodes missing in the alt path.
  // assert disabled = nothing
  // assert enabled = AssertionError
  public void testAltPathLastStepLongHole() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            4,
            new Token[] {token("abc", 1, 3, 0, 3), token("a", 0, 1, 0, 1), token("d", 3, 1, 3, 4)});

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"abc", "a", "d"},
        new int[] {0, 0, 3},
        new int[] {1, 1, 4},
        new int[] {1, 0, 2},
        new int[] {2, 1, 1},
        4);
  }

  // LUCENE-8723
  // Token stream ends without any edge to fix the long edge's output node
  // assert disabled = dropped token
  // assert enabled = AssertionError: 2
  public void testAltPathLastStepHoleWithoutEndToken() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            2,
            new Token[] {token("abc", 1, 3, 0, 3), token("a", 0, 1, 0, 1), token("b", 1, 1, 1, 2)});

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"abc", "a", "b"},
        new int[] {0, 0, 1},
        new int[] {1, 1, 2},
        new int[] {1, 0, 1},
        new int[] {1, 1, 1},
        2);
  }

  // similar to AltPathLastStepHoleWithoutEndToken, but instead of no token to trigger long path
  // resolution,
  // the next token has no way to reference to the long path so we have to resolve as if that last
  // token wasn't present.
  public void testAltPathLastStepHoleFollowedByHole() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            5,
            new Token[] {token("abc", 1, 3, 0, 3), token("b", 1, 1, 1, 2), token("e", 3, 1, 4, 5)});

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"abc", "b", "e"},
        new int[] {0, 1, 4},
        new int[] {3, 2, 5},
        new int[] {1, 1, 2},
        new int[] {1, 1, 1},
        5);
  }

  // Two Shingled long paths pass each other which gives a flattened graph with tokens backing up a
  // lot.
  public void testShingledGap() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            5,
            new Token[] {
              token("abc", 1, 3, 0, 3),
              token("a", 0, 1, 0, 1),
              token("b", 1, 1, 1, 2),
              token("cde", 1, 3, 2, 5),
              token("d", 1, 1, 3, 4),
              token("e", 1, 1, 4, 5)
            });

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"abc", "a", "d", "b", "cde", "e"},
        new int[] {0, 0, 3, 3, 4, 4},
        new int[] {1, 1, 3, 3, 5, 5},
        new int[] {1, 0, 1, 0, 1, 0},
        new int[] {1, 1, 1, 1, 1, 1},
        5);
  }

  // With shingles, token order may change during flattening.
  // We need to be careful not to free input nodes if they still have unreleased edges.
  // with/without exceptions ArrayIndexOutOfBoundsException
  public void testShingledGapWithHoles() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            5,
            new Token[] {
              token("abc", 1, 3, 0, 3),
              token("b", 1, 1, 1, 2),
              token("cde", 1, 3, 2, 5),
              token("d", 1, 1, 3, 4),
              token("e", 1, 1, 4, 5)
            });

    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out,
        new String[] {"abc", "d", "b", "cde", "e"},
        new int[] {0, 3, 3, 4, 4},
        new int[] {3, 3, 3, 5, 5},
        new int[] {1, 1, 0, 1, 0},
        new int[] {1, 1, 1, 1, 1},
        5);
  }

  // When the first token is a hole there is no original token to offset from.
  public void testFirstTokenHole() throws Exception {
    TokenStream in = new CannedTokenStream(0, 9, new Token[] {token("start", 2, 1, 0, 5)});
    TokenStream out = new FlattenGraphFilter(in);

    assertTokenStreamContents(
        out, new String[] {"start"}, new int[] {0}, new int[] {5}, new int[] {2}, new int[] {1}, 9);
  }

  // The singled token starts from a hole.
  // Hole recovery will cause the shingled token to start later in the output than its alternate
  // paths.
  // This will result in it being released too early.
  public void testShingleFromGap() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            9,
            new Token[] {
              token("a", 1, 1, 4, 8),
              token("abc", 0, 3, 4, 7),
              token("cd", 2, 2, 6, 8),
              token("d", 1, 1, 7, 8),
              token("e", 1, 1, 8, 9)
            });
    TokenStream out = new FlattenGraphFilter(in);
    assertTokenStreamContents(
        out,
        new String[] {"a", "abc", "d", "cd", "e"},
        new int[] {4, 4, 7, 7, 8},
        new int[] {7, 7, 8, 8, 9},
        new int[] {1, 0, 1, 1, 1},
        new int[] {1, 1, 2, 1, 1},
        9);
  }

  public void testShingledGapAltPath() throws Exception {
    TokenStream in =
        new CannedTokenStream(
            0,
            4,
            new Token[] {
              token("abc", 1, 3, 0, 3), token("abcd", 0, 4, 0, 4), token("cd", 2, 2, 2, 4),
            });
    TokenStream out = new FlattenGraphFilter(in);
    assertTokenStreamContents(
        out,
        new String[] {"abc", "abcd", "cd"},
        new int[] {0, 0, 2},
        new int[] {3, 4, 4},
        new int[] {1, 0, 1},
        new int[] {1, 2, 1},
        4);
  }

  // Lots of shingles and alternate paths connecting to each other. One edge 'c' missing between
  // 'ab' and 'def'
  public void testHeavilyConnectedGraphWithGap() throws IOException {
    TokenStream in =
        new CannedTokenStream(
            0,
            7,
            new Token[] {
              token("a", 1, 1, 0, 1),
              token("ab", 0, 2, 0, 2),
              token("abcdef", 0, 6, 0, 6),
              token("abcd", 0, 4, 0, 4),
              token("bcdef", 1, 5, 1, 7),
              token("def", 2, 3, 4, 7),
              token("e", 1, 1, 5, 6),
              token("f", 1, 1, 6, 7)
            });
    TokenStream out = new FlattenGraphFilter(in);
    assertTokenStreamContents(
        out,
        new String[] {"a", "ab", "abcdef", "abcd", "bcdef", "e", "def", "f"},
        new int[] {0, 0, 0, 0, 5, 5, 6, 6},
        new int[] {1, 1, 7, 1, 7, 6, 7, 7},
        new int[] {1, 0, 0, 0, 1, 0, 1, 0},
        new int[] {1, 1, 3, 1, 2, 1, 1, 1},
        7);
  }
  // This graph can create a disconnected input node that is farther ahead in the output than its
  // subsequent input node.
  // Exceptions: Free too early or dropped tokens.
  public void testShingleWithLargeLeadingGap() throws IOException {
    TokenStream in =
        new CannedTokenStream(
            0,
            6,
            new Token[] {
              token("abcde", 1, 5, 0, 5), token("ef", 4, 2, 4, 6), token("f", 1, 1, 5, 6),
            });
    TokenStream out = new FlattenGraphFilter(in);
    assertTokenStreamContents(
        out,
        new String[] {"abcde", "f", "ef"},
        new int[] {0, 5, 5},
        new int[] {5, 6, 6},
        new int[] {1, 1, 0},
        new int[] {1, 1, 1},
        6);
  }

  /**
   * build CharsRef containing 2-4 tokens
   *
   * @param tokens vocabulary of tokens
   * @param charsRefBuilder CharsRefBuilder
   * @param random Random for selecting tokens
   * @return Charsref containing 2-4 tokens.
   */
  private CharsRef buildMultiTokenCharsRef(
      String[] tokens, CharsRefBuilder charsRefBuilder, Random random) {
    int srcLen = random.nextInt(2) + 2;
    String[] srcTokens = new String[srcLen];
    for (int pos = 0; pos < srcLen; pos++) {
      srcTokens[pos] = tokens[random().nextInt(tokens.length)];
    }
    SynonymMap.Builder.join(srcTokens, charsRefBuilder);
    return charsRefBuilder.toCharsRef();
  }

  // Create a random graph then delete some edges to see if we can trip up FlattenGraphFilter
  public void testRandomGraphs() throws Exception {
    String[] baseTokens = new String[] {"t1", "t2", "t3", "t4"};
    String[] synTokens = new String[] {"s1", "s2", "s3", "s4"};

    SynonymMap.Builder mapBuilder = new SynonymMap.Builder();
    CharsRefBuilder charRefBuilder = new CharsRefBuilder();
    Random random = random();

    // between 10 and 20 synonym entries
    int synCount = random.nextInt(10) + 10;
    for (int i = 0; i < synCount; i++) {
      int type = random.nextInt(4);
      CharsRef src;
      CharsRef dest;
      switch (type) {
        case 0:
          // 1:1
          src = charRefBuilder.append(baseTokens[random.nextInt(baseTokens.length)]).toCharsRef();
          charRefBuilder.clear();
          dest = charRefBuilder.append(synTokens[random.nextInt(synTokens.length)]).toCharsRef();
          charRefBuilder.clear();
          break;
        case 1:
          // many:1
          src = buildMultiTokenCharsRef(baseTokens, charRefBuilder, random);
          charRefBuilder.clear();
          dest = charRefBuilder.append(synTokens[random.nextInt(synTokens.length)]).toCharsRef();
          charRefBuilder.clear();
          break;
        case 2:
          // 1:many
          src = charRefBuilder.append(baseTokens[random.nextInt(baseTokens.length)]).toCharsRef();
          charRefBuilder.clear();
          dest = buildMultiTokenCharsRef(synTokens, charRefBuilder, random);
          charRefBuilder.clear();
          break;
        default:
          // many:many
          src = buildMultiTokenCharsRef(baseTokens, charRefBuilder, random);
          charRefBuilder.clear();
          dest = buildMultiTokenCharsRef(synTokens, charRefBuilder, random);
          charRefBuilder.clear();
      }
      mapBuilder.add(src, dest, true);
    }

    SynonymMap synMap = mapBuilder.build();

    int stopWordCount = random.nextInt(4) + 1;
    CharArraySet stopWords = new CharArraySet(stopWordCount, true);
    while (stopWords.size() < stopWordCount) {
      int index = random.nextInt(baseTokens.length + synTokens.length);
      String[] tokenArray = baseTokens;
      if (index >= baseTokens.length) {
        index -= baseTokens.length;
        tokenArray = synTokens;
      }
      stopWords.add(tokenArray[index]);
    }

    Analyzer withFlattenGraph =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer in = new WhitespaceTokenizer();
            TokenStream result = new SynonymGraphFilter(in, synMap, true);
            result = new StopFilter(result, stopWords);
            result = new FlattenGraphFilter(result);
            return new TokenStreamComponents(in, result);
          }
        };

    int tokenCount = random.nextInt(20) + 20;
    List<String> stringTokens = new ArrayList<>();
    while (stringTokens.size() < tokenCount) {
      stringTokens.add(baseTokens[random.nextInt(baseTokens.length)]);
    }

    String text = String.join(" ", stringTokens);
    // FlattenGraphFilter can create inconsistent offsets.
    // If that is resolved we can check offsets
    // Until then converting to automaton will pull text through and check if we hit asserts.
    // checkAnalysisConsistency(random, withFlattenGraph, false, text);
    TokenStreamToAutomaton tsta = new TokenStreamToAutomaton();
    TokenStream flattenedTokenStream = withFlattenGraph.tokenStream("field", text);
    assertFalse(Operations.hasDeadStates(tsta.toAutomaton(flattenedTokenStream)));
    flattenedTokenStream.close();

    /*
       CheckGeneralization can get VERY slow as matching holes to tokens or other holes generates a lot of potentially valid paths.
       Analyzer withoutFlattenGraph =
           new Analyzer() {
             @Override
             protected TokenStreamComponents createComponents(String fieldName) {
               Tokenizer in = new WhitespaceTokenizer();
               TokenStream result = new SynonymGraphFilter(in, synMap, true);
               result = new StopFilter(result, stopWords);
               return new TokenStreamComponents(in, result);
             }
           };
       checkGeneralization(
           withFlattenGraph.tokenStream("field", text),
           withoutFlattenGraph.tokenStream("field", text));

    */
  }

  /*
   * Make some strings, make an automaton that accepts those strings, convert that automaton into a TokenStream,
   * flatten it, back to an automaton, and see if the original strings are still accepted.
   */
  public void testPathsNotLost() throws IOException {
    int wordCount = random().nextInt(5) + 5;
    List<BytesRef> acceptStrings = new LinkedList<>();
    for (int i = 0; i < wordCount; i++) {
      int wordLen = random().nextInt(5) + 5;
      BytesRef ref = new BytesRef(wordLen);
      ref.length = wordLen;
      ref.offset = 0;
      for (int j = 0; j < wordLen; j++) {
        ref.bytes[j] = (byte) (random().nextInt(5) + 65);
      }
      acceptStrings.add(ref);
    }
    acceptStrings.sort(Comparator.naturalOrder());

    acceptStrings = acceptStrings.stream().limit(wordCount).collect(Collectors.toList());
    Automaton nonFlattenedAutomaton = DaciukMihovAutomatonBuilder.build(acceptStrings);

    TokenStream ts = AutomatonToTokenStream.toTokenStream(nonFlattenedAutomaton);
    TokenStream flattenedTokenStream = new FlattenGraphFilter(ts);
    TokenStreamToAutomaton tsta = new TokenStreamToAutomaton();
    Automaton flattenedAutomaton = tsta.toAutomaton(flattenedTokenStream);

    // TokenStreamToAutomaton adds position increment transitions into the automaton.
    List<BytesRef> acceptStringsWithPosSep = createAcceptStringsWithPosSep(acceptStrings);

    for (BytesRef acceptString : acceptStringsWithPosSep) {
      assertTrue(
          "string not accepted " + acceptString.utf8ToString(),
          recursivelyValidate(acceptString, 0, 0, flattenedAutomaton));
    }
  }

  /**
   * adds POS_SEP bytes between bytes to match TokenStreamToAutomaton format.
   *
   * @param acceptStrings Byte refs of accepted strings. Each byte is a transition
   * @return List of ByteRefs where each byte is separated by a POS_SEP byte.
   */
  private List<BytesRef> createAcceptStringsWithPosSep(List<BytesRef> acceptStrings) {
    List<BytesRef> acceptStringsWithPosSep = new ArrayList<>();
    for (BytesRef acceptString : acceptStrings) {
      BytesRef withPosSep = new BytesRef(acceptString.length * 2 - 1);
      withPosSep.length = acceptString.length * 2 - 1;
      withPosSep.offset = 0;
      for (int i = 0; i < acceptString.length; i++) {
        withPosSep.bytes[i * 2] = acceptString.bytes[i];
        if (i * 2 + 1 < withPosSep.length) {
          withPosSep.bytes[i * 2 + 1] = TokenStreamToAutomaton.POS_SEP;
        }
      }
      acceptStringsWithPosSep.add(withPosSep);
    }
    return acceptStringsWithPosSep;
  }

  /**
   * Checks if acceptString is accepted by the automaton. Automaton may be an NFA.
   *
   * @param acceptString String to test
   * @param acceptStringIndex current index into acceptString, initial value should be 0
   * @param state state to transition from. initial value should be 0
   * @param automaton Automaton to test
   * @return true if acceptString is accepted by the automaton. otherwise false.
   */
  public boolean recursivelyValidate(
      BytesRef acceptString, int acceptStringIndex, int state, Automaton automaton) {
    if (acceptStringIndex == acceptString.length) {
      return automaton.isAccept(state);
    }

    Transition transition = new Transition();
    automaton.initTransition(state, transition);
    int numTransitions = automaton.getNumTransitions(state);
    boolean accept = false;
    // Automaton can be NFA, so we need to check all matching transitions
    for (int i = 0; i < numTransitions; i++) {
      automaton.getTransition(state, i, transition);
      if (transition.min <= acceptString.bytes[acceptStringIndex]
          && transition.max >= acceptString.bytes[acceptStringIndex]) {
        accept =
            recursivelyValidate(acceptString, acceptStringIndex + 1, transition.dest, automaton);
      }
      if (accept == true) {
        break;
      }
    }
    return accept;
  }

  /**
   * This method checks if strings that lead to the accept state of the not flattened TokenStream
   * also lead to the accept state in the flattened TokenStream. This gets complicated when you
   * factor in holes. The FlattenGraphFilter will remove alternate paths that are made entirely of
   * holes. An alternate path of Holes is indistinguishable from a path that just has long
   * lengths(ex: testStrangelyNumberedNodes). Also alternate paths that end in multiple holes could
   * be interpreted as sequential holes after the branching has converged during flattening. This
   * leads to a lot of weird logic about navigating around holes that may compromise the accuracy of
   * this test.
   *
   * @param flattened flattened TokenStream
   * @param notFlattened not flattened TokenStream
   * @throws IOException on error creating Automata
   */
  /* private void checkGeneralization(TokenStream flattened, TokenStream notFlattened)
      throws IOException {
    TokenStreamToAutomaton tsta = new TokenStreamToAutomaton();

    List<LinkedList<Integer>> acceptStrings = getAcceptStrings(tsta.toAutomaton(notFlattened));
    checkAcceptStrings(acceptStrings, tsta.toAutomaton(flattened));
    flattened.close();
    notFlattened.close();
  }*/

  /**
   * gets up to 10000 strings that lead to accept state in the given automaton.
   *
   * @param automaton automaton
   * @return list of accept sequences
   */
  /* private List<LinkedList<Integer>> getAcceptStrings(Automaton automaton) {
    List<LinkedList<Integer>> acceptedSequences = new LinkedList<>();
    LinkedList<Integer> prefix = new LinkedList<>();
    // state 0 is always the start node
    // Particularly branching automatons can create lots of possible acceptable strings. limit to
    // the first 10K
    buildAcceptStringRecursive(automaton, 0, prefix, acceptedSequences, 10000);
    return acceptedSequences;
  }*/

  /**
   * @param automaton automaton to generate strings from
   * @param state state to start at
   * @param prefix string prefix
   * @param acceptedSequences List of strings build so far.
   * @param limit maximum number of acceptedSequences.
   */
  /*private void buildAcceptStringRecursive(
      Automaton automaton,
      int state,
      LinkedList<Integer> prefix,
      List<LinkedList<Integer>> acceptedSequences,
      int limit) {
    if (acceptedSequences.size() == limit) {
      return;
    }
    if (automaton.isAccept(state)) {
      acceptedSequences.add(new LinkedList<>(prefix));
      return;
    }
    int numTransitions = automaton.getNumTransitions(state);
    Transition transition = new Transition();
    for (int i = 0; i < numTransitions; i++) {
      automaton.getTransition(state, i, transition);
      // min and max are the same transitions made from TokenStreamToAutomaton
      prefix.addLast(transition.min);
      buildAcceptStringRecursive(automaton, transition.dest, prefix, acceptedSequences, limit);
      prefix.removeLast();
    }
  }

  private void checkAcceptStrings(List<LinkedList<Integer>> acceptSequence, Automaton automaton) {
    for (LinkedList<Integer> acceptString : acceptSequence) {
      assertTrue(
          "String did not lead to accept state " + acceptString,
          recursivelyValidateWithHoles(acceptString, 0, automaton));
    }
  }

  private boolean recursivelyValidateWithHoles(
      LinkedList<Integer> acceptSequence, int state, Automaton automaton) {
    if (acceptSequence.isEmpty()) {
      return automaton.isAccept(state);
    }

    Integer curr = acceptSequence.pop();
    int numTransitions = automaton.getNumTransitions(state);
    Transition transition = new Transition();

    boolean accept = false;
    // Automaton can be NFA, so we need to check all matching transitions
    for (int i = 0; i < numTransitions; i++) {
      automaton.getTransition(state, i, transition);
      if (transition.min <= curr && transition.max >= curr) {
        accept = recursivelyValidateWithHoles(acceptSequence, transition.dest, automaton);
        // Factoring in flattened graphs the space covered by a hole may be bigger in the flattened
        // graph.
        // Try consuming more steps with holes.
        if (accept == false
            && transition.min == TokenStreamToAutomaton.HOLE
            && transition.max == TokenStreamToAutomaton.HOLE) {
          acceptSequence.push(TokenStreamToAutomaton.HOLE);
          acceptSequence.push(TokenStreamToAutomaton.POS_SEP);
          accept = recursivelyValidateWithHoles(acceptSequence, transition.dest, automaton);
          acceptSequence.pop();
          acceptSequence.pop();
        }
      } else if (transition.min == TokenStreamToAutomaton.HOLE
          && transition.max == TokenStreamToAutomaton.HOLE
          && automaton.getNumTransitions(transition.dest) > 0) {
        //consume multiple holes in the automaton
        // clear POS_INC
        automaton.getTransition(transition.dest, 0, transition);
        acceptSequence.push(curr);
        accept = recursivelyValidateWithHoles(acceptSequence, transition.dest, automaton);
        acceptSequence.pop();
      } else if(curr == TokenStreamToAutomaton.HOLE) {
        //consume non-holes in the automaton with holes
        while (transition.min != TokenStreamToAutomaton.POS_SEP
                && automaton.getNumTransitions(transition.dest) > 0) {
          automaton.getTransition(transition.dest, 0, transition);
        }
        acceptSequence.push(curr);
        accept = recursivelyValidateWithHoles(acceptSequence, transition.dest, automaton);
        acceptSequence.pop();
      }
      if (accept) {
        break;
      }
    }
    // Flatten graph filter will remove side paths that are only Holes. Gaps may also change size as
    // graph is flattened.
    // Traverse over them if curr is a hole to make sure the gap is kept
    if (accept == false && curr == TokenStreamToAutomaton.HOLE && acceptSequence.size() > 0) {
      // get rid of the separator
      acceptSequence.pop();

      for (int i = 0; i < numTransitions; i++) {
        automaton.getTransition(state, i, transition);
        //advance to the next POS_SEP in automaton
        while (transition.min != TokenStreamToAutomaton.POS_SEP
            && automaton.getNumTransitions(transition.dest) > 0) {
          automaton.getTransition(transition.dest, 0, transition);
        }
        accept = recursivelyValidateWithHoles(acceptSequence, transition.dest, automaton);
        if (accept) {
          break;
        }
      }

      // might be multiple holes squashed under a one step path. Try burning remaining holes
      if (accept == false) {
        accept = recursivelyValidateWithHoles(acceptSequence, state, automaton);
      }

      acceptSequence.push(TokenStreamToAutomaton.POS_SEP);
    }
    acceptSequence.push(curr);
    return accept;
  } */

  // NOTE: TestSynonymGraphFilter's testRandomSyns also tests FlattenGraphFilter
}
