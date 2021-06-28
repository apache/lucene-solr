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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

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

  // The end node the long path is supposed to flatten over doesn't exist
  // assert disabled = pos length of abc = 4
  // assert enabled = AssertionError: outputEndNode=3 vs inputTo=2
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-9963")
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

  // Last node in an alt path releases the long path. but it doesn't exist in this graph
  // pos length of abc = 1
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-9963")
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

  // Posinc >2 gets squashed to 2
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

  // multiple nodes missing in the alt path. Last edge shows up after long edge and short edge,
  // which looks good but the output graph isn't flat.
  // assert disabled = nothing
  // assert enabled = AssertionError
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-9963")
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
        new int[] {1, 0, 1},
        new int[] {1, 1, 1},
        4);
  }

  // LUCENE-8723
  // Token stream ends without last node showing up
  // assert disabled = dropped token
  // assert enabled = AssertionError: 2
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-9963")
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
  // Is there some way we can do this and validate output nodes?
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-9963")
  public void testRandomGraphs() throws Exception {
    String[] baseTokens = new String[] {"t1", "t2", "t3", "t4"};
    String[] synTokens = new String[] {"s1", "s2", "s3", "s4"};

    SynonymMap.Builder mapBuilder = new SynonymMap.Builder();
    CharsRefBuilder charRefBuilder = new CharsRefBuilder();
    Random random = random();

    // between 20 and 20 synonym entries
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

    Analyzer a =
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
    checkAnalysisConsistency(random, a, false, text);
  }

  // NOTE: TestSynonymGraphFilter's testRandomSyns also tests FlattenGraphFilter
}
