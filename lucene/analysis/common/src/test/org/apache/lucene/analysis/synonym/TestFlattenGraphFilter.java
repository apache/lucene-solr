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

package org.apache.lucene.analysis.synonym;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

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

  // NOTE: TestSynonymGraphFilter's testRandomSyns also tests FlattenGraphFilter
}
