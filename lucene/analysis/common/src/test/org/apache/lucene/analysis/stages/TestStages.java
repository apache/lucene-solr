package org.apache.lucene.analysis.stages;

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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.ArcAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;

public class TestStages extends BaseTokenStreamTestCase {

  /** Like assertAnalyzesTo, but handles a graph: verifies
   *  the automaton == the union of the expectedStrings. */
  private void assertMatches(Automaton a, String... paths) {
    List<Automaton> subs = new ArrayList<Automaton>();
    for(String path : paths) {
      String[] tokens = path.split(" ");
      State lastState = new State();
      Automaton sub = new Automaton(lastState);
      subs.add(sub);
      for(int i=0;i<tokens.length;i++) {
        String token = tokens[i];
        BytesRef br = new BytesRef(token);
        for(int j=0;j<br.length;j++) {
          State state = new State();
          lastState.addTransition(new Transition(br.bytes[br.offset+j], state));
          lastState = state;
        }
        if (i < tokens.length-1) {
          State state = new State();
          lastState.addTransition(new Transition(AutomatonStage.POS_SEP, state));
          lastState = state;
        }
      }
      lastState.setAccept(true);
    }

    Automaton expected = BasicOperations.union(subs);
    if (!BasicOperations.sameLanguage(expected, a)) {
      System.out.println("expected:\n" + Automaton.minimize(expected).toDot());
      System.out.println("actual:\n" + Automaton.minimize(a).toDot());
      throw new AssertionError("languages differ");
    }
  }

  /** Runs the text through the analyzer and verifies the
   *  resulting automaton == union of the expectedStrings. */
  private void assertMatches(String text, Stage end, String... expectedStrings) throws IOException {
    AutomatonStage a = new AutomatonStage(new AssertingStage(end));
    CharTermAttribute termAtt = a.get(CharTermAttribute.class);
    for(int i=0;i<2;i++) {
      a.reset(new StringReader(text));
      while (a.next()) {
        System.out.println("token=" + termAtt);
      }
      assertMatches(a.getAutomaton(), expectedStrings);
    }
    assertFalse(a.anyNodesCanChange());
  }

  public void testBasic() throws Exception {
    assertMatches("This is a test",
                  new LowerCaseFilterStage(TEST_VERSION_CURRENT, new WhitespaceTokenizerStage()),
                  "this is a test");
  }

  public void testSplitOnDash() throws Exception {
    assertMatches("The drill-down-test works",
                  new SplitOnDashFilterStage(new LowerCaseFilterStage(TEST_VERSION_CURRENT, new WhitespaceTokenizerStage())),
                  "the drill-down-test works",
                  "the drill down test works");
  }

  private void add(SynonymMap.Builder b, String input, String output) {
    if (VERBOSE) {
      System.out.println("  add input=" + input + " output=" + output);
    }
    CharsRef inputCharsRef = new CharsRef();
    SynonymMap.Builder.join(input.split(" +"), inputCharsRef);

    CharsRef outputCharsRef = new CharsRef();
    SynonymMap.Builder.join(output.split(" +"), outputCharsRef);

    b.add(inputCharsRef, outputCharsRef, true);
  }

  public void testSyn() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    assertMatches("a b c foo",
                  new SynonymFilterStage(new WhitespaceTokenizerStage(), b.build(), true),
                  "a b c foo", "x foo");
  }

  public void testSynAfterDecompound() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    // Decompounder splits a-b into and b, and then
    // SynFilter runs after that and sees "a b c" match: 
    assertMatches("a-b c foo",
                  new SynonymFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage()), b.build(), true),
                  "a b c foo", "a-b c foo", "x foo");
  }

  // No buffering needed:
  public void testBasicStageAnalyzer() throws Exception {
    Analyzer a = new StageAnalyzer() {
        @Override
        protected Stage getStages() {
          return new LowerCaseFilterStage(TEST_VERSION_CURRENT, new WhitespaceTokenizerStage());
        }
      };

    assertAnalyzesTo(a, "This is a test",
                     new String[] {"this", "is", "a", "test"},
                     new int[] {0, 5, 8, 10},
                     new int[] {4, 7, 9, 14},
                     null,
                     new int[] {1, 1, 1, 1});
  }

  // Buffering needed:
  public void testSplitOnDashStageAnalyzer() throws Exception {
    Analyzer a = new StageAnalyzer() {
        @Override
        protected Stage getStages() {
          return new SplitOnDashFilterStage(new LowerCaseFilterStage(TEST_VERSION_CURRENT, new WhitespaceTokenizerStage()));
        }
      };

    assertAnalyzesTo(a, "The drill-down-test works",
                     new String[] {"the", "drill-down-test", "drill", "down", "test", "works"},
                     new int[] {0, 4, 4, 4, 4, 20},
                     new int[] {3, 19, 19, 19, 19, 25},
                     null,
                     new int[] {1, 1, 0, 1, 1, 1});
  }

  // Buffering needed:
  public void testSynStageAnalyzer() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    final SynonymMap map = b.build();

    Analyzer a = new StageAnalyzer() {
        @Override
        protected Stage getStages() {
          return new SynonymFilterStage(new WhitespaceTokenizerStage(), map, true);
        }
      };

    assertAnalyzesTo(a, "a b c foo",
                     new String[] {"a", "x", "b", "c", "foo"},
                     new int[] {0, 0, 2, 4, 6},
                     new int[] {1, 5, 3, 5, 9},
                     null,
                     new int[] {1, 0, 1, 1, 1});
  }

  // Buffering needed:
  public void testSynAfterDecompoundStageAnalyzer() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a b c", "x");
    final SynonymMap map = b.build();
    Analyzer a = new StageAnalyzer() {
        @Override
        protected Stage getStages() {
          return new SynonymFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage()), map, true);
        }
      };

    // Decompounder splits a-b into and b, and then
    // SynFilter runs after that and sees "a b c" match: 
    assertAnalyzesTo(a, "a-b c foo",
                     new String[] {"a-b", "a", "x", "b", "c", "foo"},
                     new int[] {0, 0, 0, 0, 4, 6},
                     new int[] {3, 3, 5, 3, 5, 9},
                     null,
                     new int[] {1, 0, 0, 1, 1, 1});
  }

  public void testStopFilterStageAnalyzer() throws Exception {
    final CharArraySet stopWords = new CharArraySet(TEST_VERSION_CURRENT, 1, false);
    stopWords.add("the");

    Analyzer a = new StageAnalyzer() {
        @Override
        protected Stage getStages() {
          return new StopFilterStage(new SplitOnDashFilterStage(new WhitespaceTokenizerStage()), stopWords);
        }
      };

    // Decompounder splits a-b into and b, and then
    // SynFilter runs after that and sees "a b c" match: 
    assertAnalyzesTo(a, "the-dog barks",
                     new String[] {"the-dog", "dog", "barks"},
                     new int[] {0, 0, 8},
                     new int[] {7, 7, 13},
                     null,
                     new int[] {1, 1, 1});
  }
}
