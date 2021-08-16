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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.DaciukMihovAutomatonBuilder;

public class TestAutomatonToTokenStream extends BaseTokenStreamTestCase {

  public void testSinglePath() throws IOException {
    List<BytesRef> acceptStrings = new ArrayList<>();
    acceptStrings.add(new BytesRef("abc"));

    Automaton flatPathAutomaton = DaciukMihovAutomatonBuilder.build(acceptStrings);
    TokenStream ts = AutomatonToTokenStream.toTokenStream(flatPathAutomaton);
    assertTokenStreamContents(
        ts,
        new String[] {"a", "b", "c"},
        new int[] {0, 1, 2},
        new int[] {1, 2, 3},
        new int[] {1, 1, 1},
        new int[] {1, 1, 1},
        3);
  }

  public void testParallelPaths() throws IOException {
    List<BytesRef> acceptStrings = new ArrayList<>();
    acceptStrings.add(new BytesRef("123"));
    acceptStrings.add(new BytesRef("abc"));

    Automaton flatPathAutomaton = DaciukMihovAutomatonBuilder.build(acceptStrings);
    TokenStream ts = AutomatonToTokenStream.toTokenStream(flatPathAutomaton);
    assertTokenStreamContents(
        ts,
        new String[] {"1", "a", "2", "b", "3", "c"},
        new int[] {0, 0, 1, 1, 2, 2},
        new int[] {1, 1, 2, 2, 3, 3},
        new int[] {1, 0, 1, 0, 1, 0},
        new int[] {1, 1, 1, 1, 1, 1},
        3);
  }

  public void testForkedPath() throws IOException {
    List<BytesRef> acceptStrings = new ArrayList<>();
    acceptStrings.add(new BytesRef("ab3"));
    acceptStrings.add(new BytesRef("abc"));

    Automaton flatPathAutomaton = DaciukMihovAutomatonBuilder.build(acceptStrings);
    TokenStream ts = AutomatonToTokenStream.toTokenStream(flatPathAutomaton);
    assertTokenStreamContents(
        ts,
        new String[] {"a", "b", "3", "c"},
        new int[] {0, 1, 2, 2},
        new int[] {1, 2, 3, 3},
        new int[] {1, 1, 1, 0},
        new int[] {1, 1, 1, 1},
        3);
  }

  public void testNonDeterministicGraph() throws IOException {
    Automaton.Builder builder = new Automaton.Builder();
    int start = builder.createState();
    int middle1 = builder.createState();
    int middle2 = builder.createState();
    int accept = builder.createState();

    builder.addTransition(start, middle1, 'a');
    builder.addTransition(start, middle2, 'a');
    builder.addTransition(middle1, accept, 'b');
    builder.addTransition(middle2, accept, 'c');
    builder.setAccept(accept, true);

    Automaton nfa = builder.finish();
    TokenStream ts = AutomatonToTokenStream.toTokenStream(nfa);
    assertTokenStreamContents(
        ts,
        new String[] {"a", "a", "b", "c"},
        new int[] {0, 0, 1, 1},
        new int[] {1, 1, 2, 2},
        new int[] {1, 0, 1, 0},
        new int[] {1, 1, 1, 1},
        2);
  }

  public void testGraphWithStartNodeCycle() {
    Automaton.Builder builder = new Automaton.Builder();
    int start = builder.createState();
    int middle = builder.createState();
    int accept = builder.createState();

    builder.addTransition(start, middle, 'a');
    builder.addTransition(middle, accept, 'b');
    builder.addTransition(middle, start, '1');

    builder.setAccept(accept, true);

    Automaton cycleGraph = builder.finish();
    expectThrows(
        IllegalArgumentException.class, () -> AutomatonToTokenStream.toTokenStream(cycleGraph));
  }

  public void testGraphWithNonStartCycle() {
    Automaton.Builder builder = new Automaton.Builder();
    int start = builder.createState();
    int middle = builder.createState();
    int accept = builder.createState();

    builder.addTransition(start, middle, 'a');
    builder.addTransition(middle, accept, 'b');
    builder.addTransition(accept, middle, 'c');
    builder.setAccept(accept, true);

    Automaton cycleGraph = builder.finish();
    expectThrows(
        IllegalArgumentException.class, () -> AutomatonToTokenStream.toTokenStream(cycleGraph));
  }
}
