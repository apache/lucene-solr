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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.FiniteStringsIterator;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/**
 * Consumes a TokenStream and creates an {@link Automaton} where the transition labels are terms from
 * the {@link TermToBytesRefAttribute}.
 * This class also provides helpers to explore the different paths of the {@link Automaton}.
 */
public final class GraphTokenStreamFiniteStrings {

  private AttributeSource[] tokens = new AttributeSource[4];
  private final Automaton det;
  private final Transition transition = new Transition();

  private class FiniteStringsTokenStream extends TokenStream {
    private final IntsRef ids;
    private final int end;
    private int offset;

    FiniteStringsTokenStream(final IntsRef ids) {
      super(tokens[0].cloneAttributes());
      assert ids != null;
      this.ids = ids;
      this.offset = ids.offset;
      this.end = ids.offset + ids.length;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (offset < end) {
        clearAttributes();
        int id = ids.ints[offset];
        tokens[id].copyTo(this);
        offset++;
        return true;
      }

      return false;
    }
  }

  public GraphTokenStreamFiniteStrings(TokenStream in) throws IOException {
    Automaton aut = build(in);
    this.det = Operations.removeDeadStates(Operations.determinize(aut, DEFAULT_MAX_DETERMINIZED_STATES));
  }

  /**
   * Returns whether the provided state is the start of multiple side paths of different length (eg: new york, ny)
   */
  public boolean hasSidePath(int state) {
    int numT = det.initTransition(state, transition);
    if (numT <= 1) {
      return false;
    }
    det.getNextTransition(transition);
    int dest = transition.dest;
    for (int i = 1; i < numT; i++) {
      det.getNextTransition(transition);
      if (dest != transition.dest) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the list of tokens that start at the provided state
   */
  public List<AttributeSource> getTerms(int state) {
    int numT = det.initTransition(state, transition);
    List<AttributeSource> tokens = new ArrayList<> ();
    for (int i = 0; i < numT; i++) {
      det.getNextTransition(transition);
      tokens.addAll(Arrays.asList(this.tokens).subList(transition.min, transition.max + 1));
    }
    return tokens;
  }

  /**
   * Returns the list of terms that start at the provided state
   */
  public Term[] getTerms(String field, int state) {
    return getTerms(state).stream()
        .map(s -> new Term(field, s.addAttribute(TermToBytesRefAttribute.class).getBytesRef()))
        .toArray(Term[]::new);
  }

  /**
   * Get all finite strings from the automaton.
   */
  public Iterator<TokenStream> getFiniteStrings() throws IOException {
    return getFiniteStrings(0, -1);
  }


  /**
   * Get all finite strings that start at {@code startState} and end at {@code endState}.
   */
  public Iterator<TokenStream> getFiniteStrings(int startState, int endState) {
    final FiniteStringsIterator it = new FiniteStringsIterator(det, startState, endState);
    return new Iterator<> () {
      IntsRef current;
      boolean finished = false;

      @Override
      public boolean hasNext() {
        if (finished == false && current == null) {
          current = it.next();
          if (current == null) {
            finished = true;
          }
        }
        return current != null;
      }

      @Override
      public TokenStream next() {
        if (current == null) {
          hasNext();
        }
        TokenStream next =  new FiniteStringsTokenStream(current);
        current = null;
        return next;
      }
    };
  }

  /**
   * Returns the articulation points (or cut vertices) of the graph:
   * https://en.wikipedia.org/wiki/Biconnected_component
   */
  public int[] articulationPoints() {
    if (det.getNumStates() == 0) {
      return new int[0];
    }
    //
    Automaton.Builder undirect = new Automaton.Builder();
    undirect.copy(det);
    for (int i = 0; i < det.getNumStates(); i++) {
      int numT = det.initTransition(i, transition);
      for (int j = 0; j < numT; j++) {
        det.getNextTransition(transition);
        undirect.addTransition(transition.dest, i, transition.min);
      }
    }
    int numStates = det.getNumStates();
    BitSet visited = new BitSet(numStates);
    int[] depth = new int[det.getNumStates()];
    int[] low = new int[det.getNumStates()];
    int[] parent = new int[det.getNumStates()];
    Arrays.fill(parent, -1);
    List<Integer> points = new ArrayList<>();
    articulationPointsRecurse(undirect.finish(), 0, 0, depth, low, parent, visited, points);
    Collections.reverse(points);
    return points.stream().mapToInt(p -> p).toArray();
  }

  /**
   * Build an automaton from the provided {@link TokenStream}.
   */
  private Automaton build(final TokenStream in) throws IOException {
    Automaton.Builder builder = new Automaton.Builder();

    final PositionIncrementAttribute posIncAtt = in.addAttribute(PositionIncrementAttribute.class);
    final PositionLengthAttribute posLengthAtt = in.addAttribute(PositionLengthAttribute.class);

    in.reset();

    int pos = -1;
    int prevIncr = 1;
    int state = -1;
    int id = -1;
    int gap = 0;
    while (in.incrementToken()) {
      int currentIncr = posIncAtt.getPositionIncrement();
      if (pos == -1 && currentIncr < 1) {
        throw new IllegalStateException("Malformed TokenStream, start token can't have increment less than 1");
      }

      if (currentIncr == 0) {
        if (gap > 0) {
          pos -= gap;
        }
      }
      else {
        pos++;
        gap = currentIncr - 1;
      }

      int endPos = pos + posLengthAtt.getPositionLength() + gap;
      while (state < endPos) {
        state = builder.createState();
      }

      id++;
      if (tokens.length < id + 1) {
        tokens = ArrayUtil.grow(tokens, id + 1);
      }

      tokens[id] = in.cloneAttributes();
      builder.addTransition(pos, endPos, id);
      pos += gap;

      // we always produce linear token graphs from getFiniteStrings(), so we need to adjust
      // posLength and posIncrement accordingly
      tokens[id].addAttribute(PositionLengthAttribute.class).setPositionLength(1);
      if (currentIncr == 0) {
        // stacked token should have the same increment as original token at this position
        tokens[id].addAttribute(PositionIncrementAttribute.class).setPositionIncrement(prevIncr);
      }

      // only save last increment on non-zero increment in case we have multiple stacked tokens
      if (currentIncr > 0) {
        prevIncr = currentIncr;
      }
    }

    in.end();
    if (state != -1) {
      builder.setAccept(state, true);
    }
    return builder.finish();
  }

  private static void articulationPointsRecurse(Automaton a, int state, int d, int[] depth, int[] low, int[] parent,
                                                BitSet visited, List<Integer> points) {
    visited.set(state);
    depth[state] = d;
    low[state] = d;
    int childCount = 0;
    boolean isArticulation = false;
    Transition t = new Transition();
    int numT = a.initTransition(state, t);
    for (int i = 0; i < numT; i++) {
      a.getNextTransition(t);
      if (visited.get(t.dest) == false) {
        parent[t.dest] = state;
        articulationPointsRecurse(a, t.dest, d + 1, depth, low, parent, visited, points);
        childCount++;
        if (low[t.dest] >= depth[state]) {
          isArticulation = true;
        }
        low[state] = Math.min(low[state], low[t.dest]);
      } else if (t.dest != parent[state]) {
        low[state] = Math.min(low[state], depth[t.dest]);
      }
    }
    if ((parent[state] != -1 && isArticulation) || (parent[state] == -1 && childCount > 1)) {
      points.add(state);
    }
  }
}