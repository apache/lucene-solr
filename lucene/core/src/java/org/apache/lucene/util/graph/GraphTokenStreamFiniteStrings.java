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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.FiniteStringsIterator;
import org.apache.lucene.util.automaton.Operations;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/**
 * Creates a list of {@link TokenStream} where each stream is the tokens that make up a finite string in graph token stream.  To do this,
 * the graph token stream is converted to an {@link Automaton} and from there we use a {@link FiniteStringsIterator} to collect the various
 * token streams for each finite string.
 */
public final class GraphTokenStreamFiniteStrings {
  private final Automaton.Builder builder = new Automaton.Builder();
  private final Map<BytesRef, Integer> termToID = new HashMap<>();
  private final Map<Integer, BytesRef> idToTerm = new HashMap<>();
  private final Map<Integer, Integer> idToInc = new HashMap<>();
  private Automaton det;

  private class FiniteStringsTokenStream extends TokenStream {
    private final BytesTermAttribute termAtt = addAttribute(BytesTermAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    private final IntsRef ids;
    private final int end;
    private int offset;

    FiniteStringsTokenStream(final IntsRef ids) {
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
        termAtt.setBytesRef(idToTerm.get(id));

        int incr = 1;
        if (idToInc.containsKey(id)) {
          incr = idToInc.get(id);
        }
        posIncAtt.setPositionIncrement(incr);
        offset++;
        return true;
      }

      return false;
    }
  }

  private GraphTokenStreamFiniteStrings() {
  }

  /**
   * Gets the list of finite string token streams from the given input graph token stream.
   */
  public static List<TokenStream> getTokenStreams(final TokenStream in) throws IOException {
    GraphTokenStreamFiniteStrings gfs = new GraphTokenStreamFiniteStrings();
    return gfs.process(in);
  }

  /**
   * Builds automaton and builds the finite string token streams.
   */
  private List<TokenStream> process(final TokenStream in) throws IOException {
    build(in);

    List<TokenStream> tokenStreams = new ArrayList<>();
    final FiniteStringsIterator finiteStrings = new FiniteStringsIterator(det);
    for (IntsRef ids; (ids = finiteStrings.next()) != null; ) {
      tokenStreams.add(new FiniteStringsTokenStream(IntsRef.deepCopyOf(ids)));
    }

    return tokenStreams;
  }

  private void build(final TokenStream in) throws IOException {
    if (det != null) {
      throw new IllegalStateException("Automation already built");
    }

    final TermToBytesRefAttribute termBytesAtt = in.addAttribute(TermToBytesRefAttribute.class);
    final PositionIncrementAttribute posIncAtt = in.addAttribute(PositionIncrementAttribute.class);
    final PositionLengthAttribute posLengthAtt = in.addAttribute(PositionLengthAttribute.class);

    in.reset();

    int pos = -1;
    int prevIncr = 1;
    int state = -1;
    while (in.incrementToken()) {
      int currentIncr = posIncAtt.getPositionIncrement();
      if (pos == -1 && currentIncr < 1) {
        throw new IllegalStateException("Malformed TokenStream, start token can't have increment less than 1");
      }

      // always use inc 1 while building, but save original increment
      int incr = Math.min(1, currentIncr);
      if (incr > 0) {
        pos += incr;
      }

      int endPos = pos + posLengthAtt.getPositionLength();
      while (state < endPos) {
        state = createState();
      }

      BytesRef term = termBytesAtt.getBytesRef();
      int id = getTermID(currentIncr, prevIncr, term);
      addTransition(pos, endPos, currentIncr, id);

      // only save last increment on non-zero increment in case we have multiple stacked tokens
      if (currentIncr > 0) {
        prevIncr = currentIncr;
      }
    }

    in.end();
    setAccept(state, true);
    finish();
  }

  /**
   * Returns a new state; state 0 is always the initial state.
   */
  private int createState() {
    return builder.createState();
  }

  /**
   * Marks the specified state as accept or not.
   */
  private void setAccept(int state, boolean accept) {
    builder.setAccept(state, accept);
  }

  /**
   * Adds a transition to the automaton.
   */
  private void addTransition(int source, int dest, int incr, int id) {
    builder.addTransition(source, dest, id);
  }

  /**
   * Call this once you are done adding states/transitions.
   */
  private void finish() {
    finish(DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Call this once you are done adding states/transitions.
   *
   * @param maxDeterminizedStates Maximum number of states created when determinizing the automaton.  Higher numbers allow this operation
   *                              to consume more memory but allow more complex automatons.
   */
  private void finish(int maxDeterminizedStates) {
    Automaton automaton = builder.finish();
    det = Operations.removeDeadStates(Operations.determinize(automaton, maxDeterminizedStates));
  }

  /**
   * Gets an integer id for a given term.
   *
   * If there is no position gaps for this token then we can reuse the id for the same term if it appeared at another
   * position without a gap.  If we have a position gap generate a new id so we can keep track of the position
   * increment.
   */
  private int getTermID(int incr, int prevIncr, BytesRef term) {
    assert term != null;
    boolean isStackedGap = incr == 0 && prevIncr > 1;
    boolean hasGap = incr > 1;
    Integer id;
    if (hasGap || isStackedGap) {
      id = idToTerm.size();
      idToTerm.put(id, BytesRef.deepCopyOf(term));

      // stacked token should have the same increment as original token at this position
      if (isStackedGap) {
        idToInc.put(id, prevIncr);
      } else {
        idToInc.put(id, incr);
      }
    } else {
      id = termToID.get(term);
      if (id == null) {
        term = BytesRef.deepCopyOf(term);
        id = idToTerm.size();
        termToID.put(term, id);
        idToTerm.put(id, term);
      }
    }

    return id;
  }
}