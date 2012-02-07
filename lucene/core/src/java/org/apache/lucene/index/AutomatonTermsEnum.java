package org.apache.lucene.index;

/**
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
import java.util.Comparator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Transition;

/**
 * A FilteredTermsEnum that enumerates terms based upon what is accepted by a
 * DFA.
 * <p>
 * The algorithm is such:
 * <ol>
 *   <li>As long as matches are successful, keep reading sequentially.
 *   <li>When a match fails, skip to the next string in lexicographic order that
 * does not enter a reject state.
 * </ol>
 * <p>
 * The algorithm does not attempt to actually skip to the next string that is
 * completely accepted. This is not possible when the language accepted by the
 * FSM is not finite (i.e. * operator).
 * </p>
 * @lucene.experimental
 */
class AutomatonTermsEnum extends FilteredTermsEnum {
  // a tableized array-based form of the DFA
  private final ByteRunAutomaton runAutomaton;
  // common suffix of the automaton
  private final BytesRef commonSuffixRef;
  // true if the automaton accepts a finite language
  private final boolean finite;
  // array of sorted transitions for each state, indexed by state number
  private final Transition[][] allTransitions;
  // for path tracking: each long records gen when we last
  // visited the state; we use gens to avoid having to clear
  private final long[] visited;
  private long curGen;
  // the reference used for seeking forwards through the term dictionary
  private final BytesRef seekBytesRef = new BytesRef(10); 
  // true if we are enumerating an infinite portion of the DFA.
  // in this case it is faster to drive the query based on the terms dictionary.
  // when this is true, linearUpperBound indicate the end of range
  // of terms where we should simply do sequential reads instead.
  private boolean linear = false;
  private final BytesRef linearUpperBound = new BytesRef(10);
  private final Comparator<BytesRef> termComp;

  /**
   * Construct an enumerator based upon an automaton, enumerating the specified
   * field, working on a supplied TermsEnum
   * <p>
   * @lucene.experimental 
   * <p>
   * @param compiled CompiledAutomaton
   */
  public AutomatonTermsEnum(TermsEnum tenum, CompiledAutomaton compiled) throws IOException {
    super(tenum);
    this.finite = compiled.finite;
    this.runAutomaton = compiled.runAutomaton;
    assert this.runAutomaton != null;
    this.commonSuffixRef = compiled.commonSuffixRef;
    this.allTransitions = compiled.sortedTransitions;

    // used for path tracking, where each bit is a numbered state.
    visited = new long[runAutomaton.getSize()];

    termComp = getComparator();
  }
  
  /**
   * Returns true if the term matches the automaton. Also stashes away the term
   * to assist with smart enumeration.
   */
  @Override
  protected AcceptStatus accept(final BytesRef term) {
    if (commonSuffixRef == null || StringHelper.endsWith(term, commonSuffixRef)) {
      if (runAutomaton.run(term.bytes, term.offset, term.length))
        return linear ? AcceptStatus.YES : AcceptStatus.YES_AND_SEEK;
      else
        return (linear && termComp.compare(term, linearUpperBound) < 0) ? 
            AcceptStatus.NO : AcceptStatus.NO_AND_SEEK;
    } else {
      return (linear && termComp.compare(term, linearUpperBound) < 0) ? 
          AcceptStatus.NO : AcceptStatus.NO_AND_SEEK;
    }
  }
  
  @Override
  protected BytesRef nextSeekTerm(final BytesRef term) throws IOException {
    //System.out.println("ATE.nextSeekTerm term=" + term);
    if (term == null) {
      assert seekBytesRef.length == 0;
      // return the empty term, as its valid
      if (runAutomaton.isAccept(runAutomaton.getInitialState())) {   
        return seekBytesRef;
      }
    } else {
      seekBytesRef.copyBytes(term);
    }

    // seek to the next possible string;
    if (nextString()) {
      return seekBytesRef;  // reposition
    } else {
      return null;          // no more possible strings can match
    }
  }

  /**
   * Sets the enum to operate in linear fashion, as we have found
   * a looping transition at position: we set an upper bound and 
   * act like a TermRangeQuery for this portion of the term space.
   */
  private void setLinear(int position) {
    assert linear == false;
    
    int state = runAutomaton.getInitialState();
    int maxInterval = 0xff;
    for (int i = 0; i < position; i++) {
      state = runAutomaton.step(state, seekBytesRef.bytes[i] & 0xff);
      assert state >= 0: "state=" + state;
    }
    for (int i = 0; i < allTransitions[state].length; i++) {
      Transition t = allTransitions[state][i];
      if (t.getMin() <= (seekBytesRef.bytes[position] & 0xff) && 
          (seekBytesRef.bytes[position] & 0xff) <= t.getMax()) {
        maxInterval = t.getMax();
        break;
      }
    }
    // 0xff terms don't get the optimization... not worth the trouble.
    if (maxInterval != 0xff)
      maxInterval++;
    int length = position + 1; /* position + maxTransition */
    if (linearUpperBound.bytes.length < length)
      linearUpperBound.bytes = new byte[length];
    System.arraycopy(seekBytesRef.bytes, 0, linearUpperBound.bytes, 0, position);
    linearUpperBound.bytes[position] = (byte) maxInterval;
    linearUpperBound.length = length;
    
    linear = true;
  }

  private final IntsRef savedStates = new IntsRef(10);
  
  /**
   * Increments the byte buffer to the next String in binary order after s that will not put
   * the machine into a reject state. If such a string does not exist, returns
   * false.
   * 
   * The correctness of this method depends upon the automaton being deterministic,
   * and having no transitions to dead states.
   * 
   * @return true if more possible solutions exist for the DFA
   */
  private boolean nextString() {
    int state;
    int pos = 0;
    savedStates.grow(seekBytesRef.length+1);
    final int[] states = savedStates.ints;
    states[0] = runAutomaton.getInitialState();
    
    while (true) {
      curGen++;
      linear = false;
      // walk the automaton until a character is rejected.
      for (state = states[pos]; pos < seekBytesRef.length; pos++) {
        visited[state] = curGen;
        int nextState = runAutomaton.step(state, seekBytesRef.bytes[pos] & 0xff);
        if (nextState == -1)
          break;
        states[pos+1] = nextState;
        // we found a loop, record it for faster enumeration
        if (!finite && !linear && visited[nextState] == curGen) {
          setLinear(pos);
        }
        state = nextState;
      }

      // take the useful portion, and the last non-reject state, and attempt to
      // append characters that will match.
      if (nextString(state, pos)) {
        return true;
      } else { /* no more solutions exist from this useful portion, backtrack */
        if ((pos = backtrack(pos)) < 0) /* no more solutions at all */
          return false;
        final int newState = runAutomaton.step(states[pos], seekBytesRef.bytes[pos] & 0xff);
        if (newState >= 0 && runAutomaton.isAccept(newState))
          /* String is good to go as-is */
          return true;
        /* else advance further */
        // TODO: paranoia? if we backtrack thru an infinite DFA, the loop detection is important!
        // for now, restart from scratch for all infinite DFAs 
        if (!finite) pos = 0;
      }
    }
  }
  
  /**
   * Returns the next String in lexicographic order that will not put
   * the machine into a reject state. 
   * 
   * This method traverses the DFA from the given position in the String,
   * starting at the given state.
   * 
   * If this cannot satisfy the machine, returns false. This method will
   * walk the minimal path, in lexicographic order, as long as possible.
   * 
   * If this method returns false, then there might still be more solutions,
   * it is necessary to backtrack to find out.
   * 
   * @param state current non-reject state
   * @param position useful portion of the string
   * @return true if more possible solutions exist for the DFA from this
   *         position
   */
  private boolean nextString(int state, int position) {
    /* 
     * the next lexicographic character must be greater than the existing
     * character, if it exists.
     */
    int c = 0;
    if (position < seekBytesRef.length) {
      c = seekBytesRef.bytes[position] & 0xff;
      // if the next byte is 0xff and is not part of the useful portion,
      // then by definition it puts us in a reject state, and therefore this
      // path is dead. there cannot be any higher transitions. backtrack.
      if (c++ == 0xff)
        return false;
    }

    seekBytesRef.length = position;
    visited[state] = curGen;

    Transition transitions[] = allTransitions[state];

    // find the minimal path (lexicographic order) that is >= c
    
    for (int i = 0; i < transitions.length; i++) {
      Transition transition = transitions[i];
      if (transition.getMax() >= c) {
        int nextChar = Math.max(c, transition.getMin());
        // append either the next sequential char, or the minimum transition
        seekBytesRef.grow(seekBytesRef.length + 1);
        seekBytesRef.length++;
        seekBytesRef.bytes[seekBytesRef.length - 1] = (byte) nextChar;
        state = transition.getDest().getNumber();
        /* 
         * as long as is possible, continue down the minimal path in
         * lexicographic order. if a loop or accept state is encountered, stop.
         */
        while (visited[state] != curGen && !runAutomaton.isAccept(state)) {
          visited[state] = curGen;
          /* 
           * Note: we work with a DFA with no transitions to dead states.
           * so the below is ok, if it is not an accept state,
           * then there MUST be at least one transition.
           */
          transition = allTransitions[state][0];
          state = transition.getDest().getNumber();
          
          // append the minimum transition
          seekBytesRef.grow(seekBytesRef.length + 1);
          seekBytesRef.length++;
          seekBytesRef.bytes[seekBytesRef.length - 1] = (byte) transition.getMin();
          
          // we found a loop, record it for faster enumeration
          if (!finite && !linear && visited[state] == curGen) {
            setLinear(seekBytesRef.length-1);
          }
        }
        return true;
      }
    }
    return false;
  }
  
  /**
   * Attempts to backtrack thru the string after encountering a dead end
   * at some given position. Returns false if no more possible strings 
   * can match.
   * 
   * @param position current position in the input String
   * @return position >=0 if more possible solutions exist for the DFA
   */
  private int backtrack(int position) {
    while (position-- > 0) {
      int nextChar = seekBytesRef.bytes[position] & 0xff;
      // if a character is 0xff its a dead-end too,
      // because there is no higher character in binary sort order.
      if (nextChar++ != 0xff) {
        seekBytesRef.bytes[position] = (byte) nextChar;
        seekBytesRef.length = position+1;
        return position;
      }
    }
    return -1; /* all solutions exhausted */
  }
}
