package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RunAutomaton;
import org.apache.lucene.util.automaton.SpecialOperations;
import org.apache.lucene.util.automaton.State;
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
public class AutomatonTermsEnum extends FilteredTermsEnum {
  // the object-oriented form of the DFA
  private final Automaton automaton;
  // a tableized array-based form of the DFA
  private final RunAutomaton runAutomaton;
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
  // used for unicode conversion from BytesRef byte[] to char[]
  private final UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
  // the reference used for seeking forwards through the term dictionary
  private final BytesRef seekBytesRef = new BytesRef(10); 
  // true if we are enumerating an infinite portion of the DFA.
  // in this case it is faster to drive the query based on the terms dictionary.
  // when this is true, linearUpperBound indicate the end of range
  // of terms where we should simply do sequential reads instead.
  private boolean linear = false;
  private final BytesRef linearUpperBound = new BytesRef(10);
  private final UnicodeUtil.UTF16Result linearUpperBoundUTF16 = new UnicodeUtil.UTF16Result();
  private final Comparator<BytesRef> termComp;

  /**
   * Expert ctor:
   * Construct an enumerator based upon an automaton, enumerating the specified
   * field, working on a supplied reader.
   * <p>
   * @lucene.internal Use the public ctor instead. This constructor allows the
   * (dangerous) option of passing in a pre-compiled RunAutomaton. If you use 
   * this ctor and compile your own RunAutomaton, you are responsible for 
   * ensuring it is in sync with the Automaton object, including internal
   * State numbering, or you will get undefined behavior.
   * <p>
   * @param preCompiled optional pre-compiled RunAutomaton (can be null)
   * @param finite true if the automaton accepts a finite language
   */
  AutomatonTermsEnum(Automaton automaton, RunAutomaton preCompiled,
      Term queryTerm, IndexReader reader, boolean finite)
      throws IOException {
    super(reader, queryTerm.field());
    this.automaton = automaton;
    this.finite = finite;

    /* 
     * tableize the automaton. this also ensures it is deterministic, and has no 
     * transitions to dead states. it also invokes Automaton.setStateNumbers to
     * number the original states (this is how they are tableized)
     */
    if (preCompiled == null)
      runAutomaton = new RunAutomaton(this.automaton);
    else
      runAutomaton = preCompiled;

    commonSuffixRef = finite ? null : new BytesRef(getValidUTF16Suffix(SpecialOperations
        .getCommonSuffix(automaton)));
    
    // build a cache of sorted transitions for every state
    allTransitions = new Transition[runAutomaton.getSize()][];
    for (State state : this.automaton.getStates())
      allTransitions[state.getNumber()] = state.getSortedTransitionArray(false);
    // used for path tracking, where each bit is a numbered state.
    visited = new long[runAutomaton.getSize()];

    setUseTermsCache(finite);
    termComp = getComparator();
  }
  
  /**
   * Construct an enumerator based upon an automaton, enumerating the specified
   * field, working on a supplied reader.
   * <p>
   * It will automatically calculate whether or not the automaton is finite
   */
  public AutomatonTermsEnum(Automaton automaton, Term queryTerm, IndexReader reader)
      throws IOException {
    this(automaton, null, queryTerm, reader, SpecialOperations.isFinite(automaton));
  }
 
  /**
   * Returns true if the term matches the automaton. Also stashes away the term
   * to assist with smart enumeration.
   */
  @Override
  protected AcceptStatus accept(final BytesRef term) {
    if (commonSuffixRef == null || term.endsWith(commonSuffixRef)) {
      UnicodeUtil.UTF8toUTF16(term.bytes, term.offset, term.length, utf16);
      if (runAutomaton.run(utf16.result, 0, utf16.length))
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
    if (term == null) {
      // return the empty term, as its valid
      if (runAutomaton.run("")) {
        seekBytesRef.copy("");
        return seekBytesRef;
      }
      
      utf16.copyText("");
    } else {
      UnicodeUtil.UTF8toUTF16(term.bytes, term.offset, term.length, utf16);
    }

    // seek to the next possible string;
    if (nextString()) {
      // reposition
      if (linear)
        setLinear(infinitePosition);
      UnicodeUtil.nextValidUTF16String(utf16);
      UnicodeUtil.UTF16toUTF8(utf16.result, 0, utf16.length, seekBytesRef);
      return seekBytesRef;
    }
    // no more possible strings can match
    return null;
  }

  // this instance prevents unicode conversion during backtracking,
  // we can just call setLinear once at the end.
  int infinitePosition;

  /**
   * Sets the enum to operate in linear fashion, as we have found
   * a looping transition at position
   */
  private void setLinear(int position) {
    int state = runAutomaton.getInitialState();
    char maxInterval = 0xffff;
    for (int i = 0; i < position; i++)
      state = runAutomaton.step(state, utf16.result[i]);
    for (int i = 0; i < allTransitions[state].length; i++) {
      Transition t = allTransitions[state][i];
      if (t.getMin() <= utf16.result[position] && utf16.result[position] <= t.getMax()) {
        maxInterval = t.getMax();
        break;
      }
    }
    // 0xffff terms don't get the optimization... not worth the trouble.
    if (maxInterval < 0xffff)
      maxInterval++;
    int length = position + 1; /* position + maxTransition */
    if (linearUpperBoundUTF16.result.length < length)
      linearUpperBoundUTF16.result = new char[length];
    System.arraycopy(utf16.result, 0, linearUpperBoundUTF16.result, 0, position);
    linearUpperBoundUTF16.result[position] = maxInterval;
    linearUpperBoundUTF16.setLength(length);
    UnicodeUtil.nextValidUTF16String(linearUpperBoundUTF16);
    UnicodeUtil.UTF16toUTF8(linearUpperBoundUTF16.result, 0, length, linearUpperBound);
  }

  /**
   * Increments the utf16 buffer to the next String in lexicographic order after s that will not put
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

    while (true) {
      curGen++;
      linear = false;
      state = runAutomaton.getInitialState();
      // walk the automaton until a character is rejected.
      for (pos = 0; pos < utf16.length; pos++) {
        visited[state] = curGen;
        int nextState = runAutomaton.step(state, utf16.result[pos]);
        if (nextState == -1)
          break;
        // we found a loop, record it for faster enumeration
        if (!finite && !linear && visited[nextState] == curGen) {
          linear = true;
          infinitePosition = pos;
        }
        state = nextState;
      }

      // take the useful portion, and the last non-reject state, and attempt to
      // append characters that will match.
      if (nextString(state, pos)) {
        return true;
      } else { /* no more solutions exist from this useful portion, backtrack */
        if (!backtrack(pos)) /* no more solutions at all */
          return false;
        else if (runAutomaton.run(utf16.result, 0, utf16.length)) 
          /* String is good to go as-is */
          return true;
        /* else advance further */
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
    char c = 0;
    if (position < utf16.length) {
      c = utf16.result[position];
      // if the next character is U+FFFF and is not part of the useful portion,
      // then by definition it puts us in a reject state, and therefore this
      // path is dead. there cannot be any higher transitions. backtrack.
      if (c == '\uFFFF')
        return false;
      else
        c++;
    }

    utf16.setLength(position);
    visited[state] = curGen;

    Transition transitions[] = allTransitions[state];

    // find the minimal path (lexicographic order) that is >= c
    
    for (int i = 0; i < transitions.length; i++) {
      Transition transition = transitions[i];
      if (transition.getMax() >= c) {
        char nextChar = (char) Math.max(c, transition.getMin());
        // append either the next sequential char, or the minimum transition
        utf16.setLength(utf16.length + 1);
        utf16.result[utf16.length - 1] = nextChar;
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
          // we found a loop, record it for faster enumeration
          if (!finite && !linear && visited[state] == curGen) {
            linear = true;
            infinitePosition = utf16.length;
          }
          // append the minimum transition
          utf16.setLength(utf16.length + 1);
          utf16.result[utf16.length - 1] = transition.getMin();
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
   * @return true if more possible solutions exist for the DFA
   */
  private boolean backtrack(int position) {
    while (position > 0) {
      char nextChar = utf16.result[position - 1];
      // if a character is U+FFFF its a dead-end too,
      // because there is no higher character in UTF-16 sort order.
      if (nextChar != '\uFFFF') {
        nextChar++;
        utf16.result[position - 1] = nextChar;
        utf16.setLength(position);
        return true;
      }
      position--;
    }
    return false; /* all solutions exhausted */
  }
  
  /**
   * if the suffix starts with a low surrogate, remove it.
   * This won't be quite as efficient, but can be converted to valid UTF-8
   * 
   * This isn't nearly as complex as cleanupPosition, because its not 
   * going to use this suffix to walk any path thru the terms.
   * 
   */
  private String getValidUTF16Suffix(String suffix) {
    if (suffix != null && suffix.length() > 0 &&
        Character.isLowSurrogate(suffix.charAt(0)))
      return suffix.substring(1);
    else
      return suffix;
  }
}
