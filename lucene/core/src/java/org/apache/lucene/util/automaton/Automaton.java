/*
 * dk.brics.automaton
 * 
 * Copyright (c) 2001-2009 Anders Moeller
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.lucene.util.automaton;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Finite-state automaton with regular expression operations.
 * <p>
 * Class invariants:
 * <ul>
 * <li>An automaton is either represented explicitly (with {@link State} and
 * {@link Transition} objects) or with a singleton string (see
 * {@link #getSingleton()} and {@link #expandSingleton()}) in case the automaton
 * is known to accept exactly one string. (Implicitly, all states and
 * transitions of an automaton are reachable from its initial state.)
 * <li>Automata are always reduced (see {@link #reduce()}) and have no
 * transitions to dead states (see {@link #removeDeadTransitions()}).
 * <li>If an automaton is nondeterministic, then {@link #isDeterministic()}
 * returns false (but the converse is not required).
 * <li>Automata provided as input to operations are generally assumed to be
 * disjoint.
 * </ul>
 * <p>
 * If the states or transitions are manipulated manually, the
 * {@link #restoreInvariant()} and {@link #setDeterministic(boolean)} methods
 * should be used afterwards to restore representation invariants that are
 * assumed by the built-in automata operations.
 * 
 * <p>
 * <p>
 * Note: This class has internal mutable state and is not thread safe. It is 
 * the caller's responsibility to ensure any necessary synchronization if you
 * wish to use the same Automaton from multiple threads. In general it is instead
 * recommended to use a {@link RunAutomaton} for multithreaded matching: it is immutable, 
 * thread safe, and much faster.  
 * </p>
 * @lucene.experimental
 */
public class Automaton implements Cloneable {
  
  /**
   * Minimize using Hopcroft's O(n log n) algorithm. This is regarded as one of
   * the most generally efficient algorithms that exist.
   * 
   * @see #setMinimization(int)
   */
  public static final int MINIMIZE_HOPCROFT = 2;
  
  /** Selects minimization algorithm (default: <code>MINIMIZE_HOPCROFT</code>). */
  static int minimization = MINIMIZE_HOPCROFT;
  
  /** Initial state of this automaton. */
  State initial;
  
  /**
   * If true, then this automaton is definitely deterministic (i.e., there are
   * no choices for any run, but a run may crash).
   */
  boolean deterministic;
  
  /** Extra data associated with this automaton. */
  transient Object info;
  
  /**
   * Hash code. Recomputed by {@link MinimizationOperations#minimize(Automaton)}
   */
  //int hash_code;
  
  /** Singleton string. Null if not applicable. */
  String singleton;
  
  /** Minimize always flag. */
  static boolean minimize_always = false;
  
  /**
   * Selects whether operations may modify the input automata (default:
   * <code>false</code>).
   */
  static boolean allow_mutation = false;
  
  /**
   * Constructs a new automaton that accepts the empty language. Using this
   * constructor, automata can be constructed manually from {@link State} and
   * {@link Transition} objects.
   * 
   * @see State
   * @see Transition
   */
  public Automaton(State initial) {
    this.initial = initial;
    deterministic = true;
    singleton = null;
  }

  public Automaton() {
    this(new State());
  }
  
  /**
   * Selects minimization algorithm (default: <code>MINIMIZE_HOPCROFT</code>).
   * 
   * @param algorithm minimization algorithm
   */
  static public void setMinimization(int algorithm) {
    minimization = algorithm;
  }
  
  /**
   * Sets or resets minimize always flag. If this flag is set, then
   * {@link MinimizationOperations#minimize(Automaton)} will automatically be
   * invoked after all operations that otherwise may produce non-minimal
   * automata. By default, the flag is not set.
   * 
   * @param flag if true, the flag is set
   */
  static public void setMinimizeAlways(boolean flag) {
    minimize_always = flag;
  }
  
  /**
   * Sets or resets allow mutate flag. If this flag is set, then all automata
   * operations may modify automata given as input; otherwise, operations will
   * always leave input automata languages unmodified. By default, the flag is
   * not set.
   * 
   * @param flag if true, the flag is set
   * @return previous value of the flag
   */
  static public boolean setAllowMutate(boolean flag) {
    boolean b = allow_mutation;
    allow_mutation = flag;
    return b;
  }
  
  /**
   * Returns the state of the allow mutate flag. If this flag is set, then all
   * automata operations may modify automata given as input; otherwise,
   * operations will always leave input automata languages unmodified. By
   * default, the flag is not set.
   * 
   * @return current value of the flag
   */
  static boolean getAllowMutate() {
    return allow_mutation;
  }
  
  void checkMinimizeAlways() {
    if (minimize_always) MinimizationOperations.minimize(this);
  }
  
  boolean isSingleton() {
    return singleton != null;
  }
  
  /**
   * Returns the singleton string for this automaton. An automaton that accepts
   * exactly one string <i>may</i> be represented in singleton mode. In that
   * case, this method may be used to obtain the string.
   * 
   * @return string, null if this automaton is not in singleton mode.
   */
  public String getSingleton() {
    return singleton;
  }
  
  /**
   * Sets initial state.
   * 
   * @param s state
   */
  /*
  public void setInitialState(State s) {
    initial = s;
    singleton = null;
  }
  */
  
  /**
   * Gets initial state.
   * 
   * @return state
   */
  public State getInitialState() {
    expandSingleton();
    return initial;
  }
  
  /**
   * Returns deterministic flag for this automaton.
   * 
   * @return true if the automaton is definitely deterministic, false if the
   *         automaton may be nondeterministic
   */
  public boolean isDeterministic() {
    return deterministic;
  }
  
  /**
   * Sets deterministic flag for this automaton. This method should (only) be
   * used if automata are constructed manually.
   * 
   * @param deterministic true if the automaton is definitely deterministic,
   *          false if the automaton may be nondeterministic
   */
  public void setDeterministic(boolean deterministic) {
    this.deterministic = deterministic;
  }
  
  /**
   * Associates extra information with this automaton.
   * 
   * @param info extra information
   */
  public void setInfo(Object info) {
    this.info = info;
  }
  
  /**
   * Returns extra information associated with this automaton.
   * 
   * @return extra information
   * @see #setInfo(Object)
   */
  public Object getInfo() {
    return info;
  }

  // cached
  private State[] numberedStates;

  public State[] getNumberedStates() {
    if (numberedStates == null) {
      expandSingleton();
      final Set<State> visited = new HashSet<State>();
      final LinkedList<State> worklist = new LinkedList<State>();
      numberedStates = new State[4];
      int upto = 0;
      worklist.add(initial);
      visited.add(initial);
      initial.number = upto;
      numberedStates[upto] = initial;
      upto++;
      while (worklist.size() > 0) {
        State s = worklist.removeFirst();
        for (int i=0;i<s.numTransitions;i++) {
          final Transition t = s.transitionsArray[i];
          if (!visited.contains(t.to)) {
            visited.add(t.to);
            worklist.add(t.to);
            t.to.number = upto;
            if (upto == numberedStates.length) {
              final State[] newArray = new State[ArrayUtil.oversize(1+upto, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
              System.arraycopy(numberedStates, 0, newArray, 0, upto);
              numberedStates = newArray;
            }
            numberedStates[upto] = t.to;
            upto++;
          }
        }
      }
      if (numberedStates.length != upto) {
        final State[] newArray = new State[upto];
        System.arraycopy(numberedStates, 0, newArray, 0, upto);
        numberedStates = newArray;
      }
    }

    return numberedStates;
  }

  public void setNumberedStates(State[] states) {
    setNumberedStates(states, states.length);
  }

  public void setNumberedStates(State[] states, int count) {
    assert count <= states.length;
    // TODO: maybe we can eventually allow for oversizing here...
    if (count < states.length) {
      final State[] newArray = new State[count];
      System.arraycopy(states, 0, newArray, 0, count);
      numberedStates = newArray;
    } else {
      numberedStates = states;
    }
  }

  public void clearNumberedStates() {
    numberedStates = null;
  }

  /**
   * Returns the set of reachable accept states.
   * 
   * @return set of {@link State} objects
   */
  public Set<State> getAcceptStates() {
    expandSingleton();
    HashSet<State> accepts = new HashSet<State>();
    HashSet<State> visited = new HashSet<State>();
    LinkedList<State> worklist = new LinkedList<State>();
    worklist.add(initial);
    visited.add(initial);
    while (worklist.size() > 0) {
      State s = worklist.removeFirst();
      if (s.accept) accepts.add(s);
      for (Transition t : s.getTransitions())
        if (!visited.contains(t.to)) {
          visited.add(t.to);
          worklist.add(t.to);
        }
    }
    return accepts;
  }
  
  /**
   * Adds transitions to explicit crash state to ensure that transition function
   * is total.
   */
  void totalize() {
    State s = new State();
    s.addTransition(new Transition(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT,
        s));
    for (State p : getNumberedStates()) {
      int maxi = Character.MIN_CODE_POINT;
      p.sortTransitions(Transition.CompareByMinMaxThenDest);
      for (Transition t : p.getTransitions()) {
        if (t.min > maxi) p.addTransition(new Transition(maxi,
            (t.min - 1), s));
        if (t.max + 1 > maxi) maxi = t.max + 1;
      }
      if (maxi <= Character.MAX_CODE_POINT) p.addTransition(new Transition(
          maxi, Character.MAX_CODE_POINT, s));
    }
    clearNumberedStates();
  }
  
  /**
   * Restores representation invariant. This method must be invoked before any
   * built-in automata operation is performed if automaton states or transitions
   * are manipulated manually.
   * 
   * @see #setDeterministic(boolean)
   */
  public void restoreInvariant() {
    removeDeadTransitions();
  }
  
  /**
   * Reduces this automaton. An automaton is "reduced" by combining overlapping
   * and adjacent edge intervals with same destination.
   */
  public void reduce() {
    final State[] states = getNumberedStates();
    if (isSingleton()) return;
    for (State s : states)
      s.reduce();
  }
  
  /**
   * Returns sorted array of all interval start points.
   */
  int[] getStartPoints() {
    final State[] states = getNumberedStates();
    Set<Integer> pointset = new HashSet<Integer>();
    pointset.add(Character.MIN_CODE_POINT);
    for (State s : states) {
      for (Transition t : s.getTransitions()) {
        pointset.add(t.min);
        if (t.max < Character.MAX_CODE_POINT) pointset.add((t.max + 1));
      }
    }
    int[] points = new int[pointset.size()];
    int n = 0;
    for (Integer m : pointset)
      points[n++] = m;
    Arrays.sort(points);
    return points;
  }
  
  /**
   * Returns the set of live states. A state is "live" if an accept state is
   * reachable from it.
   * 
   * @return set of {@link State} objects
   */
  private State[] getLiveStates() {
    final State[] states = getNumberedStates();
    Set<State> live = new HashSet<State>();
    for (State q : states) {
      if (q.isAccept()) {
        live.add(q);
      }
    }
    // map<state, set<state>>
    @SuppressWarnings({"rawtypes","unchecked"}) Set<State> map[] = new Set[states.length];
    for (int i = 0; i < map.length; i++)
      map[i] = new HashSet<State>();
    for (State s : states) {
      for(int i=0;i<s.numTransitions;i++) {
        map[s.transitionsArray[i].to.number].add(s);
      }
    }
    LinkedList<State> worklist = new LinkedList<State>(live);
    while (worklist.size() > 0) {
      State s = worklist.removeFirst();
      for (State p : map[s.number])
        if (!live.contains(p)) {
          live.add(p);
          worklist.add(p);
        }
    }

    return live.toArray(new State[live.size()]);
  }

  /**
   * Removes transitions to dead states and calls {@link #reduce()}.
   * (A state is "dead" if no accept state is
   * reachable from it.)
   */
  public void removeDeadTransitions() {
    final State[] states = getNumberedStates();
    //clearHashCode();
    if (isSingleton()) return;
    State[] live = getLiveStates();

    BitSet liveSet = new BitSet(states.length);
    for (State s : live)
      liveSet.set(s.number);

    for (State s : states) {
      // filter out transitions to dead states:
      int upto = 0;
      for(int i=0;i<s.numTransitions;i++) {
        final Transition t = s.transitionsArray[i];
        if (liveSet.get(t.to.number)) {
          s.transitionsArray[upto++] = s.transitionsArray[i];
        }
      }
      s.numTransitions = upto;
    }
    for(int i=0;i<live.length;i++) {
      live[i].number = i;
    }
    if (live.length > 0) {
      setNumberedStates(live);
    } else {
      // sneaky corner case -- if machine accepts no strings
      clearNumberedStates();
    }
    reduce();
  }
  
  /**
   * Returns a sorted array of transitions for each state (and sets state
   * numbers).
   */
  public Transition[][] getSortedTransitions() {
    final State[] states = getNumberedStates();
    Transition[][] transitions = new Transition[states.length][];
    for (State s : states) {
      s.sortTransitions(Transition.CompareByMinMaxThenDest);
      s.trimTransitionsArray();
      transitions[s.number] = s.transitionsArray;
      assert s.transitionsArray != null;
    }
    return transitions;
  }
  
  /**
   * Expands singleton representation to normal representation. Does nothing if
   * not in singleton representation.
   */
  public void expandSingleton() {
    if (isSingleton()) {
      State p = new State();
      initial = p;
      for (int i = 0, cp = 0; i < singleton.length(); i += Character.charCount(cp)) {
        State q = new State();
        p.addTransition(new Transition(cp = singleton.codePointAt(i), q));
        p = q;
      }
      p.accept = true;
      deterministic = true;
      singleton = null;
    }
  }
  
  /**
   * Returns the number of states in this automaton.
   */
  public int getNumberOfStates() {
    if (isSingleton()) return singleton.codePointCount(0, singleton.length()) + 1;
    return getNumberedStates().length;
  }
  
  /**
   * Returns the number of transitions in this automaton. This number is counted
   * as the total number of edges, where one edge may be a character interval.
   */
  public int getNumberOfTransitions() {
    if (isSingleton()) return singleton.codePointCount(0, singleton.length());
    int c = 0;
    for (State s : getNumberedStates())
      c += s.numTransitions();
    return c;
  }
  
  @Override
  public boolean equals(Object obj) {
    throw new UnsupportedOperationException("use BasicOperations.sameLanguage instead");
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Must be invoked when the stored hash code may no longer be valid.
   */
  /*
  void clearHashCode() {
    hash_code = 0;
  }
  */
  
  /**
   * Returns a string representation of this automaton.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (isSingleton()) {
      b.append("singleton: ");
      int length = singleton.codePointCount(0, singleton.length());
      int codepoints[] = new int[length];
      for (int i = 0, j = 0, cp = 0; i < singleton.length(); i += Character.charCount(cp))
        codepoints[j++] = cp = singleton.codePointAt(i);
      for (int c : codepoints)
        Transition.appendCharString(c, b);
      b.append("\n");
    } else {
      State[] states = getNumberedStates();
      b.append("initial state: ").append(initial.number).append("\n");
      for (State s : states)
        b.append(s.toString());
    }
    return b.toString();
  }
  
  /**
   * Returns <a href="http://www.research.att.com/sw/tools/graphviz/"
   * target="_top">Graphviz Dot</a> representation of this automaton.
   */
  public String toDot() {
    StringBuilder b = new StringBuilder("digraph Automaton {\n");
    b.append("  rankdir = LR;\n");
    State[] states = getNumberedStates();
    for (State s : states) {
      b.append("  ").append(s.number);
      if (s.accept) b.append(" [shape=doublecircle,label=\"\"];\n");
      else b.append(" [shape=circle,label=\"\"];\n");
      if (s == initial) {
        b.append("  initial [shape=plaintext,label=\"\"];\n");
        b.append("  initial -> ").append(s.number).append("\n");
      }
      for (Transition t : s.getTransitions()) {
        b.append("  ").append(s.number);
        t.appendDot(b);
      }
    }
    return b.append("}\n").toString();
  }
  
  /**
   * Returns a clone of this automaton, expands if singleton.
   */
  Automaton cloneExpanded() {
    Automaton a = clone();
    a.expandSingleton();
    return a;
  }
  
  /**
   * Returns a clone of this automaton unless <code>allow_mutation</code> is
   * set, expands if singleton.
   */
  Automaton cloneExpandedIfRequired() {
    if (allow_mutation) {
      expandSingleton();
      return this;
    } else return cloneExpanded();
  }
  
  /**
   * Returns a clone of this automaton.
   */
  @Override
  public Automaton clone() {
    try {
      Automaton a = (Automaton) super.clone();
      if (!isSingleton()) {
        HashMap<State,State> m = new HashMap<State,State>();
        State[] states = getNumberedStates();
        for (State s : states)
          m.put(s, new State());
        for (State s : states) {
          State p = m.get(s);
          p.accept = s.accept;
          if (s == initial) a.initial = p;
          for (Transition t : s.getTransitions())
            p.addTransition(new Transition(t.min, t.max, m.get(t.to)));
        }
      }
      a.clearNumberedStates();
      return a;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Returns a clone of this automaton, or this automaton itself if
   * <code>allow_mutation</code> flag is set.
   */
  Automaton cloneIfRequired() {
    if (allow_mutation) return this;
    else return clone();
  }
  
  /**
   * See {@link BasicOperations#concatenate(Automaton, Automaton)}.
   */
  public Automaton concatenate(Automaton a) {
    return BasicOperations.concatenate(this, a);
  }
  
  /**
   * See {@link BasicOperations#concatenate(List)}.
   */
  static public Automaton concatenate(List<Automaton> l) {
    return BasicOperations.concatenate(l);
  }
  
  /**
   * See {@link BasicOperations#optional(Automaton)}.
   */
  public Automaton optional() {
    return BasicOperations.optional(this);
  }
  
  /**
   * See {@link BasicOperations#repeat(Automaton)}.
   */
  public Automaton repeat() {
    return BasicOperations.repeat(this);
  }
  
  /**
   * See {@link BasicOperations#repeat(Automaton, int)}.
   */
  public Automaton repeat(int min) {
    return BasicOperations.repeat(this, min);
  }
  
  /**
   * See {@link BasicOperations#repeat(Automaton, int, int)}.
   */
  public Automaton repeat(int min, int max) {
    return BasicOperations.repeat(this, min, max);
  }
  
  /**
   * See {@link BasicOperations#complement(Automaton)}.
   */
  public Automaton complement() {
    return BasicOperations.complement(this);
  }
  
  /**
   * See {@link BasicOperations#minus(Automaton, Automaton)}.
   */
  public Automaton minus(Automaton a) {
    return BasicOperations.minus(this, a);
  }
  
  /**
   * See {@link BasicOperations#intersection(Automaton, Automaton)}.
   */
  public Automaton intersection(Automaton a) {
    return BasicOperations.intersection(this, a);
  }
  
  /**
   * See {@link BasicOperations#subsetOf(Automaton, Automaton)}.
   */
  public boolean subsetOf(Automaton a) {
    return BasicOperations.subsetOf(this, a);
  }
  
  /**
   * See {@link BasicOperations#union(Automaton, Automaton)}.
   */
  public Automaton union(Automaton a) {
    return BasicOperations.union(this, a);
  }
  
  /**
   * See {@link BasicOperations#union(Collection)}.
   */
  static public Automaton union(Collection<Automaton> l) {
    return BasicOperations.union(l);
  }
  
  /**
   * See {@link BasicOperations#determinize(Automaton)}.
   */
  public void determinize() {
    BasicOperations.determinize(this);
  }
  
  /**
   * See {@link BasicOperations#isEmptyString(Automaton)}.
   */
  public boolean isEmptyString() {
    return BasicOperations.isEmptyString(this);
  }
  
  /**
   * See {@link MinimizationOperations#minimize(Automaton)}. Returns the
   * automaton being given as argument.
   */
  public static Automaton minimize(Automaton a) {
    MinimizationOperations.minimize(a);
    return a;
  }
}
