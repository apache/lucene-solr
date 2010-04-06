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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <tt>Automaton</tt> state.
 * 
 * @lucene.experimental
 */
public class State implements Serializable, Comparable<State> {
  
  boolean accept;
  Set<Transition> transitions;
  
  int number;

  int id;
  static int next_id;
  
  /**
   * Constructs a new state. Initially, the new state is a reject state.
   */
  public State() {
    resetTransitions();
    id = next_id++;
  }
  
  /**
   * Resets transition set.
   */
  final void resetTransitions() {
    transitions = new HashSet<Transition>();
  }
  
  /**
   * Returns the set of outgoing transitions. Subsequent changes are reflected
   * in the automaton.
   * 
   * @return transition set
   */
  public Set<Transition> getTransitions() {
    return transitions;
  }
  
  /**
   * Adds an outgoing transition.
   * 
   * @param t transition
   */
  public void addTransition(Transition t) {
    transitions.add(t);
  }
  
  /**
   * Sets acceptance for this state.
   * 
   * @param accept if true, this state is an accept state
   */
  public void setAccept(boolean accept) {
    this.accept = accept;
  }
  
  /**
   * Returns acceptance status.
   * 
   * @return true is this is an accept state
   */
  public boolean isAccept() {
    return accept;
  }
  
  /**
   * Performs lookup in transitions, assuming determinism.
   * 
   * @param c character to look up
   * @return destination state, null if no matching outgoing transition
   * @see #step(char, Collection)
   */
  public State step(char c) {
    for (Transition t : transitions)
      if (t.min <= c && c <= t.max) return t.to;
    return null;
  }
  
  /**
   * Performs lookup in transitions, allowing nondeterminism.
   * 
   * @param c character to look up
   * @param dest collection where destination states are stored
   * @see #step(char)
   */
  public void step(char c, Collection<State> dest) {
    for (Transition t : transitions)
      if (t.min <= c && c <= t.max) dest.add(t.to);
  }
  
  void addEpsilon(State to) {
    if (to.accept) accept = true;
    for (Transition t : to.transitions)
      transitions.add(t);
  }
  
  /**
   * Returns transitions sorted by (min, reverse max, to) or (to, min, reverse
   * max)
   */
  public Transition[] getSortedTransitionArray(boolean to_first) {
    Transition[] e = transitions.toArray(new Transition[transitions.size()]);
    Arrays.sort(e, new TransitionComparator(to_first));
    return e;
  }
  
  /**
   * Returns sorted list of outgoing transitions.
   * 
   * @param to_first if true, order by (to, min, reverse max); otherwise (min,
   *          reverse max, to)
   * @return transition list
   */
  public List<Transition> getSortedTransitions(boolean to_first) {
    return Arrays.asList(getSortedTransitionArray(to_first));
  }
  
  
  /**
   * Return this state's number. 
   * <p>
   * Expert: Will be useless unless {@link Automaton#setStateNumbers(Set)}
   * has been called first to number the states.
   * @return the number
   */
  public int getNumber() {
    return number;
  }
  
  /**
   * Returns string describing this state. Normally invoked via
   * {@link Automaton#toString()}.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("state ").append(number);
    if (accept) b.append(" [accept]");
    else b.append(" [reject]");
    b.append(":\n");
    for (Transition t : transitions)
      b.append("  ").append(t.toString()).append("\n");
    return b.toString();
  }
  
  /**
   * Compares this object with the specified object for order. States are
   * ordered by the time of construction.
   */
  public int compareTo(State s) {
    return s.id - id;
  }
  
  /**
   * See {@link java.lang.Object#equals(java.lang.Object)}.
   */
  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
  
  /**
   * See {@link java.lang.Object#hashCode()}.
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
