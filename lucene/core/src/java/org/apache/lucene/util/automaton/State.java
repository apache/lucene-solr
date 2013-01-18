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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

/**
 * <tt>Automaton</tt> state.
 * 
 * @lucene.experimental
 */
public class State implements Comparable<State> {
  
  boolean accept;
  public Transition[] transitionsArray;
  public int numTransitions;
  
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
    transitionsArray = new Transition[0];
    numTransitions = 0;
  }

  private class TransitionsIterable implements Iterable<Transition> {
    @Override
    public Iterator<Transition> iterator() {
      return new Iterator<Transition>() {
        int upto;
        @Override
        public boolean hasNext() {
          return upto < numTransitions;
        }
        @Override
        public Transition next() {
          return transitionsArray[upto++];
        }
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
  
  /**
   * Returns the set of outgoing transitions. Subsequent changes are reflected
   * in the automaton.
   * 
   * @return transition set
   */
  public Iterable<Transition> getTransitions() {
    return new TransitionsIterable();
  }

  public int numTransitions() {
    return numTransitions;
  }

  public void setTransitions(Transition[] transitions) {
    this.numTransitions = transitions.length;
    this.transitionsArray = transitions;
  }
  
  /**
   * Adds an outgoing transition.
   * 
   * @param t transition
   */
  public void addTransition(Transition t) {
    if (numTransitions == transitionsArray.length) {
      final Transition[] newArray = new Transition[ArrayUtil.oversize(1+numTransitions, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(transitionsArray, 0, newArray, 0, numTransitions);
      transitionsArray = newArray;
    }
    transitionsArray[numTransitions++] = t;
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
   * @param c codepoint to look up
   * @return destination state, null if no matching outgoing transition
   * @see #step(int, Collection)
   */
  public State step(int c) {
    assert c >= 0;
    for (int i=0;i<numTransitions;i++) {
      final Transition t = transitionsArray[i];
      if (t.min <= c && c <= t.max) return t.to;
    }
    return null;
  }
  
  /**
   * Performs lookup in transitions, allowing nondeterminism.
   * 
   * @param c codepoint to look up
   * @param dest collection where destination states are stored
   * @see #step(int)
   */
  public void step(int c, Collection<State> dest) {
    for (int i=0;i<numTransitions;i++) {
      final Transition t = transitionsArray[i];
      if (t.min <= c && c <= t.max) dest.add(t.to);
    }
  }
  
  /** Virtually adds an epsilon transition to the target
   *  {@code to} state.  This is implemented by copying all
   *  transitions from {@code to} to this state, and if {@code
   *  to} is an accept state then set accept for this state. */
  void addEpsilon(State to) {
    if (to.accept) accept = true;
    for (Transition t : to.getTransitions())
      addTransition(t);
  }

  /** Downsizes transitionArray to numTransitions */
  public void trimTransitionsArray() {
    if (numTransitions < transitionsArray.length) {
      final Transition[] newArray = new Transition[numTransitions];
      System.arraycopy(transitionsArray, 0, newArray, 0, numTransitions);
      transitionsArray = newArray;
    }
  }
  
  /**
   * Reduces this state. A state is "reduced" by combining overlapping
   * and adjacent edge intervals with same destination.
   */
  public void reduce() {
    if (numTransitions <= 1) {
      return;
    }
    sortTransitions(Transition.CompareByDestThenMinMax);
    State p = null;
    int min = -1, max = -1;
    int upto = 0;
    for (int i=0;i<numTransitions;i++) {
      final Transition t = transitionsArray[i];
      if (p == t.to) {
        if (t.min <= max + 1) {
          if (t.max > max) max = t.max;
        } else {
          if (p != null) {
            transitionsArray[upto++] = new Transition(min, max, p);
          }
          min = t.min;
          max = t.max;
        }
      } else {
        if (p != null) {
          transitionsArray[upto++] = new Transition(min, max, p);
        }
        p = t.to;
        min = t.min;
        max = t.max;
      }
    }

    if (p != null) {
      transitionsArray[upto++] = new Transition(min, max, p);
    }
    numTransitions = upto;
  }

  /**
   * Returns sorted list of outgoing transitions.
   * 
   * @param to_first if true, order by (to, min, reverse max); otherwise (min,
   *          reverse max, to)
   * @return transition list
   */
  
  /** Sorts transitions array in-place. */
  public void sortTransitions(Comparator<Transition> comparator) {
    // mergesort seems to perform better on already sorted arrays:
    if (numTransitions > 1) ArrayUtil.mergeSort(transitionsArray, 0, numTransitions, comparator);
  }
  
  /**
   * Return this state's number. 
   * <p>
   * Expert: Will be useless unless {@link Automaton#getNumberedStates}
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
    for (Transition t : getTransitions())
      b.append("  ").append(t.toString()).append("\n");
    return b.toString();
  }
  
  /**
   * Compares this object with the specified object for order. States are
   * ordered by the time of construction.
   */
  @Override
  public int compareTo(State s) {
    return s.id - id;
  }

  @Override
  public int hashCode() {
    return id;
  }  
}
