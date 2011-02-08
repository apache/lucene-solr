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

/**
 * Finite-state automaton with fast run operation.
 * 
 * @lucene.experimental
 */
public abstract class RunAutomaton {
  final int maxInterval;
  final int size;
  final boolean[] accept;
  final int initial;
  final int[] transitions; // delta(state,c) = transitions[state*points.length +
                     // getCharClass(c)]
  final int[] points; // char interval start points
  final int[] classmap; // map from char number to class class
  
  /**
   * Returns a string representation of this automaton.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("initial state: ").append(initial).append("\n");
    for (int i = 0; i < size; i++) {
      b.append("state " + i);
      if (accept[i]) b.append(" [accept]:\n");
      else b.append(" [reject]:\n");
      for (int j = 0; j < points.length; j++) {
        int k = transitions[i * points.length + j];
        if (k != -1) {
          int min = points[j];
          int max;
          if (j + 1 < points.length) max = (points[j + 1] - 1);
          else max = maxInterval;
          b.append(" ");
          Transition.appendCharString(min, b);
          if (min != max) {
            b.append("-");
            Transition.appendCharString(max, b);
          }
          b.append(" -> ").append(k).append("\n");
        }
      }
    }
    return b.toString();
  }
  
  /**
   * Returns number of states in automaton.
   */
  public final int getSize() {
    return size;
  }
  
  /**
   * Returns acceptance status for given state.
   */
  public final boolean isAccept(int state) {
    return accept[state];
  }
  
  /**
   * Returns initial state.
   */
  public final int getInitialState() {
    return initial;
  }
  
  /**
   * Returns array of codepoint class interval start points. The array should
   * not be modified by the caller.
   */
  public final int[] getCharIntervals() {
    return points.clone();
  }
  
  /**
   * Gets character class of given codepoint
   */
  final int getCharClass(int c) {
    return SpecialOperations.findIndex(c, points);
  }

  /**
   * Constructs a new <code>RunAutomaton</code> from a deterministic
   * <code>Automaton</code>.
   * 
   * @param a an automaton
   */
  public RunAutomaton(Automaton a, int maxInterval, boolean tableize) {
    this.maxInterval = maxInterval;
    a.determinize();
    points = a.getStartPoints();
    final State[] states = a.getNumberedStates();
    initial = a.initial.number;
    size = states.length;
    accept = new boolean[size];
    transitions = new int[size * points.length];
    for (int n = 0; n < size * points.length; n++)
      transitions[n] = -1;
    for (State s : states) {
      int n = s.number;
      accept[n] = s.accept;
      for (int c = 0; c < points.length; c++) {
        State q = s.step(points[c]);
        if (q != null) transitions[n * points.length + c] = q.number;
      }
    }
    /*
     * Set alphabet table for optimal run performance.
     */
    if (tableize) {
      classmap = new int[maxInterval + 1];
      int i = 0;
      for (int j = 0; j <= maxInterval; j++) {
        if (i + 1 < points.length && j == points[i + 1])
          i++;
        classmap[j] = i;
      }
    } else {
      classmap = null;
    }
  }
  
  /**
   * Returns the state obtained by reading the given char from the given state.
   * Returns -1 if not obtaining any such state. (If the original
   * <code>Automaton</code> had no dead states, -1 is returned here if and only
   * if a dead state is entered in an equivalent automaton with a total
   * transition function.)
   */
  public final int step(int state, int c) {
    if (classmap == null)
      return transitions[state * points.length + getCharClass(c)];
    else
      return transitions[state * points.length + classmap[c]];
  }
}
