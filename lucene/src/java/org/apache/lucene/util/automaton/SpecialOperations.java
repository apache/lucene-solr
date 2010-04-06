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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Special automata operations.
 * 
 * @lucene.experimental
 */
final public class SpecialOperations {
  
  private SpecialOperations() {}
  
  /**
   * Finds the largest entry whose value is less than or equal to c, or 0 if
   * there is no such entry.
   */
  static int findIndex(char c, char[] points) {
    int a = 0;
    int b = points.length;
    while (b - a > 1) {
      int d = (a + b) >>> 1;
      if (points[d] > c) b = d;
      else if (points[d] < c) a = d;
      else return d;
    }
    return a;
  }
  
  /**
   * Returns true if the language of this automaton is finite.
   */
  public static boolean isFinite(Automaton a) {
    if (a.isSingleton()) return true;
    return isFinite(a.initial, new HashSet<State>());
  }
  
  /**
   * Checks whether there is a loop containing s. (This is sufficient since
   * there are never transitions to dead states.)
   */
  private static boolean isFinite(State s, HashSet<State> path) {
    path.add(s);
    for (Transition t : s.transitions)
      if (path.contains(t.to) || !isFinite(t.to, path)) return false;
    path.remove(s);
    return true;
  }
  
  /**
   * Returns the longest string that is a prefix of all accepted strings and
   * visits each state at most once.
   * 
   * @return common prefix
   */
  public static String getCommonPrefix(Automaton a) {
    if (a.isSingleton()) return a.singleton;
    StringBuilder b = new StringBuilder();
    HashSet<State> visited = new HashSet<State>();
    State s = a.initial;
    boolean done;
    do {
      done = true;
      visited.add(s);
      if (!s.accept && s.transitions.size() == 1) {
        Transition t = s.transitions.iterator().next();
        if (t.min == t.max && !visited.contains(t.to)) {
          b.append(t.min);
          s = t.to;
          done = false;
        }
      }
    } while (!done);
    return b.toString();
  }
  
  /**
   * Returns the longest string that is a suffix of all accepted strings and
   * visits each state at most once.
   * 
   * @return common suffix
   */
  public static String getCommonSuffix(Automaton a) {
    if (a.isSingleton()) // if singleton, the suffix is the string itself.
      return a.singleton;
    
    // reverse the language of the automaton, then reverse its common prefix.
    Automaton r = a.clone();
    reverse(r);
    r.determinize();
    return reverseUnicode3(SpecialOperations.getCommonPrefix(r));
  }
  
  /**
   * Reverses the language of the given (non-singleton) automaton while returning
   * the set of new initial states.
   */
  private static Set<State> reverse(Automaton a) {
    a.expandSingleton();
    // reverse all edges
    HashMap<State, HashSet<Transition>> m = new HashMap<State, HashSet<Transition>>();
    Set<State> states = a.getStates();
    Set<State> accept = a.getAcceptStates();
    for (State r : states) {
      m.put(r, new HashSet<Transition>());
      r.accept = false;
    }
    for (State r : states)
      for (Transition t : r.getTransitions())
        m.get(t.to).add(new Transition(t.min, t.max, r));
    for (State r : states)
      r.transitions = m.get(r);
    // make new initial+final states
    a.initial.accept = true;
    a.initial = new State();
    for (State r : accept)
      a.initial.addEpsilon(r); // ensures that all initial states are reachable
    a.deterministic = false;
    return accept;
  }
  
  /**
   * Intentionally use a unicode 3 reverse.
   * This is because we are only going to reverse it again...
   */
  private static String reverseUnicode3( final String input ){
    char[] charInput = input.toCharArray();
    reverseUnicode3(charInput, 0, charInput.length);
    return new String(charInput);
  }
  
  /**
   * Intentionally use a unicode 3 reverse.
   * This is because it is only used by getCommonSuffix(),
   * which will reverse the entire FSM using code unit reversal,
   * so we must then reverse its common prefix back using the 
   * same code unit reversal.
   */
  private static void reverseUnicode3(char[] buffer, int start, int len){
    if (len <= 1) return;
    int num = len>>1;
    for (int i = start; i < ( start + num ); i++) {
      char c = buffer[i];
      buffer[i] = buffer[start * 2 + len - i - 1];
      buffer[start * 2 + len - i - 1] = c;
    }
  }
}
