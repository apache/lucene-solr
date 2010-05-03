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

import java.util.ArrayList;
import java.util.Collection;

/**
 * Construction of basic automata.
 * 
 * @lucene.experimental
 */
final public class BasicAutomata {
  
  private BasicAutomata() {}
  
  /**
   * Returns a new (deterministic) automaton with the empty language.
   */
  public static Automaton makeEmpty() {
    Automaton a = new Automaton();
    State s = new State();
    a.initial = s;
    a.deterministic = true;
    return a;
  }
  
  /**
   * Returns a new (deterministic) automaton that accepts only the empty string.
   */
  public static Automaton makeEmptyString() {
    Automaton a = new Automaton();
    a.singleton = "";
    a.deterministic = true;
    return a;
  }
  
  /**
   * Returns a new (deterministic) automaton that accepts all strings.
   */
  public static Automaton makeAnyString() {
    Automaton a = new Automaton();
    State s = new State();
    a.initial = s;
    s.accept = true;
    s.addTransition(new Transition(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT,
        s));
    a.deterministic = true;
    return a;
  }
  
  /**
   * Returns a new (deterministic) automaton that accepts any single codepoint.
   */
  public static Automaton makeAnyChar() {
    return makeCharRange(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
  }
  
  /**
   * Returns a new (deterministic) automaton that accepts a single codepoint of
   * the given value.
   */
  public static Automaton makeChar(int c) {
    Automaton a = new Automaton();
    a.singleton = new String(Character.toChars(c));
    a.deterministic = true;
    return a;
  }
  
  /**
   * Returns a new (deterministic) automaton that accepts a single codepoint whose
   * value is in the given interval (including both end points).
   */
  public static Automaton makeCharRange(int min, int max) {
    if (min == max) return makeChar(min);
    Automaton a = new Automaton();
    State s1 = new State();
    State s2 = new State();
    a.initial = s1;
    s2.accept = true;
    if (min <= max) s1.addTransition(new Transition(min, max, s2));
    a.deterministic = true;
    return a;
  }
  
  /**
   * Constructs sub-automaton corresponding to decimal numbers of length
   * x.substring(n).length().
   */
  private static State anyOfRightLength(String x, int n) {
    State s = new State();
    if (x.length() == n) s.setAccept(true);
    else s.addTransition(new Transition('0', '9', anyOfRightLength(x, n + 1)));
    return s;
  }
  
  /**
   * Constructs sub-automaton corresponding to decimal numbers of value at least
   * x.substring(n) and length x.substring(n).length().
   */
  private static State atLeast(String x, int n, Collection<State> initials,
      boolean zeros) {
    State s = new State();
    if (x.length() == n) s.setAccept(true);
    else {
      if (zeros) initials.add(s);
      char c = x.charAt(n);
      s.addTransition(new Transition(c, atLeast(x, n + 1, initials, zeros
          && c == '0')));
      if (c < '9') s.addTransition(new Transition((char) (c + 1), '9',
          anyOfRightLength(x, n + 1)));
    }
    return s;
  }
  
  /**
   * Constructs sub-automaton corresponding to decimal numbers of value at most
   * x.substring(n) and length x.substring(n).length().
   */
  private static State atMost(String x, int n) {
    State s = new State();
    if (x.length() == n) s.setAccept(true);
    else {
      char c = x.charAt(n);
      s.addTransition(new Transition(c, atMost(x, (char) n + 1)));
      if (c > '0') s.addTransition(new Transition('0', (char) (c - 1),
          anyOfRightLength(x, n + 1)));
    }
    return s;
  }
  
  /**
   * Constructs sub-automaton corresponding to decimal numbers of value between
   * x.substring(n) and y.substring(n) and of length x.substring(n).length()
   * (which must be equal to y.substring(n).length()).
   */
  private static State between(String x, String y, int n,
      Collection<State> initials, boolean zeros) {
    State s = new State();
    if (x.length() == n) s.setAccept(true);
    else {
      if (zeros) initials.add(s);
      char cx = x.charAt(n);
      char cy = y.charAt(n);
      if (cx == cy) s.addTransition(new Transition(cx, between(x, y, n + 1,
          initials, zeros && cx == '0')));
      else { // cx<cy
        s.addTransition(new Transition(cx, atLeast(x, n + 1, initials, zeros
            && cx == '0')));
        s.addTransition(new Transition(cy, atMost(y, n + 1)));
        if (cx + 1 < cy) s.addTransition(new Transition((char) (cx + 1),
            (char) (cy - 1), anyOfRightLength(x, n + 1)));
      }
    }
    return s;
  }
  
  /**
   * Returns a new automaton that accepts strings representing decimal
   * non-negative integers in the given interval.
   * 
   * @param min minimal value of interval
   * @param max maximal value of interval (both end points are included in the
   *          interval)
   * @param digits if >0, use fixed number of digits (strings must be prefixed
   *          by 0's to obtain the right length) - otherwise, the number of
   *          digits is not fixed
   * @exception IllegalArgumentException if min>max or if numbers in the
   *              interval cannot be expressed with the given fixed number of
   *              digits
   */
  public static Automaton makeInterval(int min, int max, int digits)
      throws IllegalArgumentException {
    Automaton a = new Automaton();
    String x = Integer.toString(min);
    String y = Integer.toString(max);
    if (min > max || (digits > 0 && y.length() > digits)) throw new IllegalArgumentException();
    int d;
    if (digits > 0) d = digits;
    else d = y.length();
    StringBuilder bx = new StringBuilder();
    for (int i = x.length(); i < d; i++)
      bx.append('0');
    bx.append(x);
    x = bx.toString();
    StringBuilder by = new StringBuilder();
    for (int i = y.length(); i < d; i++)
      by.append('0');
    by.append(y);
    y = by.toString();
    Collection<State> initials = new ArrayList<State>();
    a.initial = between(x, y, 0, initials, digits <= 0);
    if (digits <= 0) {
      ArrayList<StatePair> pairs = new ArrayList<StatePair>();
      for (State p : initials)
        if (a.initial != p) pairs.add(new StatePair(a.initial, p));
      BasicOperations.addEpsilons(a, pairs);
      a.initial.addTransition(new Transition('0', a.initial));
      a.deterministic = false;
    } else a.deterministic = true;
    a.checkMinimizeAlways();
    return a;
  }
  
  /**
   * Returns a new (deterministic) automaton that accepts the single given
   * string.
   */
  public static Automaton makeString(String s) {
    Automaton a = new Automaton();
    a.singleton = s;
    a.deterministic = true;
    return a;
  }
}
