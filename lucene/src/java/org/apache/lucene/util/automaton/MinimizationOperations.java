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
import java.util.LinkedList;

/**
 * Operations for minimizing automata.
 * 
 * @lucene.experimental
 */
final public class MinimizationOperations {
  
  private MinimizationOperations() {}

  /**
   * Minimizes (and determinizes if not already deterministic) the given
   * automaton.
   * 
   * @see Automaton#setMinimization(int)
   */
  public static void minimize(Automaton a) {
    if (!a.isSingleton()) {
      minimizeHopcroft(a);
    }
    // recompute hash code
    //a.hash_code = 1a.getNumberOfStates() * 3 + a.getNumberOfTransitions() * 2;
    //if (a.hash_code == 0) a.hash_code = 1;
  }
  
  /**
   * Minimizes the given automaton using Hopcroft's algorithm.
   */
  public static void minimizeHopcroft(Automaton a) {
    a.determinize();
    if (a.initial.numTransitions == 1) {
      Transition t = a.initial.transitionsArray[0];
      if (t.to == a.initial && t.min == Character.MIN_CODE_POINT
          && t.max == Character.MAX_CODE_POINT) return;
    }
    a.totalize();

    // initialize data structures
    final int[] sigma = a.getStartPoints();
    final State[] states = a.getNumberedStates();
    final int sigmaLen = sigma.length, statesLen = states.length;
    @SuppressWarnings("unchecked") final LinkedList<State>[][] reverse =
      (LinkedList<State>[][]) new LinkedList[statesLen][sigmaLen];
    @SuppressWarnings("unchecked") final LinkedList<State>[] partition =
      (LinkedList<State>[]) new LinkedList[statesLen];
    @SuppressWarnings("unchecked") final ArrayList<State>[] splitblock =
      (ArrayList<State>[]) new ArrayList[statesLen];
    final int[] block = new int[statesLen];
    final StateList[][] active = new StateList[statesLen][sigmaLen];
    final StateListNode[][] active2 = new StateListNode[statesLen][sigmaLen];
    final LinkedList<IntPair> pending = new LinkedList<IntPair>();
    final boolean[][] pending2 = new boolean[sigmaLen][statesLen];
    final ArrayList<State> split = new ArrayList<State>();
    final boolean[] split2 = new boolean[statesLen];
    final ArrayList<Integer> refine = new ArrayList<Integer>();
    final boolean[] refine2 = new boolean[statesLen];
    for (int q = 0; q < statesLen; q++) {
      splitblock[q] = new ArrayList<State>();
      partition[q] = new LinkedList<State>();
      for (int x = 0; x < sigmaLen; x++) {
        active[q][x] = new StateList();
      }
    }
    // find initial partition and reverse edges
    for (int q = 0; q < statesLen; q++) {
      final State qq = states[q];
      final int j = qq.accept ? 0 : 1;
      partition[j].add(qq);
      block[qq.number] = j;
      for (int x = 0; x < sigmaLen; x++) {
        final LinkedList<State>[] r =
          reverse[qq.step(sigma[x]).number];
        if (r[x] == null)
          r[x] = new LinkedList<State>();
        r[x].add(qq);
      }
    }
    // initialize active sets
    for (int j = 0; j <= 1; j++)
      for (int x = 0; x < sigmaLen; x++)
        for (State qq : partition[j])
          if (reverse[qq.number][x] != null)
            active2[qq.number][x] = active[j][x].add(qq);
    // initialize pending
    for (int x = 0; x < sigmaLen; x++) {
      final int j = (active[0][x].size <= active[1][x].size) ? 0 : 1;
      pending.add(new IntPair(j, x));
      pending2[x][j] = true;
    }
    // process pending until fixed point
    int k = 2;
    while (!pending.isEmpty()) {
      IntPair ip = pending.removeFirst();
      final int p = ip.n1;
      final int x = ip.n2;
      pending2[x][p] = false;
      // find states that need to be split off their blocks
      for (StateListNode m = active[p][x].first; m != null; m = m.next) {
        final LinkedList<State> r = reverse[m.q.number][x];
        if (r != null) for (State s : r) {
          if (!split2[s.number]) {
            split2[s.number] = true;
            split.add(s);
            final int j = block[s.number];
            splitblock[j].add(s);
            if (!refine2[j]) {
              refine2[j] = true;
              refine.add(j);
            }
          }
        }
      }
      // refine blocks
      for (int j : refine) {
        if (splitblock[j].size() < partition[j].size()) {
          final LinkedList<State> b1 = partition[j];
          final LinkedList<State> b2 = partition[k];
          for (State s : splitblock[j]) {
            b1.remove(s);
            b2.add(s);
            block[s.number] = k;
            for (int c = 0; c < sigmaLen; c++) {
              final StateListNode sn = active2[s.number][c];
              if (sn != null && sn.sl == active[j][c]) {
                sn.remove();
                active2[s.number][c] = active[k][c].add(s);
              }
            }
          }
          // update pending
          for (int c = 0; c < sigmaLen; c++) {
            final int aj = active[j][c].size;
            final int ak = active[k][c].size;
            if (!pending2[c][j] && 0 < aj && aj <= ak) {
              pending2[c][j] = true;
              pending.add(new IntPair(j, c));
            } else {
              pending2[c][k] = true;
              pending.add(new IntPair(k, c));
            }
          }
          k++;
        }
        for (State s : splitblock[j])
          split2[s.number] = false;
        refine2[j] = false;
        splitblock[j].clear();
      }
      split.clear();
      refine.clear();
    }
    // make a new state for each equivalence class, set initial state
    State[] newstates = new State[k];
    for (int n = 0; n < newstates.length; n++) {
      final State s = new State();
      newstates[n] = s;
      for (State q : partition[n]) {
        if (q == a.initial) a.initial = s;
        s.accept = q.accept;
        s.number = q.number; // select representative
        q.number = n;
      }
    }
    // build transitions and set acceptance
    for (int n = 0; n < newstates.length; n++) {
      final State s = newstates[n];
      s.accept = states[s.number].accept;
      for (Transition t : states[s.number].getTransitions())
        s.addTransition(new Transition(t.min, t.max, newstates[t.to.number]));
    }
    a.clearNumberedStates();
    a.removeDeadTransitions();
  }
  
  static final class IntPair {
    
    final int n1, n2;
    
    IntPair(int n1, int n2) {
      this.n1 = n1;
      this.n2 = n2;
    }
  }
  
  static final class StateList {
    
    int size;
    
    StateListNode first, last;
    
    StateListNode add(State q) {
      return new StateListNode(q, this);
    }
  }
  
  static final class StateListNode {
    
    final State q;
    
    StateListNode next, prev;
    
    final StateList sl;
    
    StateListNode(State q, StateList sl) {
      this.q = q;
      this.sl = sl;
      if (sl.size++ == 0) sl.first = sl.last = this;
      else {
        sl.last.next = this;
        prev = sl.last;
        sl.last = this;
      }
    }
    
    void remove() {
      sl.size--;
      if (sl.first == this) sl.first = next;
      else prev.next = next;
      if (sl.last == this) sl.last = prev;
      else next.prev = prev;
    }
  }
}
