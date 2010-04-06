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
import java.util.Set;

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
    a.hash_code = a.getNumberOfStates() * 3 + a.getNumberOfTransitions() * 2;
    if (a.hash_code == 0) a.hash_code = 1;
  }
  
  private static <T> void initialize(ArrayList<T> list, int size) {
    for (int i = 0; i < size; i++)
      list.add(null);
  }
  
  /**
   * Minimizes the given automaton using Hopcroft's algorithm.
   */
  public static void minimizeHopcroft(Automaton a) {
    a.determinize();
    Set<Transition> tr = a.initial.getTransitions();
    if (tr.size() == 1) {
      Transition t = tr.iterator().next();
      if (t.to == a.initial && t.min == Character.MIN_VALUE
          && t.max == Character.MAX_VALUE) return;
    }
    a.totalize();
    // make arrays for numbered states and effective alphabet
    Set<State> ss = a.getStates();
    State[] states = new State[ss.size()];
    int number = 0;
    for (State q : ss) {
      states[number] = q;
      q.number = number++;
    }
    char[] sigma = a.getStartPoints();
    // initialize data structures
    ArrayList<ArrayList<LinkedList<State>>> reverse = new ArrayList<ArrayList<LinkedList<State>>>();
    for (int q = 0; q < states.length; q++) {
      ArrayList<LinkedList<State>> v = new ArrayList<LinkedList<State>>();
      initialize(v, sigma.length);
      reverse.add(v);
    }
    boolean[][] reverse_nonempty = new boolean[states.length][sigma.length];
    ArrayList<LinkedList<State>> partition = new ArrayList<LinkedList<State>>();
    initialize(partition, states.length);
    int[] block = new int[states.length];
    StateList[][] active = new StateList[states.length][sigma.length];
    StateListNode[][] active2 = new StateListNode[states.length][sigma.length];
    LinkedList<IntPair> pending = new LinkedList<IntPair>();
    boolean[][] pending2 = new boolean[sigma.length][states.length];
    ArrayList<State> split = new ArrayList<State>();
    boolean[] split2 = new boolean[states.length];
    ArrayList<Integer> refine = new ArrayList<Integer>();
    boolean[] refine2 = new boolean[states.length];
    ArrayList<ArrayList<State>> splitblock = new ArrayList<ArrayList<State>>();
    initialize(splitblock, states.length);
    for (int q = 0; q < states.length; q++) {
      splitblock.set(q, new ArrayList<State>());
      partition.set(q, new LinkedList<State>());
      for (int x = 0; x < sigma.length; x++) {
        reverse.get(q).set(x, new LinkedList<State>());
        active[q][x] = new StateList();
      }
    }
    // find initial partition and reverse edges
    for (int q = 0; q < states.length; q++) {
      State qq = states[q];
      int j;
      if (qq.accept) j = 0;
      else j = 1;
      partition.get(j).add(qq);
      block[qq.number] = j;
      for (int x = 0; x < sigma.length; x++) {
        char y = sigma[x];
        State p = qq.step(y);
        reverse.get(p.number).get(x).add(qq);
        reverse_nonempty[p.number][x] = true;
      }
    }
    // initialize active sets
    for (int j = 0; j <= 1; j++)
      for (int x = 0; x < sigma.length; x++)
        for (State qq : partition.get(j))
          if (reverse_nonempty[qq.number][x]) active2[qq.number][x] = active[j][x]
              .add(qq);
    // initialize pending
    for (int x = 0; x < sigma.length; x++) {
      int a0 = active[0][x].size;
      int a1 = active[1][x].size;
      int j;
      if (a0 <= a1) j = 0;
      else j = 1;
      pending.add(new IntPair(j, x));
      pending2[x][j] = true;
    }
    // process pending until fixed point
    int k = 2;
    while (!pending.isEmpty()) {
      IntPair ip = pending.removeFirst();
      int p = ip.n1;
      int x = ip.n2;
      pending2[x][p] = false;
      // find states that need to be split off their blocks
      for (StateListNode m = active[p][x].first; m != null; m = m.next)
        for (State s : reverse.get(m.q.number).get(x))
          if (!split2[s.number]) {
            split2[s.number] = true;
            split.add(s);
            int j = block[s.number];
            splitblock.get(j).add(s);
            if (!refine2[j]) {
              refine2[j] = true;
              refine.add(j);
            }
          }
      // refine blocks
      for (int j : refine) {
        if (splitblock.get(j).size() < partition.get(j).size()) {
          LinkedList<State> b1 = partition.get(j);
          LinkedList<State> b2 = partition.get(k);
          for (State s : splitblock.get(j)) {
            b1.remove(s);
            b2.add(s);
            block[s.number] = k;
            for (int c = 0; c < sigma.length; c++) {
              StateListNode sn = active2[s.number][c];
              if (sn != null && sn.sl == active[j][c]) {
                sn.remove();
                active2[s.number][c] = active[k][c].add(s);
              }
            }
          }
          // update pending
          for (int c = 0; c < sigma.length; c++) {
            int aj = active[j][c].size;
            int ak = active[k][c].size;
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
        for (State s : splitblock.get(j))
          split2[s.number] = false;
        refine2[j] = false;
        splitblock.get(j).clear();
      }
      split.clear();
      refine.clear();
    }
    // make a new state for each equivalence class, set initial state
    State[] newstates = new State[k];
    for (int n = 0; n < newstates.length; n++) {
      State s = new State();
      newstates[n] = s;
      for (State q : partition.get(n)) {
        if (q == a.initial) a.initial = s;
        s.accept = q.accept;
        s.number = q.number; // select representative
        q.number = n;
      }
    }
    // build transitions and set acceptance
    for (int n = 0; n < newstates.length; n++) {
      State s = newstates[n];
      s.accept = states[s.number].accept;
      for (Transition t : states[s.number].transitions)
        s.transitions.add(new Transition(t.min, t.max, newstates[t.to.number]));
    }
    a.removeDeadTransitions();
  }
  
  static class IntPair {
    
    int n1, n2;
    
    IntPair(int n1, int n2) {
      this.n1 = n1;
      this.n2 = n2;
    }
  }
  
  static class StateList {
    
    int size;
    
    StateListNode first, last;
    
    StateListNode add(State q) {
      return new StateListNode(q, this);
    }
  }
  
  static class StateListNode {
    
    State q;
    
    StateListNode next, prev;
    
    StateList sl;
    
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
