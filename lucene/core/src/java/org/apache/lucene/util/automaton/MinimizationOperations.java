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

import java.util.BitSet;
import java.util.ArrayList;
import java.util.HashSet;
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
    @SuppressWarnings({"rawtypes","unchecked"}) final ArrayList<State>[][] reverse =
      (ArrayList<State>[][]) new ArrayList[statesLen][sigmaLen];
    @SuppressWarnings({"rawtypes","unchecked"}) final HashSet<State>[] partition =
      (HashSet<State>[]) new HashSet[statesLen];
    @SuppressWarnings({"rawtypes","unchecked"}) final ArrayList<State>[] splitblock =
      (ArrayList<State>[]) new ArrayList[statesLen];
    final int[] block = new int[statesLen];
    final StateList[][] active = new StateList[statesLen][sigmaLen];
    final StateListNode[][] active2 = new StateListNode[statesLen][sigmaLen];
    final LinkedList<IntPair> pending = new LinkedList<IntPair>();
    final BitSet pending2 = new BitSet(sigmaLen*statesLen);
    final BitSet split = new BitSet(statesLen), 
      refine = new BitSet(statesLen), refine2 = new BitSet(statesLen);
    for (int q = 0; q < statesLen; q++) {
      splitblock[q] = new ArrayList<State>();
      partition[q] = new HashSet<State>();
      for (int x = 0; x < sigmaLen; x++) {
        active[q][x] = new StateList();
      }
    }
    // find initial partition and reverse edges
    for (int q = 0; q < statesLen; q++) {
      final State qq = states[q];
      final int j = qq.accept ? 0 : 1;
      partition[j].add(qq);
      block[q] = j;
      for (int x = 0; x < sigmaLen; x++) {
        final ArrayList<State>[] r =
          reverse[qq.step(sigma[x]).number];
        if (r[x] == null)
          r[x] = new ArrayList<State>();
        r[x].add(qq);
      }
    }
    // initialize active sets
    for (int j = 0; j <= 1; j++) {
      for (int x = 0; x < sigmaLen; x++) {
        for (final State qq : partition[j]) {
          if (reverse[qq.number][x] != null)
            active2[qq.number][x] = active[j][x].add(qq);
        }
      }
    }
    // initialize pending
    for (int x = 0; x < sigmaLen; x++) {
      final int j = (active[0][x].size <= active[1][x].size) ? 0 : 1;
      pending.add(new IntPair(j, x));
      pending2.set(x*statesLen + j);
    }
    // process pending until fixed point
    int k = 2;
    while (!pending.isEmpty()) {
      final IntPair ip = pending.removeFirst();
      final int p = ip.n1;
      final int x = ip.n2;
      pending2.clear(x*statesLen + p);
      // find states that need to be split off their blocks
      for (StateListNode m = active[p][x].first; m != null; m = m.next) {
        final ArrayList<State> r = reverse[m.q.number][x];
        if (r != null) for (final State s : r) {
          final int i = s.number;
          if (!split.get(i)) {
            split.set(i);
            final int j = block[i];
            splitblock[j].add(s);
            if (!refine2.get(j)) {
              refine2.set(j);
              refine.set(j);
            }
          }
        }
      }
      // refine blocks
      for (int j = refine.nextSetBit(0); j >= 0; j = refine.nextSetBit(j+1)) {
        final ArrayList<State> sb = splitblock[j];
        if (sb.size() < partition[j].size()) {
          final HashSet<State> b1 = partition[j];
          final HashSet<State> b2 = partition[k];
          for (final State s : sb) {
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
            final int aj = active[j][c].size,
              ak = active[k][c].size,
              ofs = c*statesLen;
            if (!pending2.get(ofs + j) && 0 < aj && aj <= ak) {
              pending2.set(ofs + j);
              pending.add(new IntPair(j, c));
            } else {
              pending2.set(ofs + k);
              pending.add(new IntPair(k, c));
            }
          }
          k++;
        }
        refine2.clear(j);
        for (final State s : sb)
          split.clear(s.number);
        sb.clear();
      }
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
