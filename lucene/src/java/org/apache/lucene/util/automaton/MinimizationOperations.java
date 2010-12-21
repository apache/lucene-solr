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
    final BitSet[][] reverse = new BitSet[statesLen][sigmaLen];
    final BitSet[] splitblock = new BitSet[statesLen], partition = new BitSet[statesLen];
    final int[] block = new int[statesLen];
    final StateList[][] active = new StateList[statesLen][sigmaLen];
    final StateListNode[][] active2 = new StateListNode[statesLen][sigmaLen];
    final LinkedList<IntPair> pending = new LinkedList<IntPair>();
    final BitSet pending2 = new BitSet(sigmaLen*statesLen);
    final BitSet split = new BitSet(statesLen), 
      refine = new BitSet(statesLen), refine2 = new BitSet(statesLen);
    for (int q = 0; q < statesLen; q++) {
      splitblock[q] = new BitSet(statesLen);
      partition[q] = new BitSet(statesLen);
      for (int x = 0; x < sigmaLen; x++) {
        active[q][x] = new StateList();
      }
    }
    // find initial partition and reverse edges
    for (int q = 0; q < statesLen; q++) {
      final State qq = states[q];
      final int j = qq.accept ? 0 : 1;
      partition[j].set(q);
      block[q] = j;
      for (int x = 0; x < sigmaLen; x++) {
        final BitSet[] r =
          reverse[qq.step(sigma[x]).number];
        if (r[x] == null)
          r[x] = new BitSet();
        r[x].set(q);
      }
    }
    // initialize active sets
    for (int j = 0; j <= 1; j++) {
      final BitSet part = partition[j];
      for (int x = 0; x < sigmaLen; x++) {
        for (int i = part.nextSetBit(0); i >= 0; i = part.nextSetBit(i+1)) {
          if (reverse[i][x] != null)
            active2[i][x] = active[j][x].add(states[i]);
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
      IntPair ip = pending.removeFirst();
      final int p = ip.n1;
      final int x = ip.n2;
      pending2.clear(x*statesLen + p);
      // find states that need to be split off their blocks
      for (StateListNode m = active[p][x].first; m != null; m = m.next) {
        final BitSet r = reverse[m.q.number][x];
        if (r != null) for (int i = r.nextSetBit(0); i >= 0; i = r.nextSetBit(i+1)) {
          if (!split.get(i)) {
            split.set(i);
            final int j = block[i];
            splitblock[j].set(i);
            if (!refine2.get(j)) {
              refine2.set(j);
              refine.set(j);
            }
          }
        }
      }
      // refine blocks
      for (int j = refine.nextSetBit(0); j >= 0; j = refine.nextSetBit(j+1)) {
        final BitSet sb = splitblock[j];
        if (sb.cardinality() < partition[j].cardinality()) {
          final BitSet b1 = partition[j], b2 = partition[k];
          for (int i = sb.nextSetBit(0); i >= 0; i = sb.nextSetBit(i+1)) {
            b1.clear(i);
            b2.set(i);
            block[i] = k;
            for (int c = 0; c < sigmaLen; c++) {
              final StateListNode sn = active2[i][c];
              if (sn != null && sn.sl == active[j][c]) {
                sn.remove();
                active2[i][c] = active[k][c].add(states[i]);
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
        for (int i = sb.nextSetBit(0); i >= 0; i = sb.nextSetBit(i+1))
          split.clear(i);
        sb.clear();
      }
      refine.clear();
    }
    // make a new state for each equivalence class, set initial state
    State[] newstates = new State[k];
    for (int n = 0; n < newstates.length; n++) {
      final State s = new State();
      newstates[n] = s;
      BitSet part = partition[n];
      for (int i = part.nextSetBit(0); i >= 0; i = part.nextSetBit(i+1)) {
        final State q = states[i];
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
