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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.Util;

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
  static int findIndex(int c, int[] points) {
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
  public static boolean isFinite(LightAutomaton a) {
    return isFinite(new Transition(), a, 0, new BitSet(a.getNumStates()), new BitSet(a.getNumStates()));
  }
  
  /**
   * Checks whether there is a loop containing s. (This is sufficient since
   * there are never transitions to dead states.)
   */
  // TODO: not great that this is recursive... in theory a
  // large automata could exceed java's stack
  private static boolean isFinite(Transition scratch, LightAutomaton a, int state, BitSet path, BitSet visited) {
    path.set(state);
    int numTransitions = a.initTransition(state, scratch);
    for(int t=0;t<numTransitions;t++) {
      a.getTransition(state, t, scratch);
      if (path.get(scratch.dest) || (!visited.get(scratch.dest) && !isFinite(scratch, a, scratch.dest, path, visited))) {
        return false;
      }
    }
    path.clear(state);
    visited.set(state);
    return true;
  }
  
  /**
   * Returns the longest string that is a prefix of all accepted strings and
   * visits each state at most once.  The automaton must be deterministic.
   * 
   * @return common prefix
   */
  public static String getCommonPrefix(LightAutomaton a) {
    assert BasicOperations.isDeterministic(a);
    StringBuilder b = new StringBuilder();
    HashSet<Integer> visited = new HashSet<>();
    int s = 0;
    boolean done;
    Transition t = new Transition();
    do {
      done = true;
      visited.add(s);
      if (a.isAccept(s) == false && a.getNumTransitions(s) == 1) {
        a.getTransition(s, 0, t);
        if (t.min == t.max && !visited.contains(t.dest)) {
          b.appendCodePoint(t.min);
          s = t.dest;
          done = false;
        }
      }
    } while (!done);

    return b.toString();
  }
  
  public static BytesRef getCommonPrefixBytesRef(LightAutomaton a) {
    BytesRef ref = new BytesRef(10);
    HashSet<Integer> visited = new HashSet<>();
    int s = 0;
    boolean done;
    Transition t = new Transition();
    do {
      done = true;
      visited.add(s);
      if (a.isAccept(s) == false && a.getNumTransitions(s) == 1) {
        a.getTransition(s, 0, t);
        if (t.min == t.max && !visited.contains(t.dest)) {
          ref.grow(++ref.length);
          ref.bytes[ref.length - 1] = (byte) t.min;
          s = t.dest;
          done = false;
        }
      }
    } while (!done);

    return ref;
  }

  public static BytesRef getCommonSuffixBytesRef(LightAutomaton a) {
    // reverse the language of the automaton, then reverse its common prefix.
    LightAutomaton r = BasicOperations.determinize(reverse(a));
    BytesRef ref = getCommonPrefixBytesRef(r);
    reverseBytes(ref);
    return ref;
  }
  
  private static void reverseBytes(BytesRef ref) {
    if (ref.length <= 1) return;
    int num = ref.length >> 1;
    for (int i = ref.offset; i < ( ref.offset + num ); i++) {
      byte b = ref.bytes[i];
      ref.bytes[i] = ref.bytes[ref.offset * 2 + ref.length - i - 1];
      ref.bytes[ref.offset * 2 + ref.length - i - 1] = b;
    }
  }
  
  // nocommit merge Special/Basic operations

  public static LightAutomaton reverse(LightAutomaton a) {
    return reverse(a, null);
  }

  public static LightAutomaton reverse(LightAutomaton a, Set<Integer> initialStates) {

    if (a.isEmpty()) {
      return a;
    }

    int numStates = a.getNumStates();

    // Build a new automaton with all edges reversed
    LightAutomaton.Builder builder = new LightAutomaton.Builder();

    // Initial node; we'll add epsilon transitions in the end:
    builder.createState();

    for(int s=0;s<numStates;s++) {
      builder.createState();
    }

    // Old initial state becomes new accept state:
    builder.setAccept(1, true);

    Transition t = new Transition();
    for (int s=0;s<numStates;s++) {
      int numTransitions = a.getNumTransitions(s);
      a.initTransition(s, t);
      for(int i=0;i<numTransitions;i++) {
        a.getNextTransition(t);
        builder.addTransition(t.dest+1, s+1, t.min, t.max);
      }
    }

    LightAutomaton result = builder.finish();
    
    for(int s : a.getAcceptStates()) {
      result.addEpsilon(0, s+1);
      if (initialStates != null) {
        initialStates.add(s+1);
      }
    }

    result.finish();

    return result;
  }

  private static class LightPathNode {

    /** Which state the path node ends on, whose
     *  transitions we are enumerating. */
    public int state;

    /** Which state the current transition leads to. */
    public int to;

    /** Which transition we are on. */
    public int transition;

    /** Which label we are on, in the min-max range of the
     *  current Transition */
    public int label;

    private final Transition t = new Transition();

    public void resetState(LightAutomaton a, int state) {
      assert a.getNumTransitions(state) != 0;
      this.state = state;
      transition = 0;
      a.getTransition(state, 0, t);
      label = t.min;
      to = t.dest;
    }

    /** Returns next label of current transition, or
     *  advances to next transition and returns its first
     *  label, if current one is exhausted.  If there are
     *  no more transitions, returns -1. */
    public int nextLabel(LightAutomaton a) {
      if (label > t.max) {
        // We've exhaused the current transition's labels;
        // move to next transitions:
        transition++;
        if (transition >= a.getNumTransitions(state)) {
          // We're done iterating transitions leaving this state
          return -1;
        }
        a.getTransition(state, transition, t);
        label = t.min;
        to = t.dest;
      }
      return label++;
    }
  }

  private static LightPathNode getNode(LightPathNode[] nodes, int index) {
    assert index < nodes.length;
    if (nodes[index] == null) {
      nodes[index] = new LightPathNode();
    }
    return nodes[index];
  }

  // TODO: this is a dangerous method ... Automaton could be
  // huge ... and it's better in general for caller to
  // enumerate & process in a single walk:

  /** Returns the set of accepted strings, up to at most
   *  <code>limit</code> strings. If more than <code>limit</code> 
   *  strings are accepted, the first limit strings found are returned. If <code>limit</code> == -1, then 
   *  the limit is infinite.  If the {@link Automaton} has
   *  cycles then this method might throw {@code
   *  IllegalArgumentException} but that is not guaranteed
   *  when the limit is set. */
  public static Set<IntsRef> getFiniteStrings(LightAutomaton a, int limit) {
    Set<IntsRef> results = new HashSet<>();

    if (limit == -1 || limit > 0) {
      // OK
    } else {
      throw new IllegalArgumentException("limit must be -1 (which means no limit), or > 0; got: " + limit);
    }

    if (a.isAccept(0)) {
      // Special case the empty string, as usual:
      results.add(new IntsRef());
    }

    if (a.getNumTransitions(0) > 0 && (limit == -1 || results.size() < limit)) {

      int numStates = a.getNumStates();

      // Tracks which states are in the current path, for
      // cycle detection:
      BitSet pathStates = new BitSet(numStates);

      // Stack to hold our current state in the
      // recursion/iteration:
      LightPathNode[] nodes = new LightPathNode[4];

      pathStates.set(0);
      LightPathNode root = getNode(nodes, 0);
      root.resetState(a, 0);

      IntsRef string = new IntsRef(1);
      string.length = 1;

      while (string.length > 0) {

        LightPathNode node = nodes[string.length-1];

        // Get next label leaving the current node:
        int label = node.nextLabel(a);

        if (label != -1) {
          string.ints[string.length-1] = label;

          if (a.isAccept(node.to)) {
            // This transition leads to an accept state,
            // so we save the current string:
            results.add(IntsRef.deepCopyOf(string));
            if (results.size() == limit) {
              break;
            }
          }

          if (a.getNumTransitions(node.to) != 0) {
            // Now recurse: the destination of this transition has
            // outgoing transitions:
            if (pathStates.get(node.to)) {
              throw new IllegalArgumentException("automaton has cycles");
            }
            pathStates.set(node.to);

            // Push node onto stack:
            if (nodes.length == string.length) {
              LightPathNode[] newNodes = new LightPathNode[ArrayUtil.oversize(nodes.length+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
              System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
              nodes = newNodes;
            }
            getNode(nodes, string.length).resetState(a, node.to);
            string.length++;
            string.grow(string.length);
          }
        } else {
          // No more transitions leaving this state,
          // pop/return back to previous state:
          assert pathStates.get(node.state);
          pathStates.clear(node.state);
          string.length--;
        }
      }
    }

    return results;
  }
}
