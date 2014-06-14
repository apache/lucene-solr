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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Basic automata operations.
 * 
 * @lucene.experimental
 */
final public class BasicOperations {
  
  private BasicOperations() {}

  /**
   * Returns an automaton that accepts the concatenation of the languages of the
   * given automata.
   * <p>
   * Complexity: linear in total number of states.
   */
  static public LightAutomaton concatenateLight(LightAutomaton a1, LightAutomaton a2) {
    // nocommit we lost the two-arg optimization here (prepend tiny automaton in front of huge one)
    return concatenateLight(Arrays.asList(a1, a2));
  }

  /**
   * Returns an automaton that accepts the concatenation of the languages of the
   * given automata.
   * <p>
   * Complexity: linear in total number of states.
   */
  static public LightAutomaton concatenateLight(List<LightAutomaton> l) {
    LightAutomaton result = new LightAutomaton();

    // First pass: create all states
    for(LightAutomaton a : l) {
      int numStates = a.getNumStates();
      for(int s=0;s<numStates;s++) {
        result.createState();
      }
    }

    // Second pass: add transitions, carefully linking accept
    // states of A to init state of next A:
    int stateOffset = 0;
    Transition t = new Transition();
    for(int i=0;i<l.size();i++) {
      LightAutomaton a = l.get(i);
      int numStates = a.getNumStates();

      LightAutomaton nextA = (i == l.size()-1) ? null : l.get(i+1);

      for(int s=0;s<numStates;s++) {
        int numTransitions = a.initTransition(s, t);
        for(int j=0;j<numTransitions;j++) {
          a.getNextTransition(t);
          result.addTransition(stateOffset + s, stateOffset + t.dest, t.min, t.max);
        }

        if (a.isAccept(s)) {
          LightAutomaton followA = nextA;
          int followOffset = stateOffset;
          int upto = i+1;
          while (true) {
            if (followA != null) {
              // Adds a "virtual" epsilon transition:
              numTransitions = followA.initTransition(0, t);
              for(int j=0;j<numTransitions;j++) {
                followA.getNextTransition(t);
                result.addTransition(stateOffset + s, followOffset + numStates + t.dest, t.min, t.max);
              }
              if (followA.isAccept(0)) {
                // Keep chaining if followA accepts empty string
                followOffset += followA.getNumStates();
                followA = (upto == l.size()-1) ? null : l.get(upto+1);
                upto++;
              } else {
                break;
              }
            } else {
              result.setAccept(stateOffset + s, true);
              break;
            }
          }
        }
      }

      stateOffset += numStates;
    }

    if (result.getNumStates() == 0) {
      result.createState();
    }

    result.finish();

    return result;
  }

  /**
   * Returns an automaton that accepts the union of the empty string and the
   * language of the given automaton.
   * <p>
   * Complexity: linear in number of states.
   */
  static public LightAutomaton optionalLight(LightAutomaton a) {
    LightAutomaton result = new LightAutomaton();
    result.createState();
    result.setAccept(0, true);
    int numStates = a.getNumStates();
    for(int i=0;i<numStates;i++) {
      result.createState();
      result.setAccept(i+1, a.isAccept(i));
    }

    Transition t = new Transition();
    int count = a.initTransition(0, t);
    for(int i=0;i<count;i++) {
      a.getNextTransition(t);
      result.addTransition(0, 1+t.dest, t.min, t.max);
    }

    for(int i=0;i<numStates;i++) {
      count = a.initTransition(i, t);
      for(int j=0;j<count;j++) {
        a.getNextTransition(t);
        result.addTransition(1+t.source, 1+t.dest, t.min, t.max);
      }
    }

    result.finish();
    return result;
  }
  
  /**
   * Returns an automaton that accepts the Kleene star (zero or more
   * concatenated repetitions) of the language of the given automaton. Never
   * modifies the input automaton language.
   * <p>
   * Complexity: linear in number of states.
   */
  static public LightAutomaton repeatLight(LightAutomaton a) {
    LightAutomaton.Builder builder = new LightAutomaton.Builder();
    builder.createState();
    builder.setAccept(0, true);
    builder.copy(a);

    Transition t = new Transition();
    int count = a.initTransition(0, t);
    for(int i=0;i<count;i++) {
      a.getNextTransition(t);
      builder.addTransition(0, t.dest+1, t.min, t.max);
    }

    int numStates = a.getNumStates();
    for(int s=0;s<numStates;s++) {
      if (a.isAccept(s)) {
        count = a.initTransition(0, t);
        for(int i=0;i<count;i++) {
          a.getNextTransition(t);
          builder.addTransition(s+1, t.dest+1, t.min, t.max);
        }
      }
    }

    return builder.finish();
  }

  // nocommit move to AutomatonTestUtil

  /**
   * Returns an automaton that accepts <code>min</code> or more concatenated
   * repetitions of the language of the given automaton.
   * <p>
   * Complexity: linear in number of states and in <code>min</code>.
   */
  static public LightAutomaton repeatLight(LightAutomaton a, int min) {
    if (min == 0) {
      return repeatLight(a);
    }
    List<LightAutomaton> as = new ArrayList<>();
    while (min-- > 0) {
      as.add(a);
    }
    as.add(repeatLight(a));
    return concatenateLight(as);
  }
  
  /**
   * Returns an automaton that accepts between <code>min</code> and
   * <code>max</code> (including both) concatenated repetitions of the language
   * of the given automaton.
   * <p>
   * Complexity: linear in number of states and in <code>min</code> and
   * <code>max</code>.
   */
  static public LightAutomaton repeatLight(LightAutomaton a, int min, int max) {
    if (min > max) {
      return BasicAutomata.makeEmptyLight();
    }

    LightAutomaton b;
    if (min == 0) {
      b = BasicAutomata.makeEmptyStringLight();
    } else if (min == 1) {
      b = new LightAutomaton();
      b.copy(a);
    } else {
      List<LightAutomaton> as = new ArrayList<>();
      for(int i=0;i<min;i++) {
        as.add(a);
      }
      b = concatenateLight(as);
    }

    Set<Integer> prevAcceptStates = new HashSet<>(b.getAcceptStates());

    for(int i=min;i<max;i++) {
      int numStates = b.getNumStates();
      b.copy(a);
      for(int s : prevAcceptStates) {
        b.addEpsilon(s, numStates);
      }
      prevAcceptStates.clear();
      for(int s : a.getAcceptStates()) {
        prevAcceptStates.add(numStates+s);
      }
    }

    b.finish();

    return b;
  }
  
  /**
   * Returns a (deterministic) automaton that accepts the complement of the
   * language of the given automaton.
   * <p>
   * Complexity: linear in number of states (if already deterministic).
   */
  static public LightAutomaton complementLight(LightAutomaton a) {
    a = determinize(a).totalize();
    int numStates = a.getNumStates();
    for (int p=0;p<numStates;p++) {
      a.setAccept(p, !a.isAccept(p));
    }
    return removeDeadStates(a);
  }
  
  /**
   * Returns a (deterministic) automaton that accepts the intersection of the
   * language of <code>a1</code> and the complement of the language of
   * <code>a2</code>. As a side-effect, the automata may be determinized, if not
   * already deterministic.
   * <p>
   * Complexity: quadratic in number of states (if already deterministic).
   */
  static public LightAutomaton minusLight(LightAutomaton a1, LightAutomaton a2) {
    if (BasicOperations.isEmpty(a1) || a1 == a2) {
      return BasicAutomata.makeEmptyLight();
    }
    if (BasicOperations.isEmpty(a2)) {
      return a1;
    }
    return intersectionLight(a1, complementLight(a2));
  }
  
  /**
   * Returns an automaton that accepts the intersection of the languages of the
   * given automata. Never modifies the input automata languages.
   * <p>
   * Complexity: quadratic in number of states.
   */
  static public LightAutomaton intersectionLight(LightAutomaton a1, LightAutomaton a2) {
    if (a1 == a2) {
      return a1;
    }
    if (a1.getNumStates() == 0) {
      return a1;
    }
    if (a2.getNumStates() == 0) {
      return a2;
    }
    Transition[][] transitions1 = a1.getSortedTransitions();
    Transition[][] transitions2 = a2.getSortedTransitions();
    LightAutomaton c = new LightAutomaton();
    c.createState();
    LinkedList<LightStatePair> worklist = new LinkedList<>();
    HashMap<LightStatePair,LightStatePair> newstates = new HashMap<>();
    LightStatePair p = new LightStatePair(0, 0, 0);
    worklist.add(p);
    newstates.put(p, p);
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      c.setAccept(p.s, a1.isAccept(p.s1) && a2.isAccept(p.s2));
      Transition[] t1 = transitions1[p.s1];
      Transition[] t2 = transitions2[p.s2];
      for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
        while (b2 < t2.length && t2[b2].max < t1[n1].min)
          b2++;
        for (int n2 = b2; n2 < t2.length && t1[n1].max >= t2[n2].min; n2++)
          if (t2[n2].max >= t1[n1].min) {
            LightStatePair q = new LightStatePair(t1[n1].dest, t2[n2].dest);
            LightStatePair r = newstates.get(q);
            if (r == null) {
              q.s = c.createState();
              worklist.add(q);
              newstates.put(q, q);
              r = q;
            }
            int min = t1[n1].min > t2[n2].min ? t1[n1].min : t2[n2].min;
            int max = t1[n1].max < t2[n2].max ? t1[n1].max : t2[n2].max;
            c.addTransition(p.s, r.s, min, max);
          }
      }
    }
    c.finish();

    return removeDeadStates(c);
  }

  /**
   * Returns an automaton that accepts the intersection of the languages of the
   * given automata. Never modifies the input automata languages.
   * <p>
   * Complexity: quadratic in number of states.
   */
  /*
  // nocommit broken
  static public LightAutomaton intersectionLight(LightAutomaton a1, LightAutomaton a2) {
    if (a1 == a2) {
      return a1;
    }
    LightAutomaton result = new LightAutomaton();
    result.createState();
    //Transition[][] transitions1 = a1.getSortedTransitions();
    //Transition[][] transitions2 = a2.getSortedTransitions();
    LinkedList<LightStatePair> worklist = new LinkedList<>();
    HashMap<LightStatePair,LightStatePair> newstates = new HashMap<>();
    LightStatePair p = new LightStatePair(0, 0, 0);
    worklist.add(p);
    newstates.put(p, p);
    LightAutomaton.Transition t1 = new LightAutomaton.Transition();
    LightAutomaton.Transition t2 = new LightAutomaton.Transition();
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      result.setAccept(p.s, a1.isAccept(p.s1) && a2.isAccept(p.s2));
      int numT1 = a1.initTransition(p.s1, t1);
      if (numT1 > 0) {
        a1.getNextTransition(t1);
      }
      int numT2 = a2.initTransition(p.s2, t2);
      if (numT2 > 0) {
        a2.getNextTransition(t2);
      }
      //Transition[] t1 = transitions1[p.s1.number];
      //Transition[] t2 = transitions2[p.s2.number];
      for (int n1 = 0, b2 = 0; n1 < numT1; n1++) {
        while (b2 < numT2 && t2.max < t1.min) {
          b2++;
          if (b2 < numT2) {
            a2.getNextTransition(t2);
          }
        }
        for (int n2 = b2; n2 < numT2 && t1.max >= t2.min; n2++) {
          if (t2.max >= t1.min) {
            LightStatePair q = new LightStatePair(t1.dest, t2.dest);
            LightStatePair r = newstates.get(q);
            if (r == null) {
              q.s = result.createState();
              worklist.add(q);
              newstates.put(q, q);
              r = q;
            }
            int min = t1.min > t2.min ? t1.min : t2.min;
            int max = t1.max < t2.max ? t1.max : t2.max;
            result.addTransition(p.s, r.s, min, max);
          }
          if (n2 < numT2-1) {
            a2.getNextTransition(t2);
          }
        }
      }
    }

    result.finish();

    return result.removeDeadTransitions();
  }
  */

  /** Returns true if these two automata accept exactly the
   *  same language.  This is a costly computation!  Note
   *  also that a1 and a2 will be determinized as a side
   *  effect.  Both automata must be determinized first! */
  public static boolean sameLanguage(LightAutomaton a1, LightAutomaton a2) {
    if (a1 == a2) {
      return true;
    }
    if (a1.isEmpty() && a2.isEmpty()) {
      return true;
    }

    return subsetOf(a2, a1) && subsetOf(a1, a2);
  }

  /**
   * Returns true if the language of <code>a1</code> is a subset of the language
   * of <code>a2</code>. Both automata must be determinized.
   * <p>
   * Complexity: quadratic in number of states.
   */
  public static boolean subsetOf(LightAutomaton a1, LightAutomaton a2) {
    if (a1.isDeterministic() == false) {
      throw new IllegalArgumentException("a1 must be deterministic");
    }
    if (a2.isDeterministic() == false) {
      throw new IllegalArgumentException("a2 must be deterministic");
    }
    // TODO: cutover to iterators instead
    Transition[][] transitions1 = a1.getSortedTransitions();
    Transition[][] transitions2 = a2.getSortedTransitions();
    LinkedList<LightStatePair> worklist = new LinkedList<>();
    HashSet<LightStatePair> visited = new HashSet<>();
    LightStatePair p = new LightStatePair(0, 0);
    worklist.add(p);
    visited.add(p);
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      if (a1.isAccept(p.s1) && a2.isAccept(p.s2) == false) {
        return false;
      }
      Transition[] t1 = transitions1[p.s1];
      Transition[] t2 = transitions2[p.s2];
      for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
        while (b2 < t2.length && t2[b2].max < t1[n1].min) {
          b2++;
        }
        int min1 = t1[n1].min, max1 = t1[n1].max;

        for (int n2 = b2; n2 < t2.length && t1[n1].max >= t2[n2].min; n2++) {
          if (t2[n2].min > min1) {
            return false;
          }
          if (t2[n2].max < Character.MAX_CODE_POINT) {
            min1 = t2[n2].max + 1;
          } else {
            min1 = Character.MAX_CODE_POINT;
            max1 = Character.MIN_CODE_POINT;
          }
          LightStatePair q = new LightStatePair(t1[n1].dest, t2[n2].dest);
          if (!visited.contains(q)) {
            worklist.add(q);
            visited.add(q);
          }
        }
        if (min1 <= max1) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns true if the language of <code>a1</code> is a subset of the language
   * of <code>a2</code>.  Both automata must be determinized.
   * <p>
   * Complexity: quadratic in number of states.
   */
  /*
  // nocommit low GC but broken!
  public static boolean subsetOf(LightAutomaton a1, LightAutomaton a2) {
    if (a1 == a2) return true;
    LinkedList<LightStatePair> worklist = new LinkedList<>();
    HashSet<LightStatePair> visited = new HashSet<>();
    LightStatePair p = new LightStatePair(0, 0);
    worklist.add(p);
    visited.add(p);
    LightAutomaton.Transition t1 = new LightAutomaton.Transition();
    LightAutomaton.Transition t2 = new LightAutomaton.Transition();
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      System.out.println("pop s1=" + p.s1 + " s2=" + p.s2);
      if (a1.isAccept(p.s1) && a2.isAccept(p.s2) == false) {
        return false;
      }

      int numT1 = a1.initTransition(p.s1, t1);
      for (int n1 = 0, b2 = 0; n1 < numT1; n1++) {
        int numT2 = a2.initTransition(p.s2, t2);
        if (numT2 > 0) {
          a2.getNextTransition(t2);
        }

        a1.getNextTransition(t1);
        while (b2 < numT2 && t2.max < t1.min) {
          b2++;
          if (b2 < numT2) {
            a2.getNextTransition(t2);
          }
        }

        int min1 = t1.min, max1 = t1.max;

        for (int n2 = b2; n2 < numT2 && t1.max >= t2.min; n2++) {
          if (t2.min > min1) {
            return false;
          }
          if (t2.max < Character.MAX_CODE_POINT) {
            min1 = t2.max + 1;
          } else {
            min1 = Character.MAX_CODE_POINT;
            max1 = Character.MIN_CODE_POINT;
          }
          LightStatePair q = new LightStatePair(t1.dest, t2.dest);
          if (!visited.contains(q)) {
            worklist.add(q);
            visited.add(q);
          }
          if (n2 < numT2-1) {
            a2.getNextTransition(t2);
          }
        }
        if (min1 <= max1) {
          return false;
        }
      }
    }
    return true;
  }
  */
  
  /**
   * Returns an automaton that accepts the union of the languages of the given
   * automata.
   * <p>
   * Complexity: linear in number of states.
   */
  public static LightAutomaton unionLight(LightAutomaton a1, LightAutomaton a2) {
    return unionLight(Arrays.asList(a1, a2));
  }

  /**
   * Returns an automaton that accepts the union of the languages of the given
   * automata.
   * <p>
   * Complexity: linear in number of states.
   */
  /*
  public static LightAutomaton unionLight(Collection<LightAutomaton> l) {
    LightAutomaton result = new LightAutomaton();
    // Create initial node:
    result.createState();
    int stateOffset = 1;

    // First pass, adding all states epsilon transitions:
    LightAutomaton.Transition t = new LightAutomaton.Transition();
    for(LightAutomaton a : l) {
      int numStates = a.getNumStates();
      if (a.isAccept(0)) {
        // If any automaton accepts empty string, we do too:
        result.setAccept(0, true);
      }

      for(int s=0;s<numStates;s++) {
        int state = result.createState();
        result.setAccept(state, a.isAccept(s));
      }

      // Add epsilon transition from new initial state to this automaton's initial state:
      int numTransitions = a.initTransition(0, t);
      for(int i=0;i<numTransitions;i++) {
        a.getNextTransition(t);
        result.addTransition(0, stateOffset + t.dest, t.min, t.max);
      }

      stateOffset += numStates;
    }

    // Second pass, copying over all other transitions:
    stateOffset = 1;
    for(LightAutomaton a : l) {
      int numStates = a.getNumStates();
      for(int s=0;s<numStates;s++) {
        int numTransitions = a.initTransition(s, t);
        for(int i=0;i<numTransitions;i++) {
          a.getNextTransition(t);
          result.addTransition(stateOffset + s, stateOffset + t.dest, t.min, t.max);
        }
      }

      stateOffset += numStates;
    }

    result.finish();

    return result;
  }
  */

  public static LightAutomaton unionLight(Collection<LightAutomaton> l) {
    LightAutomaton result = new LightAutomaton();

    // Create initial state:
    result.createState();

    // Copy over all automata
    Transition t = new Transition();
    for(LightAutomaton a : l) {
      result.copy(a);
    }
    
    // Add epsilon transition from new initial state
    int stateOffset = 1;
    for(LightAutomaton a : l) {
      if (a.getNumStates() == 0) {
        continue;
      }
      result.addEpsilon(0, stateOffset);
      stateOffset += a.getNumStates();
    }

    result.finish();

    return result;
  }

  // Simple custom ArrayList<Transition>
  private final static class TransitionListLight {
    // dest, min, max
    int[] transitions = new int[3];
    int next;

    public void add(Transition t) {
      if (transitions.length < next+3) {
        transitions = ArrayUtil.grow(transitions, next+3);
      }
      transitions[next] = t.dest;
      transitions[next+1] = t.min;
      transitions[next+2] = t.max;
      next += 3;
    }
  }

  // Holds all transitions that start on this int point, or
  // end at this point-1
  private final static class PointTransitionsLight implements Comparable<PointTransitionsLight> {
    int point;
    final TransitionListLight ends = new TransitionListLight();
    final TransitionListLight starts = new TransitionListLight();

    @Override
    public int compareTo(PointTransitionsLight other) {
      return point - other.point;
    }

    public void reset(int point) {
      this.point = point;
      ends.next = 0;
      starts.next = 0;
    }

    @Override
    public boolean equals(Object other) {
      return ((PointTransitionsLight) other).point == point;
    }

    @Override
    public int hashCode() {
      return point;
    }
  }

  private final static class PointTransitionSetLight {
    int count;
    PointTransitionsLight[] points = new PointTransitionsLight[5];

    private final static int HASHMAP_CUTOVER = 30;
    private final HashMap<Integer,PointTransitionsLight> map = new HashMap<>();
    private boolean useHash = false;

    private PointTransitionsLight next(int point) {
      // 1st time we are seeing this point
      if (count == points.length) {
        final PointTransitionsLight[] newArray = new PointTransitionsLight[ArrayUtil.oversize(1+count, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(points, 0, newArray, 0, count);
        points = newArray;
      }
      PointTransitionsLight points0 = points[count];
      if (points0 == null) {
        points0 = points[count] = new PointTransitionsLight();
      }
      points0.reset(point);
      count++;
      return points0;
    }

    private PointTransitionsLight find(int point) {
      if (useHash) {
        final Integer pi = point;
        PointTransitionsLight p = map.get(pi);
        if (p == null) {
          p = next(point);
          map.put(pi, p);
        }
        return p;
      } else {
        for(int i=0;i<count;i++) {
          if (points[i].point == point) {
            return points[i];
          }
        }

        final PointTransitionsLight p = next(point);
        if (count == HASHMAP_CUTOVER) {
          // switch to HashMap on the fly
          assert map.size() == 0;
          for(int i=0;i<count;i++) {
            map.put(points[i].point, points[i]);
          }
          useHash = true;
        }
        return p;
      }
    }

    public void reset() {
      if (useHash) {
        map.clear();
        useHash = false;
      }
      count = 0;
    }

    public void sort() {
      // Tim sort performs well on already sorted arrays:
      if (count > 1) ArrayUtil.timSort(points, 0, count);
    }

    public void add(Transition t) {
      find(t.min).starts.add(t);
      find(1+t.max).ends.add(t);
    }

    @Override
    public String toString() {
      StringBuilder s = new StringBuilder();
      for(int i=0;i<count;i++) {
        if (i > 0) {
          s.append(' ');
        }
        s.append(points[i].point).append(':').append(points[i].starts.next/3).append(',').append(points[i].ends.next/3);
      }
      return s.toString();
    }
  }

  /**
   * Determinizes the given automaton.
   * <p>
   * Worst case complexity: exponential in number of states.
   */
  public static LightAutomaton determinize(LightAutomaton a) {
    if (a.isDeterministic()) {
      return a;
    }
    if (a.getNumStates() == 0) {
      return a;
    }

    // subset construction
    LightAutomaton.Builder b = new LightAutomaton.Builder();

    //System.out.println("DET:");
    //a.writeDot("/l/la/lucene/core/detin.dot");

    SortedIntSetLight.FrozenIntSetLight initialset = new SortedIntSetLight.FrozenIntSetLight(0, 0);

    // Create state 0:
    b.createState();

    LinkedList<SortedIntSetLight.FrozenIntSetLight> worklist = new LinkedList<>();
    Map<SortedIntSetLight.FrozenIntSetLight,Integer> newstate = new HashMap<>();

    worklist.add(initialset);

    b.setAccept(0, a.isAccept(0));
    newstate.put(initialset, 0);

    int newStateUpto = 0;
    int[] newStatesArray = new int[5];
    newStatesArray[newStateUpto] = 0;
    newStateUpto++;

    // like Set<Integer,PointTransitions>
    final PointTransitionSetLight points = new PointTransitionSetLight();

    // like SortedMap<Integer,Integer>
    final SortedIntSetLight statesSet = new SortedIntSetLight(5);

    Transition t = new Transition();

    while (worklist.size() > 0) {
      SortedIntSetLight.FrozenIntSetLight s = worklist.removeFirst();
      //System.out.println("det: pop set=" + s);

      // Collate all outgoing transitions by min/1+max:
      for(int i=0;i<s.values.length;i++) {
        final int s0 = s.values[i];
        int numTransitions = a.getNumTransitions(s0);
        a.initTransition(s0, t);
        for(int j=0;j<numTransitions;j++) {
          a.getNextTransition(t);
          points.add(t);
        }
      }

      if (points.count == 0) {
        // No outgoing transitions -- skip it
        continue;
      }

      points.sort();

      int lastPoint = -1;
      int accCount = 0;

      final int r = s.state;

      for(int i=0;i<points.count;i++) {

        final int point = points.points[i].point;

        if (statesSet.upto > 0) {
          assert lastPoint != -1;

          statesSet.computeHash();
          
          Integer q = newstate.get(statesSet);
          if (q == null) {
            q = b.createState();
            final SortedIntSetLight.FrozenIntSetLight p = statesSet.freeze(q);
            //System.out.println("  make new state=" + q + " -> " + p + " accCount=" + accCount);
            worklist.add(p);
            b.setAccept(q, accCount > 0);
            newstate.put(p, q);
          } else {
            assert (accCount > 0 ? true:false) == b.isAccept(q): "accCount=" + accCount + " vs existing accept=" +
              b.isAccept(q) + " states=" + statesSet;
          }

          // System.out.println("  add trans src=" + r + " dest=" + q + " min=" + lastPoint + " max=" + (point-1));

          b.addTransition(r, q, lastPoint, point-1);
        }

        // process transitions that end on this point
        // (closes an overlapping interval)
        int[] transitions = points.points[i].ends.transitions;
        int limit = points.points[i].ends.next;
        for(int j=0;j<limit;j+=3) {
          int dest = transitions[j];
          statesSet.decr(dest);
          accCount -= a.isAccept(dest) ? 1:0;
        }
        points.points[i].ends.next = 0;

        // process transitions that start on this point
        // (opens a new interval)
        transitions = points.points[i].starts.transitions;
        limit = points.points[i].starts.next;
        for(int j=0;j<limit;j+=3) {
          int dest = transitions[j];
          statesSet.incr(dest);
          accCount += a.isAccept(dest) ? 1:0;
        }
        lastPoint = point;
        points.points[i].starts.next = 0;
      }
      points.reset();
      assert statesSet.upto == 0: "upto=" + statesSet.upto;
    }

    LightAutomaton result = b.finish();
    assert result.isDeterministic();
    return result;
  }

  /**
   * Returns true if the given automaton accepts no strings.
   */
  public static boolean isEmpty(LightAutomaton a) {
    return a.isAccept(0) == false && a.getNumTransitions(0) == 0;
  }
  
  /**
   * Returns true if the given automaton accepts all strings.
   */
  public static boolean isTotal(LightAutomaton a) {
    if (a.isAccept(0) && a.getNumTransitions(0) == 1) {
      Transition t = new Transition();
      a.getTransition(0, 0, t);
      return t.dest == 0 && t.min == Character.MIN_CODE_POINT
          && t.max == Character.MAX_CODE_POINT;
    }
    return false;
  }
  
  /**
   * Returns true if the given string is accepted by the automaton.  The input must be deterministic.
   * <p>
   * Complexity: linear in the length of the string.
   * <p>
   * <b>Note:</b> for full performance, use the {@link RunAutomaton} class.
   */
  public static boolean run(LightAutomaton a, String s) {
    assert a.isDeterministic();
    int state = 0;
    for (int i = 0, cp = 0; i < s.length(); i += Character.charCount(cp)) {
      int nextState = a.step(state, cp = s.codePointAt(i));
      if (nextState == -1) {
        return false;
      }
      state = nextState;
    }
    return a.isAccept(state);
  }

  /**
   * Returns true if the given string (expressed as unicode codepoints) is accepted by the automaton.  The input must be deterministic.
   * <p>
   * Complexity: linear in the length of the string.
   * <p>
   * <b>Note:</b> for full performance, use the {@link RunAutomaton} class.
   */
  public static boolean run(LightAutomaton a, IntsRef s) {
    assert a.isDeterministic();
    int state = 0;
    for (int i=0;i<s.length;i++) {
      int nextState = a.step(state, s.ints[s.offset+i]);
      if (nextState == -1) {
        return false;
      }
      state = nextState;
    }
    return a.isAccept(state);
  }

  /**
   * Returns the set of live states. A state is "live" if an accept state is
   * reachable from it and if it is reachable from the initial state.
   */
  private static BitSet getLiveStates(LightAutomaton a) {
    int numStates = a.getNumStates();
    BitSet reachableFromInitial = getLiveStatesFromInitial(a);
    BitSet reachableFromAccept = getLiveStatesFromInitial(SpecialOperations.reverse(a));
    for(int acceptState : a.getAcceptStates()) {
      reachableFromAccept.set(1+acceptState);
    }

    for(int i=0;i<numStates;i++) {
      if (reachableFromAccept.get(i+1) == false) {      
        reachableFromInitial.clear(i);
      }
    }
    return reachableFromInitial;
  }

  /** Returns bitset marking states reachable from the initial node. */
  private static BitSet getLiveStatesFromInitial(LightAutomaton a) {
    int numStates = a.getNumStates();
    BitSet live = new BitSet(numStates);
    LinkedList<Integer> workList = new LinkedList<>();
    live.set(0);
    workList.add(0);

    Transition t = new Transition();
    while (workList.isEmpty() == false) {
      int s = workList.removeFirst();
      int count = a.initTransition(s, t);
      for(int i=0;i<count;i++) {
        a.getNextTransition(t);
        if (live.get(t.dest) == false) {
          live.set(t.dest);
          workList.add(t.dest);
        }
      }
    }

    return live;
  }

  /**
   * Removes transitions to dead states (a state is "dead" if it is not
   * reachable from the initial state or no accept state is reachable from it.)
   */
  public static LightAutomaton removeDeadStates(LightAutomaton a) {
    int numStates = a.getNumStates();
    BitSet liveSet = getLiveStates(a);

    int[] map = new int[numStates];

    LightAutomaton result = new LightAutomaton();
    //System.out.println("liveSet: " + liveSet + " numStates=" + numStates);
    for(int i=0;i<numStates;i++) {
      if (liveSet.get(i)) {
        map[i] = result.createState();
        result.setAccept(map[i], a.isAccept(i));
      }
    }

    Transition t = new Transition();

    for (int i=0;i<numStates;i++) {
      if (liveSet.get(i)) {
        int numTransitions = a.initTransition(i, t);
        // filter out transitions to dead states:
        for(int j=0;j<numTransitions;j++) {
          a.getNextTransition(t);
          if (liveSet.get(t.dest)) {
            result.addTransition(map[i], map[t.dest], t.min, t.max);
          }
        }
      }
    }

    // nocommit need test case for "accepts no strings"

    result.finish();
    return result;
  }

}
