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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   * Complexity: linear in number of states.
   */
  static public Automaton concatenate(Automaton a1, Automaton a2) {
    if (a1.isSingleton() && a2.isSingleton()) return BasicAutomata
        .makeString(a1.singleton + a2.singleton);
    if (isEmpty(a1) || isEmpty(a2))
      return BasicAutomata.makeEmpty();
    // adding epsilon transitions with the NFA concatenation algorithm
    // in this case always produces a resulting DFA, preventing expensive
    // redundant determinize() calls for this common case.
    boolean deterministic = a1.isSingleton() && a2.isDeterministic();
    if (a1 == a2) {
      a1 = a1.cloneExpanded();
      a2 = a2.cloneExpanded();
    } else {
      a1 = a1.cloneExpandedIfRequired();
      a2 = a2.cloneExpandedIfRequired();
    }
    for (State s : a1.getAcceptStates()) {
      s.accept = false;
      s.addEpsilon(a2.initial);
    }
    a1.deterministic = deterministic;
    //a1.clearHashCode();
    a1.clearNumberedStates();
    a1.checkMinimizeAlways();
    return a1;
  }
  
  /**
   * Returns an automaton that accepts the concatenation of the languages of the
   * given automata.
   * <p>
   * Complexity: linear in total number of states.
   */
  static public Automaton concatenate(List<Automaton> l) {
    if (l.isEmpty()) return BasicAutomata.makeEmptyString();
    boolean all_singleton = true;
    for (Automaton a : l)
      if (!a.isSingleton()) {
        all_singleton = false;
        break;
      }
    if (all_singleton) {
      StringBuilder b = new StringBuilder();
      for (Automaton a : l)
        b.append(a.singleton);
      return BasicAutomata.makeString(b.toString());
    } else {
      for (Automaton a : l)
        if (BasicOperations.isEmpty(a)) return BasicAutomata.makeEmpty();
      Set<Integer> ids = new HashSet<Integer>();
      for (Automaton a : l)
        ids.add(System.identityHashCode(a));
      boolean has_aliases = ids.size() != l.size();
      Automaton b = l.get(0);
      if (has_aliases) b = b.cloneExpanded();
      else b = b.cloneExpandedIfRequired();
      Set<State> ac = b.getAcceptStates();
      boolean first = true;
      for (Automaton a : l)
        if (first) first = false;
        else {
          if (a.isEmptyString()) continue;
          Automaton aa = a;
          if (has_aliases) aa = aa.cloneExpanded();
          else aa = aa.cloneExpandedIfRequired();
          Set<State> ns = aa.getAcceptStates();
          for (State s : ac) {
            s.accept = false;
            s.addEpsilon(aa.initial);
            if (s.accept) ns.add(s);
          }
          ac = ns;
        }
      b.deterministic = false;
      //b.clearHashCode();
      b.clearNumberedStates();
      b.checkMinimizeAlways();
      return b;
    }
  }
  
  /**
   * Returns an automaton that accepts the union of the empty string and the
   * language of the given automaton.
   * <p>
   * Complexity: linear in number of states.
   */
  static public Automaton optional(Automaton a) {
    a = a.cloneExpandedIfRequired();
    State s = new State();
    s.addEpsilon(a.initial);
    s.accept = true;
    a.initial = s;
    a.deterministic = false;
    //a.clearHashCode();
    a.clearNumberedStates();
    a.checkMinimizeAlways();
    return a;
  }
  
  /**
   * Returns an automaton that accepts the Kleene star (zero or more
   * concatenated repetitions) of the language of the given automaton. Never
   * modifies the input automaton language.
   * <p>
   * Complexity: linear in number of states.
   */
  static public Automaton repeat(Automaton a) {
    a = a.cloneExpanded();
    State s = new State();
    s.accept = true;
    s.addEpsilon(a.initial);
    for (State p : a.getAcceptStates())
      p.addEpsilon(s);
    a.initial = s;
    a.deterministic = false;
    //a.clearHashCode();
    a.clearNumberedStates();
    a.checkMinimizeAlways();
    return a;
  }
  
  /**
   * Returns an automaton that accepts <code>min</code> or more concatenated
   * repetitions of the language of the given automaton.
   * <p>
   * Complexity: linear in number of states and in <code>min</code>.
   */
  static public Automaton repeat(Automaton a, int min) {
    if (min == 0) return repeat(a);
    List<Automaton> as = new ArrayList<Automaton>();
    while (min-- > 0)
      as.add(a);
    as.add(repeat(a));
    return concatenate(as);
  }
  
  /**
   * Returns an automaton that accepts between <code>min</code> and
   * <code>max</code> (including both) concatenated repetitions of the language
   * of the given automaton.
   * <p>
   * Complexity: linear in number of states and in <code>min</code> and
   * <code>max</code>.
   */
  static public Automaton repeat(Automaton a, int min, int max) {
    if (min > max) return BasicAutomata.makeEmpty();
    max -= min;
    a.expandSingleton();
    Automaton b;
    if (min == 0) b = BasicAutomata.makeEmptyString();
    else if (min == 1) b = a.clone();
    else {
      List<Automaton> as = new ArrayList<Automaton>();
      while (min-- > 0)
        as.add(a);
      b = concatenate(as);
    }
    if (max > 0) {
      Automaton d = a.clone();
      while (--max > 0) {
        Automaton c = a.clone();
        for (State p : c.getAcceptStates())
          p.addEpsilon(d.initial);
        d = c;
      }
      for (State p : b.getAcceptStates())
        p.addEpsilon(d.initial);
      b.deterministic = false;
      //b.clearHashCode();
      b.clearNumberedStates();
      b.checkMinimizeAlways();
    }
    return b;
  }
  
  /**
   * Returns a (deterministic) automaton that accepts the complement of the
   * language of the given automaton.
   * <p>
   * Complexity: linear in number of states (if already deterministic).
   */
  static public Automaton complement(Automaton a) {
    a = a.cloneExpandedIfRequired();
    a.determinize();
    a.totalize();
    for (State p : a.getNumberedStates())
      p.accept = !p.accept;
    a.removeDeadTransitions();
    return a;
  }
  
  /**
   * Returns a (deterministic) automaton that accepts the intersection of the
   * language of <code>a1</code> and the complement of the language of
   * <code>a2</code>. As a side-effect, the automata may be determinized, if not
   * already deterministic.
   * <p>
   * Complexity: quadratic in number of states (if already deterministic).
   */
  static public Automaton minus(Automaton a1, Automaton a2) {
    if (BasicOperations.isEmpty(a1) || a1 == a2) return BasicAutomata
        .makeEmpty();
    if (BasicOperations.isEmpty(a2)) return a1.cloneIfRequired();
    if (a1.isSingleton()) {
      if (BasicOperations.run(a2, a1.singleton)) return BasicAutomata.makeEmpty();
      else return a1.cloneIfRequired();
    }
    return intersection(a1, a2.complement());
  }
  
  /**
   * Returns an automaton that accepts the intersection of the languages of the
   * given automata. Never modifies the input automata languages.
   * <p>
   * Complexity: quadratic in number of states.
   */
  static public Automaton intersection(Automaton a1, Automaton a2) {
    if (a1.isSingleton()) {
      if (BasicOperations.run(a2, a1.singleton)) return a1.cloneIfRequired();
      else return BasicAutomata.makeEmpty();
    }
    if (a2.isSingleton()) {
      if (BasicOperations.run(a1, a2.singleton)) return a2.cloneIfRequired();
      else return BasicAutomata.makeEmpty();
    }
    if (a1 == a2) return a1.cloneIfRequired();
    Transition[][] transitions1 = a1.getSortedTransitions();
    Transition[][] transitions2 = a2.getSortedTransitions();
    Automaton c = new Automaton();
    LinkedList<StatePair> worklist = new LinkedList<StatePair>();
    HashMap<StatePair,StatePair> newstates = new HashMap<StatePair,StatePair>();
    StatePair p = new StatePair(c.initial, a1.initial, a2.initial);
    worklist.add(p);
    newstates.put(p, p);
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      p.s.accept = p.s1.accept && p.s2.accept;
      Transition[] t1 = transitions1[p.s1.number];
      Transition[] t2 = transitions2[p.s2.number];
      for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
        while (b2 < t2.length && t2[b2].max < t1[n1].min)
          b2++;
        for (int n2 = b2; n2 < t2.length && t1[n1].max >= t2[n2].min; n2++)
          if (t2[n2].max >= t1[n1].min) {
            StatePair q = new StatePair(t1[n1].to, t2[n2].to);
            StatePair r = newstates.get(q);
            if (r == null) {
              q.s = new State();
              worklist.add(q);
              newstates.put(q, q);
              r = q;
            }
            int min = t1[n1].min > t2[n2].min ? t1[n1].min : t2[n2].min;
            int max = t1[n1].max < t2[n2].max ? t1[n1].max : t2[n2].max;
            p.s.addTransition(new Transition(min, max, r.s));
          }
      }
    }
    c.deterministic = a1.deterministic && a2.deterministic;
    c.removeDeadTransitions();
    c.checkMinimizeAlways();
    return c;
  }

  /** Returns true if these two automata accept exactly the
   *  same language.  This is a costly computation!  Note
   *  also that a1 and a2 will be determinized as a side
   *  effect. */
  public static boolean sameLanguage(Automaton a1, Automaton a2) {
    if (a1 == a2) {
      return true;
    }
    if (a1.isSingleton() && a2.isSingleton()) {
      return a1.singleton.equals(a2.singleton);
    } else if (a1.isSingleton()) {
      // subsetOf is faster if the first automaton is a singleton
      return subsetOf(a1, a2) && subsetOf(a2, a1);
    } else {
      return subsetOf(a2, a1) && subsetOf(a1, a2);
    }
  }
  
  /**
   * Returns true if the language of <code>a1</code> is a subset of the language
   * of <code>a2</code>. As a side-effect, <code>a2</code> is determinized if
   * not already marked as deterministic.
   * <p>
   * Complexity: quadratic in number of states.
   */
  public static boolean subsetOf(Automaton a1, Automaton a2) {
    if (a1 == a2) return true;
    if (a1.isSingleton()) {
      if (a2.isSingleton()) return a1.singleton.equals(a2.singleton);
      return BasicOperations.run(a2, a1.singleton);
    }
    a2.determinize();
    Transition[][] transitions1 = a1.getSortedTransitions();
    Transition[][] transitions2 = a2.getSortedTransitions();
    LinkedList<StatePair> worklist = new LinkedList<StatePair>();
    HashSet<StatePair> visited = new HashSet<StatePair>();
    StatePair p = new StatePair(a1.initial, a2.initial);
    worklist.add(p);
    visited.add(p);
    while (worklist.size() > 0) {
      p = worklist.removeFirst();
      if (p.s1.accept && !p.s2.accept) {
        return false;
      }
      Transition[] t1 = transitions1[p.s1.number];
      Transition[] t2 = transitions2[p.s2.number];
      for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
        while (b2 < t2.length && t2[b2].max < t1[n1].min)
          b2++;
        int min1 = t1[n1].min, max1 = t1[n1].max;

        for (int n2 = b2; n2 < t2.length && t1[n1].max >= t2[n2].min; n2++) {
          if (t2[n2].min > min1) {
            return false;
          }
          if (t2[n2].max < Character.MAX_CODE_POINT) min1 = t2[n2].max + 1;
          else {
            min1 = Character.MAX_CODE_POINT;
            max1 = Character.MIN_CODE_POINT;
          }
          StatePair q = new StatePair(t1[n1].to, t2[n2].to);
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
   * Returns an automaton that accepts the union of the languages of the given
   * automata.
   * <p>
   * Complexity: linear in number of states.
   */
  public static Automaton union(Automaton a1, Automaton a2) {
    if ((a1.isSingleton() && a2.isSingleton() && a1.singleton
        .equals(a2.singleton))
        || a1 == a2) return a1.cloneIfRequired();
    if (a1 == a2) {
      a1 = a1.cloneExpanded();
      a2 = a2.cloneExpanded();
    } else {
      a1 = a1.cloneExpandedIfRequired();
      a2 = a2.cloneExpandedIfRequired();
    }
    State s = new State();
    s.addEpsilon(a1.initial);
    s.addEpsilon(a2.initial);
    a1.initial = s;
    a1.deterministic = false;
    //a1.clearHashCode();
    a1.clearNumberedStates();
    a1.checkMinimizeAlways();
    return a1;
  }
  
  /**
   * Returns an automaton that accepts the union of the languages of the given
   * automata.
   * <p>
   * Complexity: linear in number of states.
   */
  public static Automaton union(Collection<Automaton> l) {
    Set<Integer> ids = new HashSet<Integer>();
    for (Automaton a : l)
      ids.add(System.identityHashCode(a));
    boolean has_aliases = ids.size() != l.size();
    State s = new State();
    for (Automaton b : l) {
      if (BasicOperations.isEmpty(b)) continue;
      Automaton bb = b;
      if (has_aliases) bb = bb.cloneExpanded();
      else bb = bb.cloneExpandedIfRequired();
      s.addEpsilon(bb.initial);
    }
    Automaton a = new Automaton();
    a.initial = s;
    a.deterministic = false;
    //a.clearHashCode();
    a.clearNumberedStates();
    a.checkMinimizeAlways();
    return a;
  }

  // Simple custom ArrayList<Transition>
  private final static class TransitionList {
    Transition[] transitions = new Transition[2];
    int count;

    public void add(Transition t) {
      if (transitions.length == count) {
        Transition[] newArray = new Transition[ArrayUtil.oversize(1+count, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(transitions, 0, newArray, 0, count);
        transitions = newArray;
      }
      transitions[count++] = t;
    }
  }

  // Holds all transitions that start on this int point, or
  // end at this point-1
  private final static class PointTransitions implements Comparable<PointTransitions> {
    int point;
    final TransitionList ends = new TransitionList();
    final TransitionList starts = new TransitionList();
    public int compareTo(PointTransitions other) {
      return point - other.point;
    }

    public void reset(int point) {
      this.point = point;
      ends.count = 0;
      starts.count = 0;
    }

    @Override
    public boolean equals(Object other) {
      return ((PointTransitions) other).point == point;
    }

    @Override
    public int hashCode() {
      return point;
    }
  }

  private final static class PointTransitionSet {
    int count;
    PointTransitions[] points = new PointTransitions[5];

    private final static int HASHMAP_CUTOVER = 30;
    private final HashMap<Integer,PointTransitions> map = new HashMap<Integer,PointTransitions>();
    private boolean useHash = false;

    private PointTransitions next(int point) {
      // 1st time we are seeing this point
      if (count == points.length) {
        final PointTransitions[] newArray = new PointTransitions[ArrayUtil.oversize(1+count, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(points, 0, newArray, 0, count);
        points = newArray;
      }
      PointTransitions points0 = points[count];
      if (points0 == null) {
        points0 = points[count] = new PointTransitions();
      }
      points0.reset(point);
      count++;
      return points0;
    }

    private PointTransitions find(int point) {
      if (useHash) {
        final Integer pi = point;
        PointTransitions p = map.get(pi);
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

        final PointTransitions p = next(point);
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
      // mergesort seems to perform better on already sorted arrays:
      if (count > 1) ArrayUtil.mergeSort(points, 0, count);
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
        s.append(points[i].point).append(':').append(points[i].starts.count).append(',').append(points[i].ends.count);
      }
      return s.toString();
    }
  }

  /**
   * Determinizes the given automaton.
   * <p>
   * Worst case complexity: exponential in number of states.
   */
  static void determinize(Automaton a) {
    if (a.deterministic || a.isSingleton()) {
      return;
    }

    final State[] allStates = a.getNumberedStates();

    // subset construction
    final boolean initAccept = a.initial.accept;
    final int initNumber = a.initial.number;
    a.initial = new State();
    SortedIntSet.FrozenIntSet initialset = new SortedIntSet.FrozenIntSet(initNumber, a.initial);

    LinkedList<SortedIntSet.FrozenIntSet> worklist = new LinkedList<SortedIntSet.FrozenIntSet>();
    Map<SortedIntSet.FrozenIntSet,State> newstate = new HashMap<SortedIntSet.FrozenIntSet,State>();

    worklist.add(initialset);

    a.initial.accept = initAccept;
    newstate.put(initialset, a.initial);

    int newStateUpto = 0;
    State[] newStatesArray = new State[5];
    newStatesArray[newStateUpto] = a.initial;
    a.initial.number = newStateUpto;
    newStateUpto++;

    // like Set<Integer,PointTransitions>
    final PointTransitionSet points = new PointTransitionSet();

    // like SortedMap<Integer,Integer>
    final SortedIntSet statesSet = new SortedIntSet(5);

    while (worklist.size() > 0) {
      SortedIntSet.FrozenIntSet s = worklist.removeFirst();

      // Collate all outgoing transitions by min/1+max:
      for(int i=0;i<s.values.length;i++) {
        final State s0 = allStates[s.values[i]];
        for(int j=0;j<s0.numTransitions;j++) {
          points.add(s0.transitionsArray[j]);
        }
      }

      if (points.count == 0) {
        // No outgoing transitions -- skip it
        continue;
      }

      points.sort();

      int lastPoint = -1;
      int accCount = 0;

      final State r = s.state;
      for(int i=0;i<points.count;i++) {

        final int point = points.points[i].point;

        if (statesSet.upto > 0) {
          assert lastPoint != -1;

          statesSet.computeHash();
          
          State q = newstate.get(statesSet);
          if (q == null) {
            q = new State();
            final SortedIntSet.FrozenIntSet p = statesSet.freeze(q);
            worklist.add(p);
            if (newStateUpto == newStatesArray.length) {
              final State[] newArray = new State[ArrayUtil.oversize(1+newStateUpto, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
              System.arraycopy(newStatesArray, 0, newArray, 0, newStateUpto);
              newStatesArray = newArray;
            }
            newStatesArray[newStateUpto] = q;
            q.number = newStateUpto;
            newStateUpto++;
            q.accept = accCount > 0;
            newstate.put(p, q);
          } else {
            assert (accCount > 0 ? true:false) == q.accept: "accCount=" + accCount + " vs existing accept=" + q.accept + " states=" + statesSet;
          }

          r.addTransition(new Transition(lastPoint, point-1, q));
        }

        // process transitions that end on this point
        // (closes an overlapping interval)
        Transition[] transitions = points.points[i].ends.transitions;
        int limit = points.points[i].ends.count;
        for(int j=0;j<limit;j++) {
          final Transition t = transitions[j];
          final Integer num = t.to.number;
          statesSet.decr(num);
          accCount -= t.to.accept ? 1:0;
        }
        points.points[i].ends.count = 0;

        // process transitions that start on this point
        // (opens a new interval)
        transitions = points.points[i].starts.transitions;
        limit = points.points[i].starts.count;
        for(int j=0;j<limit;j++) {
          final Transition t = transitions[j];
          final Integer num = t.to.number;
          statesSet.incr(num);
          accCount += t.to.accept ? 1:0;
        }
        lastPoint = point;
        points.points[i].starts.count = 0;
      }
      points.reset();
      assert statesSet.upto == 0: "upto=" + statesSet.upto;
    }
    a.deterministic = true;
    a.setNumberedStates(newStatesArray, newStateUpto);
  }
  
  /**
   * Adds epsilon transitions to the given automaton. This method adds extra
   * character interval transitions that are equivalent to the given set of
   * epsilon transitions.
   * 
   * @param pairs collection of {@link StatePair} objects representing pairs of
   *          source/destination states where epsilon transitions should be
   *          added
   */
  public static void addEpsilons(Automaton a, Collection<StatePair> pairs) {
    a.expandSingleton();
    HashMap<State,HashSet<State>> forward = new HashMap<State,HashSet<State>>();
    HashMap<State,HashSet<State>> back = new HashMap<State,HashSet<State>>();
    for (StatePair p : pairs) {
      HashSet<State> to = forward.get(p.s1);
      if (to == null) {
        to = new HashSet<State>();
        forward.put(p.s1, to);
      }
      to.add(p.s2);
      HashSet<State> from = back.get(p.s2);
      if (from == null) {
        from = new HashSet<State>();
        back.put(p.s2, from);
      }
      from.add(p.s1);
    }
    // calculate epsilon closure
    LinkedList<StatePair> worklist = new LinkedList<StatePair>(pairs);
    HashSet<StatePair> workset = new HashSet<StatePair>(pairs);
    while (!worklist.isEmpty()) {
      StatePair p = worklist.removeFirst();
      workset.remove(p);
      HashSet<State> to = forward.get(p.s2);
      HashSet<State> from = back.get(p.s1);
      if (to != null) {
        for (State s : to) {
          StatePair pp = new StatePair(p.s1, s);
          if (!pairs.contains(pp)) {
            pairs.add(pp);
            forward.get(p.s1).add(s);
            back.get(s).add(p.s1);
            worklist.add(pp);
            workset.add(pp);
            if (from != null) {
              for (State q : from) {
                StatePair qq = new StatePair(q, p.s1);
                if (!workset.contains(qq)) {
                  worklist.add(qq);
                  workset.add(qq);
                }
              }
            }
          }
        }
      }
    }
    // add transitions
    for (StatePair p : pairs)
      p.s1.addEpsilon(p.s2);
    a.deterministic = false;
    //a.clearHashCode();
    a.clearNumberedStates();
    a.checkMinimizeAlways();
  }
  
  /**
   * Returns true if the given automaton accepts the empty string and nothing
   * else.
   */
  public static boolean isEmptyString(Automaton a) {
    if (a.isSingleton()) return a.singleton.length() == 0;
    else return a.initial.accept && a.initial.numTransitions() == 0;
  }
  
  /**
   * Returns true if the given automaton accepts no strings.
   */
  public static boolean isEmpty(Automaton a) {
    if (a.isSingleton()) return false;
    return !a.initial.accept && a.initial.numTransitions() == 0;
  }
  
  /**
   * Returns true if the given automaton accepts all strings.
   */
  public static boolean isTotal(Automaton a) {
    if (a.isSingleton()) return false;
    if (a.initial.accept && a.initial.numTransitions() == 1) {
      Transition t = a.initial.getTransitions().iterator().next();
      return t.to == a.initial && t.min == Character.MIN_CODE_POINT
          && t.max == Character.MAX_CODE_POINT;
    }
    return false;
  }
  
  /**
   * Returns true if the given string is accepted by the automaton.
   * <p>
   * Complexity: linear in the length of the string.
   * <p>
   * <b>Note:</b> for full performance, use the {@link RunAutomaton} class.
   */
  public static boolean run(Automaton a, String s) {
    if (a.isSingleton()) return s.equals(a.singleton);
    if (a.deterministic) {
      State p = a.initial;
      for (int i = 0, cp = 0; i < s.length(); i += Character.charCount(cp)) {
        State q = p.step(cp = s.codePointAt(i));
        if (q == null) return false;
        p = q;
      }
      return p.accept;
    } else {
      State[] states = a.getNumberedStates();
      LinkedList<State> pp = new LinkedList<State>();
      LinkedList<State> pp_other = new LinkedList<State>();
      BitSet bb = new BitSet(states.length);
      BitSet bb_other = new BitSet(states.length);
      pp.add(a.initial);
      ArrayList<State> dest = new ArrayList<State>();
      boolean accept = a.initial.accept;
      for (int i = 0, c = 0; i < s.length(); i += Character.charCount(c)) {
        c = s.codePointAt(i);
        accept = false;
        pp_other.clear();
        bb_other.clear();
        for (State p : pp) {
          dest.clear();
          p.step(c, dest);
          for (State q : dest) {
            if (q.accept) accept = true;
            if (!bb_other.get(q.number)) {
              bb_other.set(q.number);
              pp_other.add(q);
            }
          }
        }
        LinkedList<State> tp = pp;
        pp = pp_other;
        pp_other = tp;
        BitSet tb = bb;
        bb = bb_other;
        bb_other = tb;
      }
      return accept;
    }
  }
}
