package org.apache.lucene.util.automaton;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;

public class AutomatonTestUtil {
  /** Returns random string, including full unicode range. */
  public static String randomRegexp(Random r) {
    while (true) {
      String regexp = randomRegexpString(r);
      // we will also generate some undefined unicode queries
      if (!UnicodeUtil.validUTF16String(regexp))
        continue;
      try {
        new RegExp(regexp, RegExp.NONE);
        return regexp;
      } catch (Exception e) {}
    }
  }

  private static String randomRegexpString(Random r) {
    final int end = r.nextInt(20);
    if (end == 0) {
      // allow 0 length
      return "";
    }
    final char[] buffer = new char[end];
    for (int i = 0; i < end; i++) {
      int t = r.nextInt(15);
      if (0 == t && i < end - 1) {
        // Make a surrogate pair
        // High surrogate
        buffer[i++] = (char) _TestUtil.nextInt(r, 0xd800, 0xdbff);
        // Low surrogate
        buffer[i] = (char) _TestUtil.nextInt(r, 0xdc00, 0xdfff);
      }
      else if (t <= 1) buffer[i] = (char) r.nextInt(0x80);
      else if (2 == t) buffer[i] = (char) _TestUtil.nextInt(r, 0x80, 0x800);
      else if (3 == t) buffer[i] = (char) _TestUtil.nextInt(r, 0x800, 0xd7ff);
      else if (4 == t) buffer[i] = (char) _TestUtil.nextInt(r, 0xe000, 0xffff);
      else if (5 == t) buffer[i] = '.';
      else if (6 == t) buffer[i] = '?';
      else if (7 == t) buffer[i] = '*';
      else if (8 == t) buffer[i] = '+';
      else if (9 == t) buffer[i] = '(';
      else if (10 == t) buffer[i] = ')';
      else if (11 == t) buffer[i] = '-';
      else if (12 == t) buffer[i] = '[';
      else if (13 == t) buffer[i] = ']';
      else if (14 == t) buffer[i] = '|';
    }
    return new String(buffer, 0, end);
  }
  
  /** picks a random int code point, avoiding surrogates;
   * throws IllegalArgumentException if this transition only
   * accepts surrogates */
  private static int getRandomCodePoint(final Random r, final Transition t) {
    final int code;
    if (t.max < UnicodeUtil.UNI_SUR_HIGH_START ||
        t.min > UnicodeUtil.UNI_SUR_HIGH_END) {
      // easy: entire range is before or after surrogates
      code = t.min+r.nextInt(t.max-t.min+1);
    } else if (t.min >= UnicodeUtil.UNI_SUR_HIGH_START) {
      if (t.max > UnicodeUtil.UNI_SUR_LOW_END) {
        // after surrogates
        code = 1+UnicodeUtil.UNI_SUR_LOW_END+r.nextInt(t.max-UnicodeUtil.UNI_SUR_LOW_END);
      } else {
        throw new IllegalArgumentException("transition accepts only surrogates: " + t);
      }
    } else if (t.max <= UnicodeUtil.UNI_SUR_LOW_END) {
      if (t.min < UnicodeUtil.UNI_SUR_HIGH_START) {
        // before surrogates
        code = t.min + r.nextInt(UnicodeUtil.UNI_SUR_HIGH_START - t.min);
      } else {
        throw new IllegalArgumentException("transition accepts only surrogates: " + t);
      }
    } else {
      // range includes all surrogates
      int gap1 = UnicodeUtil.UNI_SUR_HIGH_START - t.min;
      int gap2 = t.max - UnicodeUtil.UNI_SUR_LOW_END;
      int c = r.nextInt(gap1+gap2);
      if (c < gap1) {
        code = t.min + c;
      } else {
        code = UnicodeUtil.UNI_SUR_LOW_END + c - gap1 + 1;
      }
    }

    assert code >= t.min && code <= t.max && (code < UnicodeUtil.UNI_SUR_HIGH_START || code > UnicodeUtil.UNI_SUR_LOW_END):
      "code=" + code + " min=" + t.min + " max=" + t.max;
    return code;
  }

  public static class RandomAcceptedStrings {

    private final Map<Transition,Boolean> leadsToAccept;
    private final Automaton a;

    private static class ArrivingTransition {
      final State from;
      final Transition t;
      public ArrivingTransition(State from, Transition t) {
        this.from = from;
        this.t = t;
      }
    }

    public RandomAcceptedStrings(Automaton a) {
      this.a = a;
      if (a.isSingleton()) {
        leadsToAccept = null;
        return;
      }

      // must use IdentityHashmap because two Transitions w/
      // different start nodes can be considered the same
      leadsToAccept = new IdentityHashMap<Transition,Boolean>();
      final Map<State,List<ArrivingTransition>> allArriving = new HashMap<State,List<ArrivingTransition>>();

      final LinkedList<State> q = new LinkedList<State>();
      final Set<State> seen = new HashSet<State>();

      // reverse map the transitions, so we can quickly look
      // up all arriving transitions to a given state
      for(State s: a.getNumberedStates()) {
        for(int i=0;i<s.numTransitions;i++) {
          final Transition t = s.transitionsArray[i];
          List<ArrivingTransition> tl = allArriving.get(t.to);
          if (tl == null) {
            tl = new ArrayList<ArrivingTransition>();
            allArriving.put(t.to, tl);
          }
          tl.add(new ArrivingTransition(s, t));
        }
        if (s.accept) {
          q.add(s);
          seen.add(s);
        }
      }

      // Breadth-first search, from accept states,
      // backwards:
      while(!q.isEmpty()) {
        final State s = q.removeFirst();
        List<ArrivingTransition> arriving = allArriving.get(s);
        if (arriving != null) {
          for(ArrivingTransition at : arriving) {
            final State from = at.from;
            if (!seen.contains(from)) {
              q.add(from);
              seen.add(from);
              leadsToAccept.put(at.t, Boolean.TRUE);
            }
          }
        }
      }
    }

    public int[] getRandomAcceptedString(Random r) {

      final List<Integer> soFar = new ArrayList<Integer>();
      if (a.isSingleton()) {
        // accepts only one
        final String s = a.singleton;
      
        int charUpto = 0;
        while(charUpto < s.length()) {
          final int cp = s.codePointAt(charUpto);
          charUpto += Character.charCount(cp);
          soFar.add(cp);
        }
      } else {

        State s = a.initial;

        while(true) {
      
          if (s.accept) {
            if (s.numTransitions == 0) {
              // stop now
              break;
            } else {
              if (r.nextBoolean()) {
                break;
              }
            }
          }

          if (s.numTransitions == 0) {
            throw new RuntimeException("this automaton has dead states");
          }

          boolean cheat = r.nextBoolean();

          final Transition t;
          if (cheat) {
            // pick a transition that we know is the fastest
            // path to an accept state
            List<Transition> toAccept = new ArrayList<Transition>();
            for(int i=0;i<s.numTransitions;i++) {
              final Transition t0 = s.transitionsArray[i];
              if (leadsToAccept.containsKey(t0)) {
                toAccept.add(t0);
              }
            }
            if (toAccept.size() == 0) {
              // this is OK -- it means we jumped into a cycle
              t = s.transitionsArray[r.nextInt(s.numTransitions)];
            } else {
              t = toAccept.get(r.nextInt(toAccept.size()));
            }
          } else {
            t = s.transitionsArray[r.nextInt(s.numTransitions)];
          }
          soFar.add(getRandomCodePoint(r, t));
          s = t.to;
        }
      }

      return ArrayUtil.toIntArray(soFar);
    }
  }
  
  /** return a random NFA/DFA for testing */
  public static Automaton randomAutomaton(Random random) {
    // get two random Automata from regexps
    Automaton a1 = new RegExp(AutomatonTestUtil.randomRegexp(random), RegExp.NONE).toAutomaton();
    if (random.nextBoolean())
      a1 = BasicOperations.complement(a1);
    
    Automaton a2 = new RegExp(AutomatonTestUtil.randomRegexp(random), RegExp.NONE).toAutomaton();
    if (random.nextBoolean()) 
      a2 = BasicOperations.complement(a2);
    
    // combine them in random ways
    switch(random.nextInt(4)) {
      case 0: return BasicOperations.concatenate(a1, a2);
      case 1: return BasicOperations.union(a1, a2);
      case 2: return BasicOperations.intersection(a1, a2);
      default: return BasicOperations.minus(a1, a2);
    }
  }
  
  /** 
   * below are original, unoptimized implementations of DFA operations for testing.
   * These are from brics automaton, full license (BSD) below:
   */
  
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

  /**
   * Simple, original brics implementation of Brzozowski minimize()
   */
  public static void minimizeSimple(Automaton a) {
    if (a.isSingleton())
      return;
    determinizeSimple(a, SpecialOperations.reverse(a));
    determinizeSimple(a, SpecialOperations.reverse(a));
  }
  
  /**
   * Simple, original brics implementation of determinize()
   */
  public static void determinizeSimple(Automaton a) {
    if (a.deterministic || a.isSingleton())
      return;
    Set<State> initialset = new HashSet<State>();
    initialset.add(a.initial);
    determinizeSimple(a, initialset);
  }
  
  /** 
   * Simple, original brics implementation of determinize()
   * Determinizes the given automaton using the given set of initial states. 
   */
  public static void determinizeSimple(Automaton a, Set<State> initialset) {
    int[] points = a.getStartPoints();
    // subset construction
    Map<Set<State>, Set<State>> sets = new HashMap<Set<State>, Set<State>>();
    LinkedList<Set<State>> worklist = new LinkedList<Set<State>>();
    Map<Set<State>, State> newstate = new HashMap<Set<State>, State>();
    sets.put(initialset, initialset);
    worklist.add(initialset);
    a.initial = new State();
    newstate.put(initialset, a.initial);
    while (worklist.size() > 0) {
      Set<State> s = worklist.removeFirst();
      State r = newstate.get(s);
      for (State q : s)
        if (q.accept) {
          r.accept = true;
          break;
        }
      for (int n = 0; n < points.length; n++) {
        Set<State> p = new HashSet<State>();
        for (State q : s)
          for (Transition t : q.getTransitions())
            if (t.min <= points[n] && points[n] <= t.max)
              p.add(t.to);
        if (!sets.containsKey(p)) {
          sets.put(p, p);
          worklist.add(p);
          newstate.put(p, new State());
        }
        State q = newstate.get(p);
        int min = points[n];
        int max;
        if (n + 1 < points.length)
          max = points[n + 1] - 1;
        else
          max = Character.MAX_CODE_POINT;
        r.addTransition(new Transition(min, max, q));
      }
    }
    a.deterministic = true;
    a.clearNumberedStates();
    a.removeDeadTransitions();
  }

  /**
   * Returns true if the language of this automaton is finite.
   * <p>
   * WARNING: this method is slow, it will blow up if the automaton is large.
   * this is only used to test the correctness of our faster implementation.
   */
  public static boolean isFiniteSlow(Automaton a) {
    if (a.isSingleton()) return true;
    return isFiniteSlow(a.initial, new HashSet<State>());
  }
  
  /**
   * Checks whether there is a loop containing s. (This is sufficient since
   * there are never transitions to dead states.)
   */
  // TODO: not great that this is recursive... in theory a
  // large automata could exceed java's stack
  private static boolean isFiniteSlow(State s, HashSet<State> path) {
    path.add(s);
    for (Transition t : s.getTransitions())
      if (path.contains(t.to) || !isFiniteSlow(t.to, path)) return false;
    path.remove(s);
    return true;
  }
  
  
  /**
   * Checks that an automaton has no detached states that are unreachable
   * from the initial state.
   */
  public static void assertNoDetachedStates(Automaton a) {
    int numStates = a.getNumberOfStates();
    a.clearNumberedStates(); // force recomputation of cached numbered states
    assert numStates == a.getNumberOfStates() : "automaton has " + (numStates - a.getNumberOfStates()) + " detached states";
  }
}
