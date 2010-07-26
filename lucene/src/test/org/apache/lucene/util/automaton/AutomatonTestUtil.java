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
  public static RegExp randomRegexp(Random r) {
    while (true) {
      String regexp = randomRegexpString(r);
      // we will also generate some undefined unicode queries
      if (!UnicodeUtil.validUTF16String(regexp))
        continue;
      try {
        // NOTE: we parse-tostring-parse again, because we are
        // really abusing RegExp.toString() here (its just for debugging)
        return new RegExp(new RegExp(regexp, RegExp.NONE).toString(), RegExp.NONE);
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
      int t = r.nextInt(11);
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
    }
    return new String(buffer, 0, end);
  }
  
  // picks a random int code point that this transition
  // accepts, avoiding the surrogates range since they are
  // "defined" in UTF32.  Don't call this on a transition
  // that only accepts UTF16 surrogate values!!
  private static int getRandomCodePoint(final Random r, final Transition t) {
    return t.min+r.nextInt(t.max-t.min+1);
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
}
