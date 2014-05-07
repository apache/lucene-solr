package org.apache.lucene.util.automaton;

/*
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.fst.Util;

public class TestSpecialOperations extends LuceneTestCase {
  /**
   * tests against the original brics implementation.
   */
  public void testIsFinite() {
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      Automaton b = a.clone();
      assertEquals(AutomatonTestUtil.isFiniteSlow(a), SpecialOperations.isFinite(b));
    }
  }

  /** Pass false for testRecursive if the expected strings
   *  may be too long */
  private Set<IntsRef> getFiniteStrings(Automaton a, int limit, boolean testRecursive) {
    Set<IntsRef> result = SpecialOperations.getFiniteStrings(a, limit);
    if (testRecursive) {
      assertEquals(AutomatonTestUtil.getFiniteStringsRecursive(a, limit), result);
    }
    return result;
  }
  
  /**
   * Basic test for getFiniteStrings
   */
  public void testFiniteStringsBasic() {
    Automaton a = BasicOperations.union(BasicAutomata.makeString("dog"), BasicAutomata.makeString("duck"));
    MinimizationOperations.minimize(a);
    Set<IntsRef> strings = getFiniteStrings(a, -1, true);
    assertEquals(2, strings.size());
    IntsRef dog = new IntsRef();
    Util.toIntsRef(new BytesRef("dog"), dog);
    assertTrue(strings.contains(dog));
    IntsRef duck = new IntsRef();
    Util.toIntsRef(new BytesRef("duck"), duck);
    assertTrue(strings.contains(duck));
  }

  public void testFiniteStringsEatsStack() {
    char[] chars = new char[50000];
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString1 = new String(chars);
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString2 = new String(chars);
    Automaton a = BasicOperations.union(BasicAutomata.makeString(bigString1), BasicAutomata.makeString(bigString2));
    Set<IntsRef> strings = getFiniteStrings(a, -1, false);
    assertEquals(2, strings.size());
    IntsRef scratch = new IntsRef();
    Util.toUTF32(bigString1.toCharArray(), 0, bigString1.length(), scratch);
    assertTrue(strings.contains(scratch));
    Util.toUTF32(bigString2.toCharArray(), 0, bigString2.length(), scratch);
    assertTrue(strings.contains(scratch));
  }

  public void testRandomFiniteStrings1() {

    int numStrings = atLeast(100);
    if (VERBOSE) {
      System.out.println("TEST: numStrings=" + numStrings);
    }

    Set<IntsRef> strings = new HashSet<IntsRef>();
    List<Automaton> automata = new ArrayList<Automaton>();
    for(int i=0;i<numStrings;i++) {
      String s = TestUtil.randomSimpleString(random(), 1, 200);
      automata.add(BasicAutomata.makeString(s));
      IntsRef scratch = new IntsRef();
      Util.toUTF32(s.toCharArray(), 0, s.length(), scratch);
      strings.add(scratch);
      if (VERBOSE) {
        System.out.println("  add string=" + s);
      }
    }

    // TODO: we could sometimes use
    // DaciukMihovAutomatonBuilder here

    // TODO: what other random things can we do here...
    Automaton a = BasicOperations.union(automata);
    if (random().nextBoolean()) {
      Automaton.minimize(a);
      if (VERBOSE) {
        System.out.println("TEST: a.minimize numStates=" + a.getNumberOfStates());
      }
    } else if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: a.determinize");
      }
      a.determinize();
    } else if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: a.reduce");
      }
      a.reduce();
    } else if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: a.getNumberedStates");
      }
      a.getNumberedStates();
    }

    Set<IntsRef> actual = getFiniteStrings(a, -1, true);
    if (strings.equals(actual) == false) {
      System.out.println("strings.size()=" + strings.size() + " actual.size=" + actual.size());
      List<IntsRef> x = new ArrayList<>(strings);
      Collections.sort(x);
      List<IntsRef> y = new ArrayList<>(actual);
      Collections.sort(y);
      int end = Math.min(x.size(), y.size());
      for(int i=0;i<end;i++) {
        System.out.println("  i=" + i + " string=" + toString(x.get(i)) + " actual=" + toString(y.get(i)));
      }
      fail("wrong strings found");
    }
  }

  // ascii only!
  private static String toString(IntsRef ints) {
    BytesRef br = new BytesRef(ints.length);
    for(int i=0;i<ints.length;i++) {
      br.bytes[i] = (byte) ints.ints[i];
    }
    br.length = ints.length;
    return br.utf8ToString();
  }

  public void testWithCycle() throws Exception {
    try {
      SpecialOperations.getFiniteStrings(new RegExp("abc.*", RegExp.NONE).toAutomaton(), -1);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testRandomFiniteStrings2() {
    // Just makes sure we can run on any random finite
    // automaton:
    int iters = atLeast(100);
    for(int i=0;i<iters;i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      try {
        // Must pass a limit because the random automaton
        // can accept MANY strings:
        SpecialOperations.getFiniteStrings(a, TestUtil.nextInt(random(), 1, 1000));
        // NOTE: cannot do this, because the method is not
        // guaranteed to detect cycles when you have a limit
        //assertTrue(SpecialOperations.isFinite(a));
      } catch (IllegalArgumentException iae) {
        assertFalse(SpecialOperations.isFinite(a));
      }
    }
  }

  public void testInvalidLimit() {
    Automaton a = AutomatonTestUtil.randomAutomaton(random());
    try {
      SpecialOperations.getFiniteStrings(a, -7);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testInvalidLimit2() {
    Automaton a = AutomatonTestUtil.randomAutomaton(random());
    try {
      SpecialOperations.getFiniteStrings(a, 0);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testSingletonNoLimit() {
    Set<IntsRef> result = SpecialOperations.getFiniteStrings(BasicAutomata.makeString("foobar"), -1);
    assertEquals(1, result.size());
    IntsRef scratch = new IntsRef();
    Util.toUTF32("foobar".toCharArray(), 0, 6, scratch);
    assertTrue(result.contains(scratch));
  }

  public void testSingletonLimit1() {
    Set<IntsRef> result = SpecialOperations.getFiniteStrings(BasicAutomata.makeString("foobar"), 1);
    assertEquals(1, result.size());
    IntsRef scratch = new IntsRef();
    Util.toUTF32("foobar".toCharArray(), 0, 6, scratch);
    assertTrue(result.contains(scratch));
  }
}
