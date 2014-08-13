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

import java.util.*;

import org.apache.lucene.util.*;
import org.apache.lucene.util.fst.Util;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

public class TestOperations extends LuceneTestCase {
  /** Test string union. */
  public void testStringUnion() {
    List<BytesRef> strings = new ArrayList<>();
    for (int i = RandomInts.randomIntBetween(random(), 0, 1000); --i >= 0;) {
      strings.add(new BytesRef(TestUtil.randomUnicodeString(random())));
    }

    Collections.sort(strings);
    Automaton union = Automata.makeStringUnion(strings);
    assertTrue(union.isDeterministic());
    assertFalse(Operations.hasDeadStatesFromInitial(union));
    
    Automaton naiveUnion = naiveUnion(strings);
    assertTrue(naiveUnion.isDeterministic());
    assertFalse(Operations.hasDeadStatesFromInitial(naiveUnion));

    
    assertTrue(Operations.sameLanguage(union, naiveUnion));
  }

  private static Automaton naiveUnion(List<BytesRef> strings) {
    Automaton[] eachIndividual = new Automaton[strings.size()];
    int i = 0;
    for (BytesRef bref : strings) {
      eachIndividual[i++] = Automata.makeString(bref.utf8ToString());
    }
    return Operations.determinize(Operations.union(Arrays.asList(eachIndividual)));
  }

  /** Test concatenation with empty language returns empty */
  public void testEmptyLanguageConcatenate() {
    Automaton a = Automata.makeString("a");
    Automaton concat = Operations.concatenate(a, Automata.makeEmpty());
    assertTrue(Operations.isEmpty(concat));
  }
  
  /** Test optimization to concatenate() with empty String to an NFA */
  public void testEmptySingletonNFAConcatenate() {
    Automaton singleton = Automata.makeString("");
    Automaton expandedSingleton = singleton;
    // an NFA (two transitions for 't' from initial state)
    Automaton nfa = Operations.union(Automata.makeString("this"),
        Automata.makeString("three"));
    Automaton concat1 = Operations.concatenate(expandedSingleton, nfa);
    Automaton concat2 = Operations.concatenate(singleton, nfa);
    assertFalse(concat2.isDeterministic());
    assertTrue(Operations.sameLanguage(Operations.determinize(concat1),
                                       Operations.determinize(concat2)));
    assertTrue(Operations.sameLanguage(Operations.determinize(nfa),
                                       Operations.determinize(concat1)));
    assertTrue(Operations.sameLanguage(Operations.determinize(nfa),
                                       Operations.determinize(concat2)));
  }

  public void testGetRandomAcceptedString() throws Throwable {
    final int ITER1 = atLeast(100);
    final int ITER2 = atLeast(100);
    for(int i=0;i<ITER1;i++) {

      final RegExp re = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE);
      //System.out.println("TEST i=" + i + " re=" + re);
      final Automaton a = Operations.determinize(re.toAutomaton());
      assertFalse(Operations.isEmpty(a));

      final AutomatonTestUtil.RandomAcceptedStrings rx = new AutomatonTestUtil.RandomAcceptedStrings(a);
      for(int j=0;j<ITER2;j++) {
        //System.out.println("TEST: j=" + j);
        int[] acc = null;
        try {
          acc = rx.getRandomAcceptedString(random());
          final String s = UnicodeUtil.newString(acc, 0, acc.length);
          //a.writeDot("adot");
          assertTrue(Operations.run(a, s));
        } catch (Throwable t) {
          System.out.println("regexp: " + re);
          if (acc != null) {
            System.out.println("fail acc re=" + re + " count=" + acc.length);
            for(int k=0;k<acc.length;k++) {
              System.out.println("  " + Integer.toHexString(acc[k]));
            }
          }
          throw t;
        }
      }
    }
  }
  /**
   * tests against the original brics implementation.
   */
  public void testIsFinite() {
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      assertEquals(AutomatonTestUtil.isFiniteSlow(a), Operations.isFinite(a));
    }
  }

  /** Pass false for testRecursive if the expected strings
   *  may be too long */
  private Set<IntsRef> getFiniteStrings(Automaton a, int limit, boolean testRecursive) {
    Set<IntsRef> result = Operations.getFiniteStrings(a, limit);
    if (testRecursive) {
      assertEquals(AutomatonTestUtil.getFiniteStringsRecursive(a, limit), result);
    }
    return result;
  }
  
  /**
   * Basic test for getFiniteStrings
   */
  public void testFiniteStringsBasic() {
    Automaton a = Operations.union(Automata.makeString("dog"), Automata.makeString("duck"));
    a = MinimizationOperations.minimize(a);
    Set<IntsRef> strings = getFiniteStrings(a, -1, true);
    assertEquals(2, strings.size());
    IntsRefBuilder dog = new IntsRefBuilder();
    Util.toIntsRef(new BytesRef("dog"), dog);
    assertTrue(strings.contains(dog.get()));
    IntsRefBuilder duck = new IntsRefBuilder();
    Util.toIntsRef(new BytesRef("duck"), duck);
    assertTrue(strings.contains(duck.get()));
  }

  public void testFiniteStringsEatsStack() {
    char[] chars = new char[50000];
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString1 = new String(chars);
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString2 = new String(chars);
    Automaton a = Operations.union(Automata.makeString(bigString1), Automata.makeString(bigString2));
    Set<IntsRef> strings = getFiniteStrings(a, -1, false);
    assertEquals(2, strings.size());
    IntsRefBuilder scratch = new IntsRefBuilder();
    Util.toUTF32(bigString1.toCharArray(), 0, bigString1.length(), scratch);
    assertTrue(strings.contains(scratch.get()));
    Util.toUTF32(bigString2.toCharArray(), 0, bigString2.length(), scratch);
    assertTrue(strings.contains(scratch.get()));
  }

  public void testRandomFiniteStrings1() {

    int numStrings = atLeast(100);
    if (VERBOSE) {
      System.out.println("TEST: numStrings=" + numStrings);
    }

    Set<IntsRef> strings = new HashSet<IntsRef>();
    List<Automaton> automata = new ArrayList<>();
    IntsRefBuilder scratch = new IntsRefBuilder();
    for(int i=0;i<numStrings;i++) {
      String s = TestUtil.randomSimpleString(random(), 1, 200);
      automata.add(Automata.makeString(s));
      Util.toUTF32(s.toCharArray(), 0, s.length(), scratch);
      strings.add(scratch.toIntsRef());
      if (VERBOSE) {
        System.out.println("  add string=" + s);
      }
    }

    // TODO: we could sometimes use
    // DaciukMihovAutomatonBuilder here

    // TODO: what other random things can we do here...
    Automaton a = Operations.union(automata);
    if (random().nextBoolean()) {
      a = MinimizationOperations.minimize(a);
      if (VERBOSE) {
        System.out.println("TEST: a.minimize numStates=" + a.getNumStates());
      }
    } else if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: a.determinize");
      }
      a = Operations.determinize(a);
    } else if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: a.removeDeadStates");
      }
      a = Operations.removeDeadStates(a);
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
      Operations.getFiniteStrings(new RegExp("abc.*", RegExp.NONE).toAutomaton(), -1);
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
        Operations.getFiniteStrings(a, TestUtil.nextInt(random(), 1, 1000));
        // NOTE: cannot do this, because the method is not
        // guaranteed to detect cycles when you have a limit
        //assertTrue(Operations.isFinite(a));
      } catch (IllegalArgumentException iae) {
        assertFalse(Operations.isFinite(a));
      }
    }
  }

  public void testInvalidLimit() {
    Automaton a = AutomatonTestUtil.randomAutomaton(random());
    try {
      Operations.getFiniteStrings(a, -7);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testInvalidLimit2() {
    Automaton a = AutomatonTestUtil.randomAutomaton(random());
    try {
      Operations.getFiniteStrings(a, 0);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testSingletonNoLimit() {
    Set<IntsRef> result = Operations.getFiniteStrings(Automata.makeString("foobar"), -1);
    assertEquals(1, result.size());
    IntsRefBuilder scratch = new IntsRefBuilder();
    Util.toUTF32("foobar".toCharArray(), 0, 6, scratch);
    assertTrue(result.contains(scratch.get()));
  }

  public void testSingletonLimit1() {
    Set<IntsRef> result = Operations.getFiniteStrings(Automata.makeString("foobar"), 1);
    assertEquals(1, result.size());
    IntsRefBuilder scratch = new IntsRefBuilder();
    Util.toUTF32("foobar".toCharArray(), 0, 6, scratch);
    assertTrue(result.contains(scratch.get()));
  }
}
