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
package org.apache.lucene.util.automaton;


import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.fst.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_MAX_DETERMINIZED_STATES;

/**
 * Test for {@link FiniteStringsIterator}.
 */
public class FiniteStringsIteratorTest extends LuceneTestCase {
  public void testRandomFiniteStrings1() {
    int numStrings = atLeast(100);
    if (VERBOSE) {
      System.out.println("TEST: numStrings=" + numStrings);
    }

    Set<IntsRef> strings = new HashSet<>();
    List<Automaton> automata = new ArrayList<>();
    IntsRefBuilder scratch = new IntsRefBuilder();
    for(int i=0;i<numStrings;i++) {
      String s = TestUtil.randomSimpleString(random(), 1, 200);
      Util.toUTF32(s.toCharArray(), 0, s.length(), scratch);
      if (strings.add(scratch.toIntsRef())) {
        automata.add(Automata.makeString(s));
        if (VERBOSE) {
          System.out.println("  add string=" + s);
        }
      }
    }

    // TODO: we could sometimes use
    // DaciukMihovAutomatonBuilder here

    // TODO: what other random things can we do here...
    Automaton a = Operations.union(automata);
    if (random().nextBoolean()) {
      a = MinimizationOperations.minimize(a, 1000000);
      if (VERBOSE) {
        System.out.println("TEST: a.minimize numStates=" + a.getNumStates());
      }
    } else if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: a.determinize");
      }
      a = Operations.determinize(a, 1000000);
    } else if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: a.removeDeadStates");
      }
      a = Operations.removeDeadStates(a);
    }

    FiniteStringsIterator iterator = new FiniteStringsIterator(a);
    List<IntsRef> actual = getFiniteStrings(iterator);
    assertFiniteStringsRecursive(a, actual);

    if (!strings.equals(new HashSet<>(actual))) {
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

  /**
   * Basic test for getFiniteStrings
   */
  public void testFiniteStringsBasic() {
    Automaton a = Operations.union(Automata.makeString("dog"), Automata.makeString("duck"));
    a = MinimizationOperations.minimize(a, DEFAULT_MAX_DETERMINIZED_STATES);
    FiniteStringsIterator iterator = new FiniteStringsIterator(a);
    List<IntsRef> actual = getFiniteStrings(iterator);
    assertFiniteStringsRecursive(a, actual);
    assertEquals(2, actual.size());
    IntsRefBuilder dog = new IntsRefBuilder();
    Util.toIntsRef(new BytesRef("dog"), dog);
    assertTrue(actual.contains(dog.get()));
    IntsRefBuilder duck = new IntsRefBuilder();
    Util.toIntsRef(new BytesRef("duck"), duck);
    assertTrue(actual.contains(duck.get()));
  }

  public void testFiniteStringsEatsStack() {
    char[] chars = new char[50000];
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString1 = new String(chars);
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString2 = new String(chars);
    Automaton a = Operations.union(Automata.makeString(bigString1), Automata.makeString(bigString2));
    FiniteStringsIterator iterator = new FiniteStringsIterator(a);
    List<IntsRef> actual = getFiniteStrings(iterator);
    assertEquals(2, actual.size());
    IntsRefBuilder scratch = new IntsRefBuilder();
    Util.toUTF32(bigString1.toCharArray(), 0, bigString1.length(), scratch);
    assertTrue(actual.contains(scratch.get()));
    Util.toUTF32(bigString2.toCharArray(), 0, bigString2.length(), scratch);
    assertTrue(actual.contains(scratch.get()));
  }


  public void testWithCycle() throws Exception {
    try {
      Automaton a = new RegExp("abc.*", RegExp.NONE).toAutomaton();
      FiniteStringsIterator iterator = new FiniteStringsIterator(a);
      getFiniteStrings(iterator);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testSingletonNoLimit() {
    Automaton a = Automata.makeString("foobar");
    FiniteStringsIterator iterator = new FiniteStringsIterator(a);
    List<IntsRef> actual = getFiniteStrings(iterator);
    assertEquals(1, actual.size());
    IntsRefBuilder scratch = new IntsRefBuilder();
    Util.toUTF32("foobar".toCharArray(), 0, 6, scratch);
    assertTrue(actual.contains(scratch.get()));
  }

  public void testShortAccept() {
    Automaton a = Operations.union(Automata.makeString("x"), Automata.makeString("xy"));
    a = MinimizationOperations.minimize(a, DEFAULT_MAX_DETERMINIZED_STATES);
    FiniteStringsIterator iterator = new FiniteStringsIterator(a);
    List<IntsRef> actual = getFiniteStrings(iterator);
    assertEquals(2, actual.size());
    IntsRefBuilder x = new IntsRefBuilder();
    Util.toIntsRef(new BytesRef("x"), x);
    assertTrue(actual.contains(x.get()));
    IntsRefBuilder xy = new IntsRefBuilder();
    Util.toIntsRef(new BytesRef("xy"), xy);
    assertTrue(actual.contains(xy.get()));
  }

  public void testSingleString() {
    Automaton a = new Automaton();
    int start = a.createState();
    int end = a.createState();
    a.setAccept(end, true);
    a.addTransition(start, end, 'a', 'a');
    a.finishState();
    Set<IntsRef> accepted = TestOperations.getFiniteStrings(a);
    assertEquals(1, accepted.size());
    IntsRefBuilder intsRef = new IntsRefBuilder();
    intsRef.append('a');
    assertTrue(accepted.contains(intsRef.toIntsRef()));
  }

  /**
   * All strings generated by the iterator.
   */
  static List<IntsRef> getFiniteStrings(FiniteStringsIterator iterator) {
    List<IntsRef> result = new ArrayList<>();
    for (IntsRef finiteString; (finiteString = iterator.next()) != null;) {
      result.add(IntsRef.deepCopyOf(finiteString));
    }

    return result;
  }

  /**
   * Check that strings the automaton returns are as expected.
   *
   * @param automaton Automaton.
   * @param actual Strings generated by automaton.
   */
  private void assertFiniteStringsRecursive(Automaton automaton, List<IntsRef> actual) {
    Set<IntsRef> expected = AutomatonTestUtil.getFiniteStringsRecursive(automaton, -1);
    // Check that no string is emitted twice.
    assertEquals(expected.size(), actual.size());
    assertEquals(expected, new HashSet<>(actual));
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
}
