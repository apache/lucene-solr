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


import java.util.*;

import org.apache.lucene.util.*;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

public class TestOperations extends LuceneTestCase {
  /** Test string union. */
  public void testStringUnion() {
    List<BytesRef> strings = new ArrayList<>();
    for (int i = RandomNumbers.randomIntBetween(random(), 0, 1000); --i >= 0;) {
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
    return Operations.determinize(Operations.union(Arrays.asList(eachIndividual)), DEFAULT_DETERMINIZE_WORK_LIMIT);
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
    assertTrue(Operations.sameLanguage(Operations.determinize(concat1, 100),
                                       Operations.determinize(concat2, 100)));
    assertTrue(Operations.sameLanguage(Operations.determinize(nfa, 100),
                                       Operations.determinize(concat1, 100)));
    assertTrue(Operations.sameLanguage(Operations.determinize(nfa, 100),
                                       Operations.determinize(concat2, 100)));
  }

  public void testGetRandomAcceptedString() throws Throwable {
    final int ITER1 = atLeast(100);
    final int ITER2 = atLeast(100);
    for(int i=0;i<ITER1;i++) {

      final RegExp re = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE);
      //System.out.println("TEST i=" + i + " re=" + re);
      final Automaton a = Operations.determinize(re.toAutomaton(), DEFAULT_DETERMINIZE_WORK_LIMIT);
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

  public void testIsFiniteEatsStack() {
    char[] chars = new char[50000];
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString1 = new String(chars);
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString2 = new String(chars);
    Automaton a = Operations.union(Automata.makeString(bigString1), Automata.makeString(bigString2));
    IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> Operations.isFinite(a));
    assertTrue(exc.getMessage().contains("input automaton is too large"));
  }

  public void testTopoSortEatsStack() {
    char[] chars = new char[50000];
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString1 = new String(chars);
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString2 = new String(chars);
    Automaton a = Operations.union(Automata.makeString(bigString1), Automata.makeString(bigString2));
    IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> Operations.topoSortStates(a));
    assertTrue(exc.getMessage().contains("input automaton is too large"));
  }

  /**
   * Returns the set of all accepted strings.
   *
   * This method exist just to ease testing.
   * For production code directly use {@link FiniteStringsIterator} instead.
   *
   * @see FiniteStringsIterator
   */
  public static Set<IntsRef> getFiniteStrings(Automaton a) {
    return getFiniteStrings(new FiniteStringsIterator(a));
  }

  /**
   * Returns the set of accepted strings, up to at most <code>limit</code> strings.
   *
   * This method exist just to ease testing.
   * For production code directly use {@link LimitedFiniteStringsIterator} instead.
   *
   * @see LimitedFiniteStringsIterator
   */
  public static Set<IntsRef> getFiniteStrings(Automaton a, int limit) {
    return getFiniteStrings(new LimitedFiniteStringsIterator(a, limit));
  }

  /**
   * Get all finite strings of an iterator.
   */
  private static Set<IntsRef> getFiniteStrings(FiniteStringsIterator iterator) {
    Set<IntsRef> result = new HashSet<>();
    for (IntsRef finiteString; (finiteString = iterator.next()) != null;) {
      result.add(IntsRef.deepCopyOf(finiteString));
    }

    return result;
  }
}
