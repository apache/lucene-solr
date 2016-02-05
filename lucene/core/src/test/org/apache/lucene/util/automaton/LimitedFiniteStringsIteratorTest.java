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


import java.util.List;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.fst.Util;

import static org.apache.lucene.util.automaton.FiniteStringsIteratorTest.getFiniteStrings;

/**
 * Test for {@link FiniteStringsIterator}.
 */
public class LimitedFiniteStringsIteratorTest extends LuceneTestCase {
 public void testRandomFiniteStrings() {
    // Just makes sure we can run on any random finite
    // automaton:
    int iters = atLeast(100);
    for(int i=0;i<iters;i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      try {
        // Must pass a limit because the random automaton
        // can accept MANY strings:
        getFiniteStrings(new LimitedFiniteStringsIterator(a, TestUtil.nextInt(random(), 1, 1000)));
        // NOTE: cannot do this, because the method is not
        // guaranteed to detect cycles when you have a limit
        //assertTrue(Operations.isFinite(a));
      } catch (IllegalArgumentException iae) {
        assertFalse(Operations.isFinite(a));
      }
    }
  }

  public void testInvalidLimitNegative() {
    Automaton a = AutomatonTestUtil.randomAutomaton(random());
    try {
      new LimitedFiniteStringsIterator(a, -7);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testInvalidLimitNull() {
    Automaton a = AutomatonTestUtil.randomAutomaton(random());
    try {
      new LimitedFiniteStringsIterator(a, 0);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  public void testSingleton() {
    Automaton a = Automata.makeString("foobar");
    List<IntsRef> actual = getFiniteStrings(new LimitedFiniteStringsIterator(a, 1));
    assertEquals(1, actual.size());
    IntsRefBuilder scratch = new IntsRefBuilder();
    Util.toUTF32("foobar".toCharArray(), 0, 6, scratch);
    assertTrue(actual.contains(scratch.get()));
  }

  public void testLimit() {
    Automaton a = Operations.union(Automata.makeString("foo"), Automata.makeString("bar"));

    // Test without limit
    FiniteStringsIterator withoutLimit = new LimitedFiniteStringsIterator(a, -1);
    assertEquals(2, getFiniteStrings(withoutLimit).size());

    // Test with limit
    FiniteStringsIterator withLimit = new LimitedFiniteStringsIterator(a, 1);
    assertEquals(1, getFiniteStrings(withLimit).size());
  }

  public void testSize() {
    Automaton a = Operations.union(Automata.makeString("foo"), Automata.makeString("bar"));
    LimitedFiniteStringsIterator iterator = new LimitedFiniteStringsIterator(a, -1);
    List<IntsRef> actual = getFiniteStrings(iterator);
    assertEquals(2, actual.size());
    assertEquals(2, iterator.size());
  }
}
