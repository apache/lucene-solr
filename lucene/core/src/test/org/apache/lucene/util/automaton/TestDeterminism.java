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


import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

/**
 * Not completely thorough, but tries to test determinism correctness
 * somewhat randomly.
 */
public class TestDeterminism extends LuceneTestCase {
  
  /** test a bunch of random regular expressions */
  public void testRegexps() throws Exception {
      int num = atLeast(500);
      for (int i = 0; i < num; i++) {
        assertAutomaton(new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE).toAutomaton());
      }
  }
  
  /** test against a simple, unoptimized det */
  public void testAgainstSimple() throws Exception {
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      a = AutomatonTestUtil.determinizeSimple(a);
      Automaton b = Operations.determinize(a, Integer.MAX_VALUE);
      // TODO: more verifications possible?
      assertTrue(Operations.sameLanguage(a, b));
    }
  }
  
  private static void assertAutomaton(Automaton a) {
    a = Operations.determinize(Operations.removeDeadStates(a), DEFAULT_DETERMINIZE_WORK_LIMIT);

    // complement(complement(a)) = a
    Automaton equivalent = Operations.complement(Operations.complement(a,
      DEFAULT_DETERMINIZE_WORK_LIMIT), DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertTrue(Operations.sameLanguage(a, equivalent));
    
    // a union a = a
    equivalent = Operations.determinize(Operations.removeDeadStates(Operations.union(a, a)),
      DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertTrue(Operations.sameLanguage(a, equivalent));
    
    // a intersect a = a
    equivalent = Operations.determinize(Operations.removeDeadStates(Operations.intersection(a, a)),
      DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertTrue(Operations.sameLanguage(a, equivalent));
    
    // a minus a = empty
    Automaton empty = Operations.minus(a, a, DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertTrue(Operations.isEmpty(empty));
    
    // as long as don't accept the empty string
    // then optional(a) - empty = a
    if (!Operations.run(a, "")) {
      //System.out.println("test " + a);
      Automaton optional = Operations.optional(a);
      //System.out.println("optional " + optional);
      equivalent = Operations.minus(optional, Automata.makeEmptyString(),
        DEFAULT_DETERMINIZE_WORK_LIMIT);
      //System.out.println("equiv " + equivalent);
      assertTrue(Operations.sameLanguage(a, equivalent));
    }
  } 
}
