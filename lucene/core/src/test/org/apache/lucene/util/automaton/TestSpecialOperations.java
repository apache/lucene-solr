package org.apache.lucene.util.automaton;

import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.fst.Util;

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
  
  /**
   * Basic test for getFiniteStrings
   */
  public void testFiniteStrings() {
    Automaton a = BasicOperations.union(BasicAutomata.makeString("dog"), BasicAutomata.makeString("duck"));
    MinimizationOperations.minimize(a);
    Set<IntsRef> strings = SpecialOperations.getFiniteStrings(a, -1);
    assertEquals(2, strings.size());
    IntsRef dog = new IntsRef();
    Util.toIntsRef(new BytesRef("dog"), dog);
    assertTrue(strings.contains(dog));
    IntsRef duck = new IntsRef();
    Util.toIntsRef(new BytesRef("duck"), duck);
    assertTrue(strings.contains(duck));
  }
}
