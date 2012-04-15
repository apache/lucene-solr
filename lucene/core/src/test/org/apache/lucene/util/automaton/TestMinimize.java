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

import org.apache.lucene.util.LuceneTestCase;

/** 
 * This test builds some randomish NFA/DFA and minimizes them.
 */
public class TestMinimize extends LuceneTestCase {
  /** the minimal and non-minimal are compared to ensure they are the same. */
  public void test() {
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      Automaton b = a.clone();
      MinimizationOperations.minimize(b);
      assertTrue(BasicOperations.sameLanguage(a, b));
    }
  }
  
  /** compare minimized against minimized with a slower, simple impl.
   * we check not only that they are the same, but that #states/#transitions
   * are the same. */
  public void testAgainstBrzozowski() {
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      AutomatonTestUtil.minimizeSimple(a);
      Automaton b = a.clone();
      MinimizationOperations.minimize(b);
      assertTrue(BasicOperations.sameLanguage(a, b));
      assertEquals(a.getNumberOfStates(), b.getNumberOfStates());
      assertEquals(a.getNumberOfTransitions(), b.getNumberOfTransitions());
    }
  }
  
  /** n^2 space usage in Hopcroft minimization? */
  public void testMinimizeHuge() {
    new RegExp("+-*(A|.....|BC)*]", RegExp.NONE).toAutomaton();
  }
}
