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
 * the minimal and non-minimal are compared to ensure they are the same.
 */
public class TestMinimize extends LuceneTestCase {
  public void test() {
    int num = 10000 * RANDOM_MULTIPLIER;
    for (int i = 0; i < num; i++) {
      Automaton a = randomishAutomaton();
      Automaton b = a.clone();
      MinimizationOperations.minimize(b);
      assertTrue(BasicOperations.sameLanguage(a, b));
    }
  }
  
  private Automaton randomishAutomaton() {
    // get two random Automata from regexps
    Automaton a1 = AutomatonTestUtil.randomRegexp(random).toAutomaton();
    if (random.nextBoolean())
      a1 = BasicOperations.complement(a1);
    
    Automaton a2 = AutomatonTestUtil.randomRegexp(random).toAutomaton();
    if (random.nextBoolean()) 
      a2 = BasicOperations.complement(a2);
    
    // combine them in random ways
    switch(random.nextInt(3)) {
      case 0: return BasicOperations.concatenate(a1, a2);
      case 1: return BasicOperations.union(a1, a2);
      default: return BasicOperations.minus(a1, a2);
    }
  }
}
