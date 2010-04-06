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

public class TestBasicOperations extends LuceneTestCase { 
  /** Test optimization to concatenate() */
  public void testSingletonConcatenate() {
    Automaton singleton = BasicAutomata.makeString("prefix");
    Automaton expandedSingleton = singleton.cloneExpanded();
    Automaton other = BasicAutomata.makeCharRange('5', '7');
    Automaton concat = BasicOperations.concatenate(singleton, other);
    assertTrue(concat.isDeterministic());
    assertEquals(BasicOperations.concatenate(expandedSingleton, other), concat);
  }
  
  /** Test optimization to concatenate() to an NFA */
  public void testSingletonNFAConcatenate() {
    Automaton singleton = BasicAutomata.makeString("prefix");
    Automaton expandedSingleton = singleton.cloneExpanded();
    // an NFA (two transitions for 't' from initial state)
    Automaton nfa = BasicOperations.union(BasicAutomata.makeString("this"),
        BasicAutomata.makeString("three"));
    Automaton concat = BasicOperations.concatenate(singleton, nfa);
    assertFalse(concat.isDeterministic());
    assertEquals(BasicOperations.concatenate(expandedSingleton, nfa), concat);
  }
  
  /** Test optimization to concatenate() with empty String */
  public void testEmptySingletonConcatenate() {
    Automaton singleton = BasicAutomata.makeString("");
    Automaton expandedSingleton = singleton.cloneExpanded();
    Automaton other = BasicAutomata.makeCharRange('5', '7');
    Automaton concat1 = BasicOperations.concatenate(expandedSingleton, other);
    Automaton concat2 = BasicOperations.concatenate(singleton, other);
    assertTrue(concat2.isDeterministic());
    assertEquals(concat1, concat2);
    assertEquals(other, concat1);
    assertEquals(other, concat2);
  }
  
  /** Test optimization to concatenate() with empty String to an NFA */
  public void testEmptySingletonNFAConcatenate() {
    Automaton singleton = BasicAutomata.makeString("");
    Automaton expandedSingleton = singleton.cloneExpanded();
    // an NFA (two transitions for 't' from initial state)
    Automaton nfa = BasicOperations.union(BasicAutomata.makeString("this"),
        BasicAutomata.makeString("three"));
    Automaton concat1 = BasicOperations.concatenate(expandedSingleton, nfa);
    Automaton concat2 = BasicOperations.concatenate(singleton, nfa);
    assertFalse(concat2.isDeterministic());
    assertEquals(concat1, concat2);
    assertEquals(nfa, concat1);
    assertEquals(nfa, concat2);
  }
}
