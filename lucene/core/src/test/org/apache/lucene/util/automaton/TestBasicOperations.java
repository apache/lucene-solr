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
import org.apache.lucene.util.UnicodeUtil;

public class TestBasicOperations extends LuceneTestCase { 
  /** Test optimization to concatenate() */
  public void testSingletonConcatenate() {
    Automaton singleton = BasicAutomata.makeString("prefix");
    Automaton expandedSingleton = singleton.cloneExpanded();
    Automaton other = BasicAutomata.makeCharRange('5', '7');
    Automaton concat = BasicOperations.concatenate(singleton, other);
    assertTrue(concat.isDeterministic());
    assertTrue(BasicOperations.sameLanguage(BasicOperations.concatenate(expandedSingleton, other), concat));
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
    assertTrue(BasicOperations.sameLanguage(BasicOperations.concatenate(expandedSingleton, nfa), concat));
  }
  
  /** Test optimization to concatenate() with empty String */
  public void testEmptySingletonConcatenate() {
    Automaton singleton = BasicAutomata.makeString("");
    Automaton expandedSingleton = singleton.cloneExpanded();
    Automaton other = BasicAutomata.makeCharRange('5', '7');
    Automaton concat1 = BasicOperations.concatenate(expandedSingleton, other);
    Automaton concat2 = BasicOperations.concatenate(singleton, other);
    assertTrue(concat2.isDeterministic());
    assertTrue(BasicOperations.sameLanguage(concat1, concat2));
    assertTrue(BasicOperations.sameLanguage(other, concat1));
    assertTrue(BasicOperations.sameLanguage(other, concat2));
  }
  
  /** Test concatenation with empty language returns empty */
  public void testEmptyLanguageConcatenate() {
    Automaton a = BasicAutomata.makeString("a");
    Automaton concat = BasicOperations.concatenate(a, BasicAutomata.makeEmpty());
    assertTrue(BasicOperations.isEmpty(concat));
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
    assertTrue(BasicOperations.sameLanguage(concat1, concat2));
    assertTrue(BasicOperations.sameLanguage(nfa, concat1));
    assertTrue(BasicOperations.sameLanguage(nfa, concat2));
  }

  /** Test singletons work correctly */
  public void testSingleton() {
    Automaton singleton = BasicAutomata.makeString("foobar");
    Automaton expandedSingleton = singleton.cloneExpanded();
    assertTrue(BasicOperations.sameLanguage(singleton, expandedSingleton));
    
    singleton = BasicAutomata.makeString("\ud801\udc1c");
    expandedSingleton = singleton.cloneExpanded();
    assertTrue(BasicOperations.sameLanguage(singleton, expandedSingleton));
  }

  public void testGetRandomAcceptedString() throws Throwable {
    final int ITER1 = atLeast(100);
    final int ITER2 = atLeast(100);
    for(int i=0;i<ITER1;i++) {

      final RegExp re = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE);
      final Automaton a = re.toAutomaton();
      assertFalse(BasicOperations.isEmpty(a));

      final AutomatonTestUtil.RandomAcceptedStrings rx = new AutomatonTestUtil.RandomAcceptedStrings(a);
      for(int j=0;j<ITER2;j++) {
        int[] acc = null;
        try {
          acc = rx.getRandomAcceptedString(random());
          final String s = UnicodeUtil.newString(acc, 0, acc.length);
          assertTrue(BasicOperations.run(a, s));
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
}
