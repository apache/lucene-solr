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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.AutomatonTestUtil.RandomAcceptedStringsLight;

public class TestLightAutomaton extends LuceneTestCase {

  public void testBasic() throws Exception {
    LightAutomaton a = new LightAutomaton();
    int start = a.createState();
    int x = a.createState();
    int y = a.createState();
    int end = a.createState();
    a.setAccept(end, true);

    a.addTransition(start, x, 'a', 'a');
    a.addTransition(start, end, 'd', 'd');
    a.addTransition(x, y, 'b', 'b');
    a.addTransition(y, end, 'c', 'c');
    a.finish();
  }

  public void testReduceBasic() throws Exception {
    LightAutomaton a = new LightAutomaton();
    int start = a.createState();
    int end = a.createState();
    a.setAccept(end, true);
    // Should collapse to a-b:
    a.addTransition(start, end, 'a', 'a');
    a.addTransition(start, end, 'b', 'b');
    a.addTransition(start, end, 'm', 'm');
    // Should collapse to x-y:
    a.addTransition(start, end, 'x', 'x');
    a.addTransition(start, end, 'y', 'y');

    a.finish();
    assertEquals(3, a.getNumTransitions(start));
    LightAutomaton.Transition scratch = new LightAutomaton.Transition();
    a.initTransition(start, scratch);
    a.getNextTransition(scratch);
    assertEquals('a', scratch.min);
    assertEquals('b', scratch.max);
    a.getNextTransition(scratch);
    assertEquals('m', scratch.min);
    assertEquals('m', scratch.max);
    a.getNextTransition(scratch);
    assertEquals('x', scratch.min);
    assertEquals('y', scratch.max);
  }

  public void testSameLanguage() throws Exception {
    LightAutomaton a1 = BasicAutomata.makeStringLight("foobar");
    LightAutomaton a2 = BasicOperations.concatenateLight(
                            BasicAutomata.makeStringLight("foo"),
                            BasicAutomata.makeStringLight("bar"));
    assertTrue(BasicOperations.sameLanguage(a1, a2));
  }

  public void testCommonPrefix() throws Exception {
    LightAutomaton a = BasicOperations.concatenateLight(
                            BasicAutomata.makeStringLight("foobar"),
                            BasicAutomata.makeAnyStringLight());
    assertEquals("foobar", SpecialOperations.getCommonPrefix(a));
  }

  public void testConcatenate1() throws Exception {
    LightAutomaton a = BasicOperations.concatenateLight(
                            BasicAutomata.makeStringLight("m"),
                            BasicAutomata.makeAnyStringLight());
    assertTrue(BasicOperations.run(a, "m"));
    assertTrue(BasicOperations.run(a, "me"));
    assertTrue(BasicOperations.run(a, "me too"));
  }

  public void testConcatenate2() throws Exception {
    LightAutomaton a = BasicOperations.concatenateLight(Arrays.asList(
                            BasicAutomata.makeStringLight("m"),
                            BasicAutomata.makeAnyStringLight(),
                            BasicAutomata.makeStringLight("n"),
                            BasicAutomata.makeAnyStringLight()));
    a = BasicOperations.determinize(a);
    assertTrue(BasicOperations.run(a, "mn"));
    assertTrue(BasicOperations.run(a, "mone"));
    assertFalse(BasicOperations.run(a, "m"));
  }

  public void testUnion1() throws Exception {
    LightAutomaton a = BasicOperations.unionLight(Arrays.asList(
                            BasicAutomata.makeStringLight("foobar"),
                            BasicAutomata.makeStringLight("barbaz")));
    a = BasicOperations.determinize(a);
    assertTrue(BasicOperations.run(a, "foobar"));
    assertTrue(BasicOperations.run(a, "barbaz"));

    // nocommit test getFinitStrings count == 2
  }

  public void testUnion2() throws Exception {
    LightAutomaton a = BasicOperations.unionLight(Arrays.asList(
                            BasicAutomata.makeStringLight("foobar"),
                            BasicAutomata.makeStringLight(""),
                            BasicAutomata.makeStringLight("barbaz")));
    a = BasicOperations.determinize(a);
    assertTrue(BasicOperations.run(a, "foobar"));
    assertTrue(BasicOperations.run(a, "barbaz"));
    assertTrue(BasicOperations.run(a, ""));

    // nocommit test getFinitStrings count == 3
  }

  public void testMinimizeSimple() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("foobar");
    //a.writeDot("a");
    LightAutomaton aMin = MinimizationOperationsLight.minimize(a);
    //aMin.writeDot("aMin");

    assertTrue(BasicOperations.sameLanguage(a, aMin));
  }

  public void testMinimize2() throws Exception {
    LightAutomaton a = BasicOperations.unionLight(Arrays.asList(BasicAutomata.makeStringLight("foobar"),
                                                                BasicAutomata.makeStringLight("boobar")));
    LightAutomaton aMin = MinimizationOperationsLight.minimize(a);
    assertTrue(BasicOperations.sameLanguage(BasicOperations.determinize(a), aMin));
  }

  public void testReverse() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("foobar");
    LightAutomaton ra = SpecialOperations.reverse(a);
    LightAutomaton a2 = BasicOperations.determinize(SpecialOperations.reverse(ra));
    
    assertTrue(BasicOperations.sameLanguage(a, a2));
  }

  public void testOptional() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("foobar");
    LightAutomaton a2 = BasicOperations.optionalLight(a);
    a2 = BasicOperations.determinize(a2);
    
    assertTrue(BasicOperations.run(a, "foobar"));
    assertFalse(BasicOperations.run(a, ""));
    assertTrue(BasicOperations.run(a2, "foobar"));
    assertTrue(BasicOperations.run(a2, ""));
  }

  public void testRepeatAny() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("zee");
    LightAutomaton a2 = BasicOperations.determinize(BasicOperations.repeatLight(a));
    assertTrue(BasicOperations.run(a2, ""));
    assertTrue(BasicOperations.run(a2, "zee"));    
    assertTrue(BasicOperations.run(a2, "zeezee"));
    assertTrue(BasicOperations.run(a2, "zeezeezee"));
  }

  public void testRepeatMin() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("zee");
    LightAutomaton a2 = BasicOperations.determinize(BasicOperations.repeatLight(a, 2));
    assertFalse(BasicOperations.run(a2, ""));
    assertFalse(BasicOperations.run(a2, "zee"));    
    assertTrue(BasicOperations.run(a2, "zeezee"));
    assertTrue(BasicOperations.run(a2, "zeezeezee"));
  }

  public void testRepeatMinMax1() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("zee");
    LightAutomaton a2 = BasicOperations.determinize(BasicOperations.repeatLight(a, 0, 2));
    assertTrue(BasicOperations.run(a2, ""));
    assertTrue(BasicOperations.run(a2, "zee"));    
    assertTrue(BasicOperations.run(a2, "zeezee"));
    assertFalse(BasicOperations.run(a2, "zeezeezee"));
  }

  public void testRepeatMinMax2() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("zee");
    LightAutomaton a2 = BasicOperations.determinize(BasicOperations.repeatLight(a, 2, 4));
    assertFalse(BasicOperations.run(a2, ""));
    assertFalse(BasicOperations.run(a2, "zee"));    
    assertTrue(BasicOperations.run(a2, "zeezee"));
    assertTrue(BasicOperations.run(a2, "zeezeezee"));
    assertTrue(BasicOperations.run(a2, "zeezeezeezee"));
    assertFalse(BasicOperations.run(a2, "zeezeezeezeezee"));
  }

  public void testComplement() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("zee");
    LightAutomaton a2 = BasicOperations.determinize(BasicOperations.complementLight(a));
    assertTrue(BasicOperations.run(a2, ""));
    assertFalse(BasicOperations.run(a2, "zee"));    
    assertTrue(BasicOperations.run(a2, "zeezee"));
    assertTrue(BasicOperations.run(a2, "zeezeezee"));
  }

  public void testInterval() throws Exception {
    LightAutomaton a = BasicOperations.determinize(BasicAutomata.makeIntervalLight(17, 100, 3));
    assertFalse(BasicOperations.run(a, ""));
    assertTrue(BasicOperations.run(a, "017"));
    assertTrue(BasicOperations.run(a, "100"));
    assertTrue(BasicOperations.run(a, "073"));
  }

  public void testCommonSuffix() throws Exception {
    LightAutomaton a = new LightAutomaton();
    int init = a.createState();
    int fini = a.createState();
    a.setAccept(init, true);
    a.setAccept(fini, true);
    a.addTransition(init, fini, 'm');
    a.addTransition(fini, fini, 'm');
    a.finish();
    assertEquals(0, SpecialOperations.getCommonSuffixBytesRef(a).length);
  }

  public void testReverseRandom1() throws Exception {
    int ITERS = atLeast(100);
    for(int i=0;i<ITERS;i++) {
      LightAutomaton a = AutomatonTestUtil.randomAutomaton(random());
      LightAutomaton ra = SpecialOperations.reverse(a);
      LightAutomaton rra = SpecialOperations.reverse(ra);
      assertTrue(BasicOperations.sameLanguage(BasicOperations.determinize(a),
                                              BasicOperations.determinize(rra)));
    }
  }

  public void testReverseRandom2() throws Exception {
    int ITERS = atLeast(100);
    for(int iter=0;iter<ITERS;iter++) {
      //System.out.println("TEST: iter=" + iter);
      LightAutomaton a = BasicOperations.removeDeadTransitions(AutomatonTestUtil.randomAutomaton(random()));
      LightAutomaton ra = SpecialOperations.reverse(a);
      LightAutomaton rda = BasicOperations.determinize(ra);

      if (a.isEmpty()) {
        assertTrue(rda.isEmpty());
        continue;
      }

      RandomAcceptedStringsLight rasl = new RandomAcceptedStringsLight(a);

      for(int iter2=0;iter2<20;iter2++) {
        // Find string accepted by original automaton
        int[] s = rasl.getRandomAcceptedString(random());

        // Reverse it
        for(int j=0;j<s.length/2;j++) {
          int x = s[j];
          s[j] = s[s.length-j-1];
          s[s.length-j-1] = x;
        }
        //System.out.println("TEST:   iter2=" + iter2 + " s=" + Arrays.toString(s));

        // Make sure reversed automaton accepts it
        assertTrue(BasicOperations.run(rda, new IntsRef(s, 0, s.length)));
      }
    }
  }

  public void testAnyStringEmptyString() throws Exception {
    LightAutomaton a = BasicOperations.determinize(BasicAutomata.makeAnyStringLight());
    assertTrue(BasicOperations.run(a, ""));
  }


  public void testRemoveDeadTransitionsEmpty() throws Exception {
    LightAutomaton a = BasicAutomata.makeEmptyLight();
    LightAutomaton a2 = BasicOperations.removeDeadTransitions(a);
    assertTrue(a2.isEmpty());
  }

  public void testInvalidAddTransition() throws Exception {
    LightAutomaton a = new LightAutomaton();
    int s1 = a.createState();
    int s2 = a.createState();
    a.addTransition(s1, s2, 'a');
    a.addTransition(s2, s2, 'a');
    try {
      a.addTransition(s1, s2, 'b');
      fail("didn't hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  public void testBuilderRandom() throws Exception {
    int ITERS = atLeast(100);
    for(int iter=0;iter<ITERS;iter++) {
      LightAutomaton a = AutomatonTestUtil.randomAutomaton(random());

      // Just get all transitions, shuffle, and build a new automaton with the same transitions:
      List<LightAutomaton.Transition> allTrans = new ArrayList<>();
      int numStates = a.getNumStates();
      for(int s=0;s<numStates;s++) {
        int count = a.getNumTransitions(s);
        for(int i=0;i<count;i++) {
          LightAutomaton.Transition t = new LightAutomaton.Transition();
          a.getTransition(s, i, t);
          allTrans.add(t);
        }
      }

      LightAutomaton.Builder builder = new LightAutomaton.Builder();
      for(int i=0;i<numStates;i++) {
        int s = builder.createState();
        builder.setAccept(s, a.isAccept(s));
      }

      Collections.shuffle(allTrans, random());
      for(LightAutomaton.Transition t : allTrans) {
        builder.addTransition(t.source, t.dest, t.min, t.max);
      }

      assertTrue(BasicOperations.sameLanguage(
                    BasicOperations.determinize(a),
                    BasicOperations.determinize(builder.finish())));
      
    }
  }

  // nocommit testMinus
  public void testMinus() throws Exception {
    LightAutomaton a1 = BasicAutomata.makeStringLight("foobar");
    LightAutomaton a2 = BasicAutomata.makeStringLight("boobar");
    LightAutomaton a3 = BasicAutomata.makeStringLight("beebar");
    LightAutomaton a = BasicOperations.unionLight(Arrays.asList(a1, a2, a3));
    if (random().nextBoolean()) {
      a = BasicOperations.determinize(a);
    } else if (random().nextBoolean()) {
      a = MinimizationOperationsLight.minimize(a);
    }

    LightAutomaton a4 = BasicOperations.determinize(BasicOperations.minusLight(a, a2));
    
    assertTrue(BasicOperations.run(a4, "foobar"));
    assertFalse(BasicOperations.run(a4, "boobar"));
    assertTrue(BasicOperations.run(a4, "beebar"));

    // nocommit test getFinitStrings count == 2

    a4 = BasicOperations.determinize(BasicOperations.minusLight(a4, a1));
    assertFalse(BasicOperations.run(a4, "foobar"));
    assertFalse(BasicOperations.run(a4, "boobar"));
    assertTrue(BasicOperations.run(a4, "beebar"));

    a4 = BasicOperations.determinize(BasicOperations.minusLight(a4, a3));
    assertFalse(BasicOperations.run(a4, "foobar"));
    assertFalse(BasicOperations.run(a4, "boobar"));
    assertFalse(BasicOperations.run(a4, "beebar"));
  }

  // nocommit
  //public void testWildcard() throws Exception {
  //WildcardQuery.toAutomaton(new Term("foo", "bar*")).writeDot("wq");
  //}

  // nocommit more tests ... it's an algebra

  // nocommit random test for testInterval if we don't have one already
}
