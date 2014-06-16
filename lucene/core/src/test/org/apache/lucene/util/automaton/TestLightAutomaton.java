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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.AutomatonTestUtil.RandomAcceptedStrings;
import org.apache.lucene.util.fst.Util;

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
    a.finishState();
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

    a.finishState();
    assertEquals(3, a.getNumTransitions(start));
    Transition scratch = new Transition();
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
    LightAutomaton a2 = BasicOperations.removeDeadStates(BasicOperations.concatenateLight(
                            BasicAutomata.makeStringLight("foo"),
                            BasicAutomata.makeStringLight("bar")));
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
    assertFalse(SpecialOperations.isFinite(a));
  }

  public void testUnion1() throws Exception {
    LightAutomaton a = BasicOperations.unionLight(Arrays.asList(
                            BasicAutomata.makeStringLight("foobar"),
                            BasicAutomata.makeStringLight("barbaz")));
    a = BasicOperations.determinize(a);
    assertTrue(BasicOperations.run(a, "foobar"));
    assertTrue(BasicOperations.run(a, "barbaz"));

    assertMatches(a, "foobar", "barbaz");
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

    assertMatches(a, "", "foobar", "barbaz");
  }

  public void testMinimizeSimple() throws Exception {
    LightAutomaton a = BasicAutomata.makeStringLight("foobar");
    LightAutomaton aMin = MinimizationOperationsLight.minimize(a);

    assertTrue(BasicOperations.sameLanguage(a, aMin));
  }

  public void testMinimize2() throws Exception {
    LightAutomaton a = BasicOperations.unionLight(Arrays.asList(BasicAutomata.makeStringLight("foobar"),
                                                                BasicAutomata.makeStringLight("boobar")));
    LightAutomaton aMin = MinimizationOperationsLight.minimize(a);
    assertTrue(BasicOperations.sameLanguage(BasicOperations.determinize(BasicOperations.removeDeadStates(a)), aMin));
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
    a.finishState();
    assertEquals(0, SpecialOperations.getCommonSuffixBytesRef(a).length);
  }

  public void testReverseRandom1() throws Exception {
    int ITERS = atLeast(100);
    for(int i=0;i<ITERS;i++) {
      LightAutomaton a = AutomatonTestUtil.randomAutomaton(random());
      LightAutomaton ra = SpecialOperations.reverse(a);
      LightAutomaton rra = SpecialOperations.reverse(ra);
      assertTrue(BasicOperations.sameLanguage(BasicOperations.determinize(BasicOperations.removeDeadStates(a)),
                                              BasicOperations.determinize(BasicOperations.removeDeadStates(rra))));
    }
  }

  public void testReverseRandom2() throws Exception {
    int ITERS = atLeast(100);
    for(int iter=0;iter<ITERS;iter++) {
      //System.out.println("TEST: iter=" + iter);
      LightAutomaton a = AutomatonTestUtil.randomAutomaton(random());
      if (random().nextBoolean()) {
        a = BasicOperations.removeDeadStates(a);
      }
      LightAutomaton ra = SpecialOperations.reverse(a);
      LightAutomaton rda = BasicOperations.determinize(ra);

      if (BasicOperations.isEmpty(a)) {
        assertTrue(BasicOperations.isEmpty(rda));
        continue;
      }

      RandomAcceptedStrings ras = new RandomAcceptedStrings(a);

      for(int iter2=0;iter2<20;iter2++) {
        // Find string accepted by original automaton
        int[] s = ras.getRandomAcceptedString(random());

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

  public void testBasicIsEmpty() throws Exception {
    LightAutomaton a = new LightAutomaton();
    a.createState();
    assertTrue(BasicOperations.isEmpty(a));
  }

  public void testRemoveDeadTransitionsEmpty() throws Exception {
    LightAutomaton a = BasicAutomata.makeEmptyLight();
    LightAutomaton a2 = BasicOperations.removeDeadStates(a);
    assertTrue(BasicOperations.isEmpty(a2));
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
      List<Transition> allTrans = new ArrayList<>();
      int numStates = a.getNumStates();
      for(int s=0;s<numStates;s++) {
        int count = a.getNumTransitions(s);
        for(int i=0;i<count;i++) {
          Transition t = new Transition();
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
      for(Transition t : allTrans) {
        builder.addTransition(t.source, t.dest, t.min, t.max);
      }

      assertTrue(BasicOperations.sameLanguage(
                    BasicOperations.determinize(BasicOperations.removeDeadStates(a)),
                    BasicOperations.determinize(BasicOperations.removeDeadStates(builder.finish()))));
      
    }
  }

  public void testIsTotal() throws Exception {
    assertFalse(BasicOperations.isTotal(new LightAutomaton()));
    LightAutomaton a = new LightAutomaton();
    int init = a.createState();
    int fini = a.createState();
    a.setAccept(fini, true);
    a.addTransition(init, fini, Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
    a.finishState();
    assertFalse(BasicOperations.isTotal(a));
    a.addTransition(fini, fini, Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
    a.finishState();
    assertFalse(BasicOperations.isTotal(a));
    a.setAccept(init, true);
    assertTrue(BasicOperations.isTotal(MinimizationOperationsLight.minimize(a)));
  }

  public void testMinimizeEmpty() throws Exception {
    LightAutomaton a = new LightAutomaton();
    int init = a.createState();
    int fini = a.createState();
    a.addTransition(init, fini, 'a');
    a.finishState();
    a = MinimizationOperationsLight.minimize(a);
    assertEquals(0, a.getNumStates());
  }

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
    assertMatches(a, "foobar", "beebar", "boobar");

    LightAutomaton a4 = BasicOperations.determinize(BasicOperations.minusLight(a, a2));
    
    assertTrue(BasicOperations.run(a4, "foobar"));
    assertFalse(BasicOperations.run(a4, "boobar"));
    assertTrue(BasicOperations.run(a4, "beebar"));
    assertMatches(a4, "foobar", "beebar");

    a4 = BasicOperations.determinize(BasicOperations.minusLight(a4, a1));
    assertFalse(BasicOperations.run(a4, "foobar"));
    assertFalse(BasicOperations.run(a4, "boobar"));
    assertTrue(BasicOperations.run(a4, "beebar"));
    assertMatches(a4, "beebar");

    a4 = BasicOperations.determinize(BasicOperations.minusLight(a4, a3));
    assertFalse(BasicOperations.run(a4, "foobar"));
    assertFalse(BasicOperations.run(a4, "boobar"));
    assertFalse(BasicOperations.run(a4, "beebar"));
    assertMatches(a4);
  }

  public void testOneInterval() throws Exception {
    LightAutomaton a = BasicAutomata.makeIntervalLight(999, 1032, 0);
    a = BasicOperations.determinize(a);
    assertTrue(BasicOperations.run(a, "0999"));
    assertTrue(BasicOperations.run(a, "00999"));
    assertTrue(BasicOperations.run(a, "000999"));
  }

  public void testAnotherInterval() throws Exception {
    LightAutomaton a = BasicAutomata.makeIntervalLight(1, 2, 0);
    a = BasicOperations.determinize(a);
    assertTrue(BasicOperations.run(a, "01"));
  }

  public void testIntervalRandom() throws Exception {
    int ITERS = atLeast(100);
    for(int iter=0;iter<ITERS;iter++) {
      int min = TestUtil.nextInt(random(), 0, 100000);
      int max = TestUtil.nextInt(random(), min, min+100000);
      int digits;
      if (random().nextBoolean()) {
        digits = 0;
      } else {
        String s = Integer.toString(max);
        digits = TestUtil.nextInt(random(), s.length(), 2*s.length());
      }
      StringBuilder b = new StringBuilder();
      for(int i=0;i<digits;i++) {
        b.append('0');
      }
      String prefix = b.toString();

      LightAutomaton a = BasicOperations.determinize(BasicAutomata.makeIntervalLight(min, max, digits));
      if (random().nextBoolean()) {
        a = MinimizationOperationsLight.minimize(a);
      }
      String mins = Integer.toString(min);
      String maxs = Integer.toString(max);
      if (digits > 0) {
        mins = prefix.substring(mins.length()) + mins;
        maxs = prefix.substring(maxs.length()) + maxs;
      }
      assertTrue(BasicOperations.run(a, mins));
      assertTrue(BasicOperations.run(a, maxs));

      for(int iter2=0;iter2<100;iter2++) {
        int x = random().nextInt(2*max);
        boolean expected = x >= min && x <= max;
        String sx = Integer.toString(x);
        if (sx.length() < digits) {
          // Left-fill with 0s
          sx = b.substring(sx.length()) + sx;
        } else if (digits == 0) {
          // Left-fill with random number of 0s:
          int numZeros = random().nextInt(10);
          StringBuilder sb = new StringBuilder();
          for(int i=0;i<numZeros;i++) {
            sb.append('0');
          }
          sb.append(sx);
          sx = sb.toString();
        }
        assertEquals(expected, BasicOperations.run(a, sx));
      }
    }
  }

  private void assertMatches(LightAutomaton a, String... strings) {
    Set<IntsRef> expected = new HashSet<>();
    for(String s : strings) {
      IntsRef ints = new IntsRef();
      expected.add(Util.toUTF32(s, ints));
    }

    assertEquals(expected, SpecialOperations.getFiniteStrings(BasicOperations.determinize(a), -1)); 
  }

  public void testConcatenatePreservesDet() throws Exception {
    LightAutomaton a1 = BasicAutomata.makeStringLight("foobar");
    assertTrue(a1.isDeterministic());
    LightAutomaton a2 = BasicAutomata.makeStringLight("baz");
    assertTrue(a2.isDeterministic());
    assertTrue((BasicOperations.concatenateLight(Arrays.asList(a1, a2)).isDeterministic()));
  }

  public void testRemoveDeadStates() throws Exception {
    LightAutomaton a = BasicOperations.concatenateLight(Arrays.asList(BasicAutomata.makeStringLight("x"),
                                                                      BasicAutomata.makeStringLight("y")));
    assertEquals(4, a.getNumStates());
    a = BasicOperations.removeDeadStates(a);
    assertEquals(3, a.getNumStates());
  }

  public void testRemoveDeadStatesEmpty1() throws Exception {
    LightAutomaton a = new LightAutomaton();
    a.finishState();
    assertTrue(BasicOperations.isEmpty(a));
    assertTrue(BasicOperations.isEmpty(BasicOperations.removeDeadStates(a)));
  }

  public void testRemoveDeadStatesEmpty2() throws Exception {
    LightAutomaton a = new LightAutomaton();
    a.finishState();
    assertTrue(BasicOperations.isEmpty(a));
    assertTrue(BasicOperations.isEmpty(BasicOperations.removeDeadStates(a)));
  }

  public void testRemoveDeadStatesEmpty3() throws Exception {
    LightAutomaton a = new LightAutomaton();
    int init = a.createState();
    int fini = a.createState();
    a.addTransition(init, fini, 'a');
    LightAutomaton a2 = BasicOperations.removeDeadStates(a);
    assertEquals(0, a2.getNumStates());
  }

  public void testConcatEmpty() throws Exception {
    // If you concat empty automaton to anything the result should still be empty:
    LightAutomaton a = BasicOperations.concatenateLight(BasicAutomata.makeEmptyLight(),
                                                        BasicAutomata.makeStringLight("foo"));
    assertEquals(new HashSet<IntsRef>(), SpecialOperations.getFiniteStrings(a, -1));

    a = BasicOperations.concatenateLight(BasicAutomata.makeStringLight("foo"),
                                         BasicAutomata.makeEmptyLight());
    assertEquals(new HashSet<IntsRef>(), SpecialOperations.getFiniteStrings(a, -1));
  }

  public void testSeemsNonEmptyButIsNot1() throws Exception {
    LightAutomaton a = new LightAutomaton();
    // Init state has a transition but doesn't lead to accept
    int init = a.createState();
    int s = a.createState();
    a.addTransition(init, s, 'a');
    a.finishState();
    assertTrue(BasicOperations.isEmpty(a));
  }

  public void testSeemsNonEmptyButIsNot2() throws Exception {
    LightAutomaton a = new LightAutomaton();
    int init = a.createState();
    int s = a.createState();
    a.addTransition(init, s, 'a');
    // An orphan'd accept state
    s = a.createState();
    a.setAccept(s, true);
    a.finishState();
    assertTrue(BasicOperations.isEmpty(a));
  }

  public void testSameLanguage1() throws Exception {
    LightAutomaton a = BasicAutomata.makeEmptyStringLight();
    LightAutomaton a2 = BasicAutomata.makeEmptyStringLight();
    int state = a2.createState();
    a2.addTransition(0, state, 'a');
    a2.finishState();
    assertTrue(BasicOperations.sameLanguage(BasicOperations.removeDeadStates(a),
                                            BasicOperations.removeDeadStates(a2)));
  }

  private LightAutomaton randomNoOp(LightAutomaton a) {
    switch (random().nextInt(5)) {
    case 0:
      if (VERBOSE) {
        System.out.println("  randomNoOp: determinize");
      }
      return BasicOperations.determinize(a);
    case 1:
      if (VERBOSE) {
        System.out.println("  randomNoOp: minimize");
      }
      return MinimizationOperationsLight.minimize(a);
    case 2:
      if (VERBOSE) {
        System.out.println("  randomNoOp: removeDeadStates");
      }
      return BasicOperations.removeDeadStates(a);
    case 3:
      if (VERBOSE) {
        System.out.println("  randomNoOp: reverse reverse");
      }
      a = SpecialOperations.reverse(a);
      a = randomNoOp(a);
      return SpecialOperations.reverse(a);
    case 4:
      if (VERBOSE) {
        System.out.println("  randomNoOp: concat empty string");
      }
      return BasicOperations.concatenateLight(a, BasicAutomata.makeEmptyStringLight());
    case 5:
      if (VERBOSE) {
        System.out.println("  randomNoOp: union empty automaton");
      }
      return BasicOperations.unionLight(a, BasicAutomata.makeEmptyLight());
    }
    assert false;
    return null;
  }

  private LightAutomaton unionTerms(Collection<BytesRef> terms) {
    LightAutomaton a;
    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: unionTerms: use union");
      }
      List<LightAutomaton> as = new ArrayList<>();
      for(BytesRef term : terms) {
        as.add(BasicAutomata.makeStringLight(term.utf8ToString()));
      }
      a = BasicOperations.unionLight(as);
    } else {
      if (VERBOSE) {
        System.out.println("TEST: unionTerms: use makeStringUnion");
      }
      List<BytesRef> termsList = new ArrayList<>(terms);
      Collections.sort(termsList);
      a = BasicAutomata.makeStringUnionLight(termsList);
    }

    return randomNoOp(a);
  }

  private String getRandomString(boolean isAscii) {
    if (isAscii) {
      return TestUtil.randomSimpleString(random());
    } else {
      return TestUtil.randomRealisticUnicodeString(random());
    }
  }

  public void testRandomFinite() throws Exception {

    int numTerms = atLeast(10);
    int iters = atLeast(100);

    // Some of the ops we do (stripping random byte, reverse) turn valid UTF8 into invalid if we allow non-ascii:
    boolean isAscii = random().nextBoolean();

    if (VERBOSE) {
      System.out.println("TEST: isAscii=" + isAscii + " numTerms" + numTerms + " iters=" + iters);
    }

    Set<BytesRef> terms = new HashSet<>();
    while (terms.size() < numTerms) {
      terms.add(new BytesRef(getRandomString(isAscii)));
    }

    LightAutomaton a = unionTerms(terms);
    assertSame(terms, a);

    for(int iter=0;iter<iters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter + " numTerms=" + terms.size());
        System.out.println("  terms:");
        for(BytesRef term : terms) {
          System.out.println("    " + term);
        }
      }
      switch(random().nextInt(14)) {

      case 0:
        // concatenate prefix
        {
          if (VERBOSE) {
            System.out.println("  op=concat prefix");
          }
          Set<BytesRef> newTerms = new HashSet<>();
          BytesRef prefix = new BytesRef(getRandomString(isAscii));
          for(BytesRef term : terms) {
            BytesRef newTerm = BytesRef.deepCopyOf(prefix);
            newTerm.append(term);
            newTerms.add(newTerm);
          }
          terms = newTerms;
          boolean wasDeterministic1 = a.isDeterministic();
          a = BasicOperations.concatenateLight(BasicAutomata.makeStringLight(prefix.utf8ToString()), a);
          assertEquals(wasDeterministic1, a.isDeterministic());
        }
        break;

      case 1:
        // concatenate suffix
        {
          BytesRef suffix = new BytesRef(getRandomString(isAscii));
          if (VERBOSE) {
            System.out.println("  op=concat suffix " + suffix);
          }
          Set<BytesRef> newTerms = new HashSet<>();
          for(BytesRef term : terms) {
            BytesRef newTerm = BytesRef.deepCopyOf(term);
            newTerm.append(suffix);
            newTerms.add(newTerm);
          }
          terms = newTerms;
          a = BasicOperations.concatenateLight(a, BasicAutomata.makeStringLight(suffix.utf8ToString()));
        }
        break;

        // nocommit sometimes concat a suffix accepting more than 1 term, and sometimes non-det

      case 2:
        // determinize
        if (VERBOSE) {
          System.out.println("  op=determinize");
        }
        a = BasicOperations.determinize(a);
        assertTrue(a.isDeterministic());
        break;

      case 3:
        if (VERBOSE) {
          System.out.println("  op=minimize");
        }
        // minimize
        a = MinimizationOperationsLight.minimize(a);
        break;

      case 4:
        // union
        {
          if (VERBOSE) {
            System.out.println("  op=union");
          }
          Set<BytesRef> newTerms = new HashSet<>();
          int numNewTerms = random().nextInt(5);
          while (newTerms.size() < numNewTerms) {
            newTerms.add(new BytesRef(getRandomString(isAscii)));
          }
          terms.addAll(newTerms);
          LightAutomaton newA = unionTerms(newTerms);
          a = BasicOperations.unionLight(a, newA);
        }
        break;

      case 5:
        // optional
        {
          if (VERBOSE) {
            System.out.println("  op=optional");
          }
          a = BasicOperations.optionalLight(a);
          terms.add(new BytesRef());
        }
        break;

      case 6:
        // minus finite 
        {
          if (VERBOSE) {
            System.out.println("  op=minus finite");
          }
          if (terms.size() > 0) {
            RandomAcceptedStrings rasl = new RandomAcceptedStrings(BasicOperations.removeDeadStates(a));
            Set<BytesRef> toRemove = new HashSet<>();
            int numToRemove = TestUtil.nextInt(random(), 1, (terms.size()+1)/2);
            while (toRemove.size() < numToRemove) {
              int[] ints = rasl.getRandomAcceptedString(random());
              BytesRef term = new BytesRef(UnicodeUtil.newString(ints, 0, ints.length));
              if (toRemove.contains(term) == false) {
                toRemove.add(term);
              }
            }
            for(BytesRef term : toRemove) {
              boolean removed = terms.remove(term);
              assertTrue(removed);
            }
            LightAutomaton a2 = unionTerms(toRemove);
            a = BasicOperations.minusLight(a, a2);
          }
        }
        break;

      case 7:
        {
          // minus infinite
          List<LightAutomaton> as = new ArrayList<>();
          int count = TestUtil.nextInt(random(), 1, 5);
          Set<Integer> prefixes = new HashSet<>();
          while(prefixes.size() < count) {
            // prefix is a leading ascii byte; we remove <prefix>* from a
            int prefix = random().nextInt(128);
            prefixes.add(prefix);
          }

          if (VERBOSE) {
            System.out.println("  op=minus infinite prefixes=" + prefixes);
          }

          for(int prefix : prefixes) {
            // prefix is a leading ascii byte; we remove <prefix>* from a
            LightAutomaton a2 = new LightAutomaton();
            int init = a2.createState();
            int state = a2.createState();
            a2.addTransition(init, state, prefix);
            a2.setAccept(state, true);
            a2.addTransition(state, state, Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
            a2.finishState();
            as.add(a2);
            Iterator<BytesRef> it = terms.iterator();
            while (it.hasNext()) {
              BytesRef term = it.next();
              if (term.length > 0 && (term.bytes[term.offset] & 0xFF) == prefix) {
                it.remove();
              }
            }
          }
          LightAutomaton a2 = randomNoOp(BasicOperations.unionLight(as));
          a = BasicOperations.minusLight(a, a2);
        }
        break;

      case 8:
        {
          int count = TestUtil.nextInt(random(), 10, 20);
          if (VERBOSE) {
            System.out.println("  op=intersect infinite count=" + count);
          }
          // intersect infinite
          List<LightAutomaton> as = new ArrayList<>();

          Set<Integer> prefixes = new HashSet<>();
          while(prefixes.size() < count) {
            int prefix = random().nextInt(128);
            prefixes.add(prefix);
          }
          if (VERBOSE) {
            System.out.println("  prefixes=" + prefixes);
          }

          for(int prefix : prefixes) {
            // prefix is a leading ascii byte; we retain <prefix>* in a
            LightAutomaton a2 = new LightAutomaton();
            int init = a2.createState();
            int state = a2.createState();
            a2.addTransition(init, state, prefix);
            a2.setAccept(state, true);
            a2.addTransition(state, state, Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
            a2.finishState();
            as.add(a2);
            prefixes.add(prefix);
          }

          LightAutomaton a2 = BasicOperations.unionLight(as);
          if (random().nextBoolean()) {
            a2 = BasicOperations.determinize(a2);
          } else if (random().nextBoolean()) {
            a2 = MinimizationOperationsLight.minimize(a2);
          }
          a = BasicOperations.intersectionLight(a, a2);

          Iterator<BytesRef> it = terms.iterator();
          while (it.hasNext()) {
            BytesRef term = it.next();
            if (term.length == 0 || prefixes.contains(term.bytes[term.offset]&0xff) == false) {
              if (VERBOSE) {
                System.out.println("  drop term=" + term);
              }
              it.remove();
            } else {
              if (VERBOSE) {
                System.out.println("  keep term=" + term);
              }
            }
          }
        }        
        break;

      case 9:
        // reverse
        if (VERBOSE) {
          System.out.println("  op=reverse");
        }
        a = SpecialOperations.reverse(a);
        Set<BytesRef> newTerms = new HashSet<>();
        for(BytesRef term : terms) {
          newTerms.add(new BytesRef(new StringBuilder(term.utf8ToString()).reverse().toString()));
        }
        terms = newTerms;
        break;

      case 10:
        if (VERBOSE) {
          System.out.println("  op=randomNoOp");
        }
        a = randomNoOp(a);
        break;

      case 11:
        // interval
        int min = random().nextInt(1000);
        int max = min + random().nextInt(50);
        // digits must be non-zero else we make cycle
        int digits = Integer.toString(max).length();
        if (VERBOSE) {
          System.out.println("  op=union interval min=" + min + " max=" + max + " digits=" + digits);
        }
        a = BasicOperations.unionLight(a, BasicAutomata.makeIntervalLight(min, max, digits));
        StringBuilder b = new StringBuilder();
        for(int i=0;i<digits;i++) {
          b.append('0');
        }
        String prefix = b.toString();
        for(int i=min;i<=max;i++) {
          String s = Integer.toString(i);
          if (s.length() < digits) {
            // Left-fill with 0s
            s = prefix.substring(s.length()) + s;
          }
          terms.add(new BytesRef(s));
        }
        break;

      case 12:
        if (VERBOSE) {
          System.out.println("  op=remove the empty string");
        }
        a = BasicOperations.minusLight(a, BasicAutomata.makeEmptyStringLight());
        terms.remove(new BytesRef());
        break;

      case 13:
        if (VERBOSE) {
          System.out.println("  op=add the empty string");
        }
        a = BasicOperations.unionLight(a, BasicAutomata.makeEmptyStringLight());
        terms.add(new BytesRef());
        break;
      }

      assertSame(terms, a);
    }

    assertSame(terms, a);
  }

  private void assertSame(Collection<BytesRef> terms, LightAutomaton a) {

    try {
      assertTrue(SpecialOperations.isFinite(a));
      assertFalse(BasicOperations.isTotal(a));

      LightAutomaton detA = BasicOperations.determinize(a);

      // Make sure all terms are accepted:
      IntsRef scratch = new IntsRef();
      for(BytesRef term : terms) {
        Util.toIntsRef(term, scratch);
        assertTrue("failed to accept term=" + term.utf8ToString(), BasicOperations.run(detA, term.utf8ToString()));
      }

      // Use getFiniteStrings:
      Set<IntsRef> expected = new HashSet<>();
      for(BytesRef term : terms) {
        IntsRef intsRef = new IntsRef();
        Util.toUTF32(term.utf8ToString(), intsRef);
        expected.add(intsRef);
      }
      Set<IntsRef> actual = SpecialOperations.getFiniteStrings(a, -1);

      if (expected.equals(actual) == false) {
        System.out.println("FAILED:");
        for(IntsRef term : expected) {
          if (actual.contains(term) == false) {
            System.out.println("  term=" + term + " should be accepted but isn't");
          }
        }
        for(IntsRef term : actual) {
          if (expected.contains(term) == false) {
            System.out.println("  term=" + term + " is accepted but should not be");
          }
        }
        throw new AssertionError("mismatch");
      }

      // Use sameLanguage:
      LightAutomaton a2 = BasicOperations.removeDeadStates(BasicOperations.determinize(unionTerms(terms)));
      assertTrue(BasicOperations.sameLanguage(a2, BasicOperations.removeDeadStates(BasicOperations.determinize(a))));

      // Do same check, in UTF8 space
      LightAutomaton utf8 = randomNoOp(new UTF32ToUTF8Light().convert(a));
    
      Set<IntsRef> expected2 = new HashSet<>();
      for(BytesRef term : terms) {
        IntsRef intsRef = new IntsRef();
        Util.toIntsRef(term, intsRef);
        expected2.add(intsRef);
      }
      assertEquals(expected2, SpecialOperations.getFiniteStrings(utf8, -1));
    } catch (AssertionError ae) {
      System.out.println("TEST: FAILED: not same");
      System.out.println("  terms (count=" + terms.size() + "):");
      for(BytesRef term : terms) {
        System.out.println("    " + term);
      }
      System.out.println("  automaton:");
      System.out.println(a.toDot());
      //a.writeDot("fail");
      throw ae;
    }
  }
}
