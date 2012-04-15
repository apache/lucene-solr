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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestCompiledAutomaton extends LuceneTestCase {

  private CompiledAutomaton build(String... strings) {
    final List<BytesRef> terms = new ArrayList<BytesRef>();
    for(String s : strings) {
      terms.add(new BytesRef(s));
    }
    Collections.sort(terms);
    final Automaton a = DaciukMihovAutomatonBuilder.build(terms);
    return new CompiledAutomaton(a, true, false);
  }

  private void testFloor(CompiledAutomaton c, String input, String expected) {
    final BytesRef b = new BytesRef(input);
    final BytesRef result = c.floor(b, b);
    if (expected == null) {
      assertNull(result);
    } else {
      assertNotNull(result);
      assertEquals("actual=" + result.utf8ToString() + " vs expected=" + expected + " (input=" + input + ")",
                   result, new BytesRef(expected));
    }
  }

  private void testTerms(String[] terms) throws Exception {
    final CompiledAutomaton c = build(terms);
    final BytesRef[] termBytes = new BytesRef[terms.length];
    for(int idx=0;idx<terms.length;idx++) {
      termBytes[idx] = new BytesRef(terms[idx]);
    }
    Arrays.sort(termBytes);

    if (VERBOSE) {
      System.out.println("\nTEST: terms in unicode order");
      for(BytesRef t : termBytes) {
        System.out.println("  " + t.utf8ToString());
      }
      //System.out.println(c.utf8.toDot());
    }

    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {
      final String s = random().nextInt(10) == 1 ? terms[random().nextInt(terms.length)] : randomString();
      if (VERBOSE) {
        System.out.println("\nTEST: floor(" + s + ")");
      }
      int loc = Arrays.binarySearch(termBytes, new BytesRef(s));
      final String expected;
      if (loc >= 0) {
        expected = s;
      } else {
        // term doesn't exist
        loc = -(loc+1);
        if (loc == 0) {
          expected = null;
        } else {
          expected = termBytes[loc-1].utf8ToString();
        }
      }
      if (VERBOSE) {
        System.out.println("  expected=" + expected);
      }
      testFloor(c, s, expected);
    }
  }

  public void testRandom() throws Exception {
    final int numTerms = atLeast(400);
    final Set<String> terms = new HashSet<String>();
    while(terms.size() != numTerms) {
      terms.add(randomString());
    }
    testTerms(terms.toArray(new String[terms.size()]));
  }

  private String randomString() {
    // return _TestUtil.randomSimpleString(random);
    return _TestUtil.randomRealisticUnicodeString(random());
  }

  public void testBasic() throws Exception {
    CompiledAutomaton c = build("fob", "foo", "goo");
    testFloor(c, "goo", "goo");
    testFloor(c, "ga", "foo");
    testFloor(c, "g", "foo");
    testFloor(c, "foc", "fob");
    testFloor(c, "foz", "foo");
    testFloor(c, "f", null);
    testFloor(c, "", null);
    testFloor(c, "aa", null);
    testFloor(c, "zzz", "goo");
  }
}
