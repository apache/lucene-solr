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
package org.apache.lucene.search.suggest.fst;

import java.util.*;

import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.Input;
import org.apache.lucene.search.suggest.InputArrayIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class WFSTCompletionTest extends LuceneTestCase {
  
  public void testBasic() throws Exception {
    Input keys[] = new Input[] {
        new Input("foo", 50),
        new Input("bar", 10),
        new Input("barbar", 12),
        new Input("barbara", 6)
    };
    
    Random random = new Random(random().nextLong());
    WFSTCompletionLookup suggester = new WFSTCompletionLookup();
    suggester.build(new InputArrayIterator(keys));
    
    // top N of 2, but only foo is available
    List<LookupResult> results = suggester.lookup(TestUtil.stringToCharSequence("f", random), false, 2);
    assertEquals(1, results.size());
    assertEquals("foo", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // make sure we don't get a dup exact suggestion:
    results = suggester.lookup(TestUtil.stringToCharSequence("foo", random), false, 2);
    assertEquals(1, results.size());
    assertEquals("foo", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // top N of 1 for 'bar': we return this even though barbar is higher
    results = suggester.lookup(TestUtil.stringToCharSequence("bar", random), false, 1);
    assertEquals(1, results.size());
    assertEquals("bar", results.get(0).key.toString());
    assertEquals(10, results.get(0).value, 0.01F);
    
    // top N Of 2 for 'b'
    results = suggester.lookup(TestUtil.stringToCharSequence("b", random), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    
    // top N of 3 for 'ba'
    results = suggester.lookup(TestUtil.stringToCharSequence("ba", random), false, 3);
    assertEquals(3, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    assertEquals("barbara", results.get(2).key.toString());
    assertEquals(6, results.get(2).value, 0.01F);
  }

  public void testExactFirst() throws Exception {

    WFSTCompletionLookup suggester = new WFSTCompletionLookup(true);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input("x y", 20),
          new Input("x", 2),
        }));

    for(int topN=1;topN<4;topN++) {
      List<LookupResult> results = suggester.lookup("x", false, topN);

      assertEquals(Math.min(topN, 2), results.size());

      assertEquals("x", results.get(0).key);
      assertEquals(2, results.get(0).value);

      if (topN > 1) {
        assertEquals("x y", results.get(1).key);
        assertEquals(20, results.get(1).value);
      }
    }
  }

  public void testNonExactFirst() throws Exception {

    WFSTCompletionLookup suggester = new WFSTCompletionLookup(false);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input("x y", 20),
          new Input("x", 2),
        }));

    for(int topN=1;topN<4;topN++) {
      List<LookupResult> results = suggester.lookup("x", false, topN);

      assertEquals(Math.min(topN, 2), results.size());

      assertEquals("x y", results.get(0).key);
      assertEquals(20, results.get(0).value);

      if (topN > 1) {
        assertEquals("x", results.get(1).key);
        assertEquals(2, results.get(1).value);
      }
    }
  }
  
  public void testRandom() throws Exception {
    int numWords = atLeast(1000);
    
    final TreeMap<String,Long> slowCompletor = new TreeMap<>();
    final TreeSet<String> allPrefixes = new TreeSet<>();
    
    Input[] keys = new Input[numWords];
    
    for (int i = 0; i < numWords; i++) {
      String s;
      while (true) {
        // TODO: would be nice to fix this slowCompletor/comparator to
        // use full range, but we might lose some coverage too...
        s = TestUtil.randomSimpleString(random());
        if (!slowCompletor.containsKey(s)) {
          break;
        }
      }
      
      for (int j = 1; j < s.length(); j++) {
        allPrefixes.add(s.substring(0, j));
      }
      // we can probably do Integer.MAX_VALUE here, but why worry.
      int weight = random().nextInt(1<<24);
      slowCompletor.put(s, (long)weight);
      keys[i] = new Input(s, weight);
    }

    WFSTCompletionLookup suggester = new WFSTCompletionLookup(false);
    suggester.build(new InputArrayIterator(keys));

    assertEquals(numWords, suggester.getCount());
    Random random = new Random(random().nextLong());
    for (String prefix : allPrefixes) {
      final int topN = TestUtil.nextInt(random, 1, 10);
      List<LookupResult> r = suggester.lookup(TestUtil.stringToCharSequence(prefix, random), false, topN);

      // 2. go thru whole treemap (slowCompletor) and check it's actually the best suggestion
      final List<LookupResult> matches = new ArrayList<>();

      // TODO: could be faster... but it's slowCompletor for a reason
      for (Map.Entry<String,Long> e : slowCompletor.entrySet()) {
        if (e.getKey().startsWith(prefix)) {
          matches.add(new LookupResult(e.getKey(), e.getValue().longValue()));
        }
      }

      assertTrue(matches.size() > 0);
      Collections.sort(matches, new Comparator<LookupResult>() {
        @Override
        public int compare(LookupResult left, LookupResult right) {
          int cmp = Float.compare(right.value, left.value);
          if (cmp == 0) {
            return left.compareTo(right);
          } else {
            return cmp;
          }
        }
      });
      if (matches.size() > topN) {
        matches.subList(topN, matches.size()).clear();
      }

      assertEquals(matches.size(), r.size());

      for(int hit=0;hit<r.size();hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(matches.get(hit).key.toString(), r.get(hit).key.toString());
        assertEquals(matches.get(hit).value, r.get(hit).value, 0f);
      }
    }
  }

  public void test0ByteKeys() throws Exception {
    BytesRef key1 = new BytesRef(4);
    key1.length = 4;
    BytesRef key2 = new BytesRef(3);
    key1.length = 3;

    WFSTCompletionLookup suggester = new WFSTCompletionLookup(false);

    suggester.build(new InputArrayIterator(new Input[] {
          new Input(key1, 50),
          new Input(key2, 50),
        }));
  }

  public void testEmpty() throws Exception {
    WFSTCompletionLookup suggester = new WFSTCompletionLookup(false);

    suggester.build(new InputArrayIterator(new Input[0]));
    assertEquals(0, suggester.getCount());
    List<LookupResult> result = suggester.lookup("a", false, 20);
    assertTrue(result.isEmpty());
  }
}
