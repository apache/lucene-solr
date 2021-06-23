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
import org.junit.Test;

public class TestIntSet extends LuceneTestCase {
    @Test
    public void testFreezeEqualitySmallSet() {
        testFreezeEquality(10);
    }

    @Test
    public void testFreezeEqualityLargeSet() {
        testFreezeEquality(100);
    }

    private void testFreezeEquality(int size) {
        StateSet stateSet = new StateSet(0);

        for (int i = 0; i < size; i++) {
            // Some duplicates is nice but not critical
            stateSet.incr(random().nextInt(i + 1));
        }


        IntSet frozen0 = stateSet.freeze(0);

        assertEquals("Frozen set not equal to origin sorted set.", stateSet, frozen0);
        assertEquals("Symmetry: Sorted set not equal to frozen set.", frozen0, stateSet);

        IntSet frozen1 = stateSet.freeze(random().nextInt());
        assertEquals("Sorted set modified while freezing?", stateSet, frozen1);
        assertEquals("Frozen sets were not equal", frozen0, frozen1);
    }

    @Test
    public void testMapCutover() {
        StateSet set = new StateSet(10);
        for (int i = 0; i < 35; i++) {
            // No duplicates so there are enough elements to trigger impl cutover
            set.incr(i);
        }


        assertTrue(set.size() > 32);

        for (int i = 0; i < 35; i++) {
            // This is pretty much the worst case, perf wise
            set.decr(i);
        }


        assertTrue(set.size() == 0);
    }

    @Test
    public void testModify() {
        StateSet set = new StateSet(2);
        set.incr(1);
        set.incr(2);


        FrozenIntSet set2 = set.freeze(0);
        assertEquals(set, set2);

        set.incr(1);

        assertEquals(set, set2);

        set.decr(1);

        assertEquals(set, set2);

        set.decr(1);
        assertNotEquals(set, set2);
        }

  @Test
  public void testHashCode() {
    StateSet set = new StateSet(1000);
    StateSet set2 = new StateSet(100);
    for (int i = 0; i < 100; i++) {
      set.incr(i);
      set2.incr(99 - i);
    }
    assertEquals(set.hashCode(), set2.hashCode());
    }
}