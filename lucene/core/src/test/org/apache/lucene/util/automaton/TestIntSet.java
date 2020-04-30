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
        SortedIntSet sortedSet = new SortedIntSet(0);

        for (int i = 0; i < size; i++) {
            // Some duplicates is nice but not critical
            sortedSet.incr(random().nextInt(i + 1));
        }

        sortedSet.computeHash();
        IntSet frozen0 = sortedSet.freeze(0);

        assertEquals("Frozen set not equal to origin sorted set.", sortedSet, frozen0);
        assertEquals("Symmetry: Sorted set not equal to frozen set.", frozen0, sortedSet);

        IntSet frozen1 = sortedSet.freeze(random().nextInt());
        assertEquals("Sorted set modified while freezing?", sortedSet, frozen1);
        assertEquals("Frozen sets were not equal", frozen0, frozen1);
    }

    @Test
    public void testMapCutover() {
        SortedIntSet set = new SortedIntSet(10);
        for (int i = 0; i < 35; i++) {
            // No duplicates so there are enough elements to trigger impl cutover
            set.incr(i);
        }

        set.computeHash();
        assertTrue(set.size() > 32);

        for (int i = 0; i < 35; i++) {
            // This is pretty much the worst case, perf wise
            set.decr(i);
        }

        set.computeHash();
        assertTrue(set.size() == 0);
    }

    @Test
    public void testModify() {
        SortedIntSet set = new SortedIntSet(2);
        set.incr(1);
        set.incr(2);
        set.computeHash();

        FrozenIntSet set2 = set.freeze(0);
        assertEquals(set, set2);

        set.incr(1);
        set.computeHash();
        assertEquals(set, set2);

        set.decr(1);
        set.computeHash();
        assertEquals(set, set2);

        set.decr(1);
        set.computeHash();
        assertNotEquals(set, set2);
    }
}