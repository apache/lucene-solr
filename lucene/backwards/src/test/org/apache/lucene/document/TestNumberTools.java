package org.apache.lucene.document;

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

public class TestNumberTools extends LuceneTestCase {
    public void testNearZero() {
        for (int i = -100; i <= 100; i++) {
            for (int j = -100; j <= 100; j++) {
                subtestTwoLongs(i, j);
            }
        }
    }

    public void testMax() {
        // make sure the constants convert to their equivelents
        assertEquals(Long.MAX_VALUE, NumberTools
                .stringToLong(NumberTools.MAX_STRING_VALUE));
        assertEquals(NumberTools.MAX_STRING_VALUE, NumberTools
                .longToString(Long.MAX_VALUE));

        // test near MAX, too
        for (long l = Long.MAX_VALUE; l > Long.MAX_VALUE - 10000; l--) {
            subtestTwoLongs(l, l - 1);
        }
    }

    public void testMin() {
        // make sure the constants convert to their equivelents
        assertEquals(Long.MIN_VALUE, NumberTools
                .stringToLong(NumberTools.MIN_STRING_VALUE));
        assertEquals(NumberTools.MIN_STRING_VALUE, NumberTools
                .longToString(Long.MIN_VALUE));

        // test near MIN, too
        for (long l = Long.MIN_VALUE; l < Long.MIN_VALUE + 10000; l++) {
            subtestTwoLongs(l, l + 1);
        }
    }

    private static void subtestTwoLongs(long i, long j) {
        // convert to strings
        String a = NumberTools.longToString(i);
        String b = NumberTools.longToString(j);

        // are they the right length?
        assertEquals(NumberTools.STR_SIZE, a.length());
        assertEquals(NumberTools.STR_SIZE, b.length());

        // are they the right order?
        if (i < j) {
            assertTrue(a.compareTo(b) < 0);
        } else if (i > j) {
            assertTrue(a.compareTo(b) > 0);
        } else {
            assertEquals(a, b);
        }

        // can we convert them back to longs?
        long i2 = NumberTools.stringToLong(a);
        long j2 = NumberTools.stringToLong(b);

        assertEquals(i, i2);
        assertEquals(j, j2);
    }
}