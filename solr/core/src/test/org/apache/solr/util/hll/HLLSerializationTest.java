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

package org.apache.solr.util.hll;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.apache.solr.util.hll.HLL.*;

/**
 * Serialization smoke-tests.
 */
public class HLLSerializationTest extends LuceneTestCase {
    /**
     * A smoke-test that covers serialization/deserialization of an HLL
     * under all possible parameters.
     */
    @Test
    @Slow
    @Nightly
    public void serializationSmokeTest() throws Exception {
        final Random random = new Random(randomLong());
        final int randomCount = 250;
        final List<Long> randoms = new ArrayList<Long>(randomCount);
        for (int i=0; i<randomCount; i++) {
          randoms.add(random.nextLong());
      }

        assertCardinality(HLLType.EMPTY, randoms);
        assertCardinality(HLLType.EXPLICIT, randoms);
        assertCardinality(HLLType.SPARSE, randoms);
        assertCardinality(HLLType.FULL, randoms);
    }

    // NOTE: log2m<=16 was chosen as the max log2m parameter so that the test
    //       completes in a reasonable amount of time. Not much is gained by
    //       testing larger values - there are no more known serialization
    //       related edge cases that appear as log2m gets even larger.
    // NOTE: This test completed successfully with log2m<=MAXIMUM_LOG2M_PARAM
    //       on 2014-01-30.
    private static void assertCardinality(final HLLType hllType, final Collection<Long> items)
           throws CloneNotSupportedException {
        for(int log2m=MINIMUM_LOG2M_PARAM; log2m<=16; log2m++) {
            for(int regw=MINIMUM_REGWIDTH_PARAM; regw<=MAXIMUM_REGWIDTH_PARAM; regw++) {
                for(int expthr=MINIMUM_EXPTHRESH_PARAM; expthr<=MAXIMUM_EXPTHRESH_PARAM; expthr++ ) {
                    for(final boolean sparse: new boolean[]{true, false}) {
                        HLL hll = new HLL(log2m, regw, expthr, sparse, hllType);
                        for(final Long item: items) {
                            hll.addRaw(item);
                        }
                        HLL copy = HLL.fromBytes(hll.toBytes());
                        assertEquals(copy.cardinality(), hll.cardinality());
                        assertEquals(copy.getType(), hll.getType());
                        assertTrue(Arrays.equals(copy.toBytes(), hll.toBytes()));

                        HLL clone = hll.clone();
                        assertEquals(clone.cardinality(), hll.cardinality());
                        assertEquals(clone.getType(), hll.getType());
                        assertTrue(Arrays.equals(clone.toBytes(), hll.toBytes()));
                    }
                }
            }
        }
    }
}
