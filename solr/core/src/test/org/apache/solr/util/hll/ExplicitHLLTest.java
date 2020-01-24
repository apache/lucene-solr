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

import java.util.HashSet;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.LongHashSet;
import static com.carrotsearch.randomizedtesting.RandomizedTest.*;


/**
 * Tests {@link HLL} of type {@link HLLType#EXPLICIT}.
 */
public class ExplicitHLLTest extends SolrTestCase {
    /**
     * Tests basic set semantics of {@link HLL#addRaw(long)}.
     */
    @Test
    public void addBasicTest() {
        { // Adding a single positive value to an empty set should work.
            final HLL hll = newHLL(128/*arbitrary*/);
            hll.addRaw(1L/*positive*/);
            assertEquals(hll.cardinality(), 1L);
        }
        { // Adding a single negative value to an empty set should work.
            final HLL hll = newHLL(128/*arbitrary*/);
            hll.addRaw(-1L/*negative*/);
            assertEquals(hll.cardinality(), 1L);
        }
        { // Adding a duplicate value to a set should be a no-op.
            final HLL hll = newHLL(128/*arbitrary*/);
            hll.addRaw(1L/*positive*/);
            assertEquals(hll.cardinality(), 1L/*arbitrary*/);
            assertEquals(hll.cardinality(), 1L/*dupe*/);
        }
    }

    // ------------------------------------------------------------------------
    /**
     * Tests {@link HLL#union(HLL)}.
     */
    @Test
    public void unionTest() {
        {// Unioning two distinct sets should work
            final HLL hllA = newHLL(128/*arbitrary*/);
            final HLL hllB = newHLL(128/*arbitrary*/);
            hllA.addRaw(1L);
            hllA.addRaw(2L);
            hllB.addRaw(3L);

            hllA.union(hllB);
            assertEquals(hllA.cardinality(), 3);
        }
        {// Unioning two sets whose union doesn't exceed the cardinality cap should not promote
            final HLL hllA = newHLL(128/*arbitrary*/);
            final HLL hllB = newHLL(128/*arbitrary*/);
            hllA.addRaw(1L);
            hllA.addRaw(2L);
            hllB.addRaw(1L);

            hllA.union(hllB);
            assertEquals(hllA.cardinality(), 2);
        }
        {// unioning two sets whose union exceeds the cardinality cap should promote
            final HLL hllA = newHLL(128/*arbitrary*/);
            final HLL hllB = newHLL(128/*arbitrary*/);

            // fill up sets to explicitThreshold
            for(long i=0; i<128/*explicitThreshold*/; i++) {
                hllA.addRaw(i);
                hllB.addRaw(i + 128);
            }

            hllA.union(hllB);
            assertEquals(hllA.getType(), HLLType.SPARSE);
        }
    }

    // ------------------------------------------------------------------------
    /**
     * Tests {@link HLL#clear()}
     */
    @Test
    public void clearTest() {
        final HLL hll = newHLL(128/*arbitrary*/);
        hll.addRaw(1L);
        assertEquals(hll.cardinality(), 1L);
        hll.clear();
        assertEquals(hll.cardinality(), 0L);
    }

    // ------------------------------------------------------------------------
    /**
     */
    @Test
    public void toFromBytesTest() {
        final ISchemaVersion schemaVersion = SerializationUtil.DEFAULT_SCHEMA_VERSION;
        final HLLType type = HLLType.EXPLICIT;
        final int padding = schemaVersion.paddingBytes(type);
        final int bytesPerWord = 8;

        {// Should work on an empty set
            final HLL hll = newHLL(128/*arbitrary*/);

            final byte[] bytes = hll.toBytes(schemaVersion);

            // assert output has correct byte length
            assertEquals(bytes.length, padding/*no elements, just padding*/);

            final HLL inHLL = HLL.fromBytes(bytes);

            assertElementsEqual(hll, inHLL);
        }
        {// Should work on a partially filled set
            final HLL hll = newHLL(128/*arbitrary*/);

            for(int i=0; i<3; i++) {
                hll.addRaw(i);
            }

            final byte[] bytes = hll.toBytes(schemaVersion);

            // assert output has correct byte length
            assertEquals(bytes.length, padding + (bytesPerWord * 3/*elements*/));

            final HLL inHLL = HLL.fromBytes(bytes);

            assertElementsEqual(hll, inHLL);
        }
        {// Should work on a full set
            final int explicitThreshold = 128;
            final HLL hll = newHLL(explicitThreshold);

            for(int i=0; i<explicitThreshold; i++) {
                hll.addRaw(27 + i/*arbitrary*/);
            }

            final byte[] bytes = hll.toBytes(schemaVersion);

            // assert output has correct byte length
            assertEquals(bytes.length, padding + (bytesPerWord * explicitThreshold/*elements*/));

            final HLL inHLL = HLL.fromBytes(bytes);

            assertElementsEqual(hll, inHLL);
        }
    }

    // ------------------------------------------------------------------------
    /**
     * Tests correctness against {@link java.util.HashSet}.
     */
    @Test
    public void randomValuesTest() {
        final int explicitThreshold = 4096;
        final HashSet<Long> canonical = new HashSet<Long>();
        final HLL hll = newHLL(explicitThreshold);

        for(int i=0;i<explicitThreshold;i++){
            long randomLong = randomLong();
            canonical.add(randomLong);
            hll.addRaw(randomLong);
        }
        final int canonicalCardinality = canonical.size();
        assertEquals(hll.cardinality(), canonicalCardinality);
    }

    // ------------------------------------------------------------------------
    /**
     * Tests promotion to {@link HLLType#SPARSE} and {@link HLLType#FULL}.
     */
    @Test
    public void promotionTest() {
        { // locally scoped for sanity
            final int explicitThreshold = 128;
            final HLL hll = new HLL(11/*log2m, unused*/, 5/*regwidth, unused*/, explicitThreshold, 256/*sparseThreshold*/, HLLType.EXPLICIT);

            for(int i=0;i<explicitThreshold + 1;i++){
                hll.addRaw(i);
            }
            assertEquals(hll.getType(), HLLType.SPARSE);
        }
        { // locally scoped for sanity
            final HLL hll = new HLL(11/*log2m, unused*/, 5/*regwidth, unused*/, 4/*expthresh => explicitThreshold = 8*/, false/*sparseon*/, HLLType.EXPLICIT);

            for(int i=0;i<9/* > explicitThreshold */;i++){
                hll.addRaw(i);
            }
            assertEquals(hll.getType(), HLLType.FULL);
        }
    }

    // ************************************************************************
    // assertion helpers
    /**
     * Asserts that values in both sets are exactly equal.
     */
    private static void assertElementsEqual(final HLL hllA, final HLL hllB) {
        final LongHashSet internalSetA = hllA.explicitStorage;
        final LongHashSet internalSetB = hllB.explicitStorage;

        assertTrue(internalSetA.equals(internalSetB));
    }

    /**
     * Builds a {@link HLLType#EXPLICIT} {@link HLL} instance with the specified
     * explicit threshold.
     *
     * @param  explicitThreshold explicit threshold to use for the constructed
     *         {@link HLL}. This must be greater than zero.
     * @return a default-sized {@link HLLType#EXPLICIT} empty {@link HLL} instance.
     *         This will never be <code>null</code>.
     */
    private static HLL newHLL(final int explicitThreshold) {
        return new HLL(11/*log2m, unused*/, 5/*regwidth, unused*/, explicitThreshold, 256/*sparseThreshold, arbitrary, unused*/, HLLType.EXPLICIT);
    }
}
