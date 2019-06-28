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

import org.apache.solr.SolrTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.IntByteHashMap;
import com.carrotsearch.hppc.cursors.IntByteCursor;
import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Tests {@link HLL} of type {@link HLLType#SPARSE}.
 */
public class SparseHLLTest extends SolrTestCase {
    private static final int log2m = 11;

    /**
     * Tests {@link HLL#addRaw(long)}.
     */
    @Test
    public void addTest() {
        { // insert an element with register value 1 (minimum set value)
            final int registerIndex = 0;
            final int registerValue = 1;
            final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue);

            final HLL hll = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            hll.addRaw(rawValue);

            assertOneRegisterSet(hll, registerIndex, (byte)registerValue);
        }
        { // insert an element with register value 31 (maximum set value)
            final int registerIndex = 0;
            final int registerValue = 31;
            final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue);

            final HLL hll = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            hll.addRaw(rawValue);

            assertOneRegisterSet(hll, registerIndex, (byte)registerValue);
        }
        { // insert an element that could overflow the register (past 31)
            final int registerIndex = 0;
            final int registerValue = 36;
            final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue);

            final HLL hll = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            hll.addRaw(rawValue);

            assertOneRegisterSet(hll, (short)registerIndex, (byte)31/*register max*/);
        }
        { // insert duplicate elements, observe no change
            final int registerIndex = 0;
            final int registerValue = 1;
            final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue);

            final HLL hll = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            hll.addRaw(rawValue);
            hll.addRaw(rawValue);

            assertOneRegisterSet(hll, registerIndex, (byte)registerValue);
        }
        { // insert elements that increase a register's value
            final int registerIndex = 0;
            final int registerValue = 1;
            final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue);

            final HLL hll = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            hll.addRaw(rawValue);

            final int registerValue2 = 2;
            final long rawValue2 = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue2);
            hll.addRaw(rawValue2);

            assertOneRegisterSet(hll, registerIndex, (byte)registerValue2);
        }
        { // insert elements that have lower register values, observe no change
            final int registerIndex = 0;
            final int registerValue = 2;
            final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue);

            final HLL hll = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            hll.addRaw(rawValue);

            final int registerValue2 = 1;
            final long rawValue2 = ProbabilisticTestUtil.constructHLLValue(log2m, registerIndex, registerValue2);
            hll.addRaw(rawValue2);

            assertOneRegisterSet(hll, registerIndex, (byte)registerValue);
        }
    }

    /**
     * Smoke test for {@link HLL#cardinality()} and the proper use of the small
     * range correction.
     */
    @Test
    public void smallRangeSmokeTest() {
        final int log2m = 11;
        final int m = (1 << log2m);
        final int regwidth = 5;

        // only one register set
        {
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, 0, 1));

            final long cardinality = hll.cardinality();

            // Trivially true that small correction conditions hold: one register
            // set implies zeroes exist, and estimator trivially smaller than 5m/2.
            // Small range correction: m * log(m/V)
            final long expected = (long)Math.ceil(m * Math.log((double)m / (m - 1)/*# of zeroes*/));
            assertEquals(cardinality, expected);
        }

        // all but one register set
        {
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary*/, HLLType.SPARSE);
            for(int i=0; i<(m - 1); i++) {
                hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, i, 1));
            }

            // Trivially true that small correction conditions hold: all but
            // one register set implies a zero exists, and estimator trivially
            // smaller than 5m/2 since it's alpha / ((m-1)/2)
            final long cardinality = hll.cardinality();

            // Small range correction: m * log(m/V)
            final long expected = (long)Math.ceil(m * Math.log((double)m / 1/*# of zeroes*/));
            assertEquals(cardinality, expected);
        }
    }

    /**
     * Smoke test for {@link HLL#cardinality()} and the proper use of the
     * uncorrected estimator.
     */
    @Test
    public void normalRangeSmokeTest() {
        final int log2m = 11;
        final int m = (1 << log2m);
        final int regwidth = 5;
        // regwidth = 5, so hash space is
        // log2m + (2^5 - 1 - 1), so L = log2m + 30
        final int l = log2m + 30;

        // all registers at 'medium' value
        {
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, m/*sparseThreshold*/, HLLType.SPARSE);

            final int registerValue = 7/*chosen to ensure neither correction kicks in*/;
            for(int i=0; i<m; i++) {
                hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, i, registerValue));
            }

            final long cardinality = hll.cardinality();

            // Simplified estimator when all registers take same value: alpha / (m/2^val)
            final double estimator = HLLUtil.alphaMSquared(m)/((double)m/Math.pow(2, registerValue));

            // Assert conditions for uncorrected range
            assertTrue(estimator <= Math.pow(2,l)/30);
            assertTrue(estimator > (5 * m /(double)2));

            final long expected = (long)Math.ceil(estimator);
            assertEquals(cardinality, expected);
        }
    }

    /**
     * Smoke test for {@link HLL#cardinality()} and the proper use of the large
     * range correction.
     */
    @Test
    public void largeRangeSmokeTest() {
        final int log2m = 11;
        final int m = (1 << log2m);
        final int regwidth = 5;
        // regwidth = 5, so hash space is
        // log2m + (2^5 - 1 - 1), so L = log2m + 30
        final int l = log2m + 30;

        // all registers at large value
        {
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, m/*sparseThreshold*/, HLLType.SPARSE);

            final int registerValue = 31/*chosen to ensure large correction kicks in*/;
            for(int i=0; i<m; i++) {
                hll.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, i, registerValue));
            }

            final long cardinality = hll.cardinality();


            // Simplified estimator when all registers take same value: alpha / (m/2^val)
            final double estimator = HLLUtil.alphaMSquared(m)/((double)m/Math.pow(2, registerValue));

            // Assert conditions for large range
            assertTrue(estimator > Math.pow(2, l)/30);

            // Large range correction: -2^32 * log(1 - E/2^32)
            final long expected = (long)Math.ceil(-1.0 * Math.pow(2, l) * Math.log(1.0 - estimator/Math.pow(2, l)));
            assertEquals(cardinality, expected);
        }
    }

    /**
     * Tests {@link HLL#union(HLL)}.
     */
    @Test
    public void unionTest() {
        final int log2m = 11/*arbitrary*/;
        final int sparseThreshold = 256/*arbitrary*/;

        { // two empty multisets should union to an empty set
            final HLL hllA = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            final HLL hllB = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);

            hllA.union(hllB);

            assertEquals(hllA.getType(), HLLType.SPARSE/*unchanged*/);
            assertEquals(hllA.cardinality(), 0L);
        }
        { // two disjoint multisets should union properly
            final HLL hllA = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            hllA.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, 1, 1));
            final HLL hllB = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            hllB.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, 2, 1));


            hllA.union(hllB);

            assertEquals(hllA.getType(), HLLType.SPARSE/*unchanged*/);
            assertEquals(hllA.cardinality(), 3L/*precomputed*/);
            assertRegisterPresent(hllA, 1, (byte)1);
            assertRegisterPresent(hllA, 2, (byte)1);
        }
        { // two exactly overlapping multisets should union properly
            final HLL hllA = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            hllA.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, 1, 10));
            final HLL hllB = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            hllB.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, 1, 13));

            hllA.union(hllB);

            assertEquals(hllA.getType(), HLLType.SPARSE/*unchanged*/);
            assertEquals(hllA.cardinality(), 2L/*precomputed*/);
            assertOneRegisterSet(hllA, 1, (byte)13/*max(10,13)*/);
        }
        { // overlapping multisets should union properly
            final HLL hllA = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            final HLL hllB = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            // register index = 3
            final long rawValueA = ProbabilisticTestUtil.constructHLLValue(log2m, 3, 11);

            // register index = 4
            final long rawValueB = ProbabilisticTestUtil.constructHLLValue(log2m, 4, 13);
            final long rawValueBPrime = ProbabilisticTestUtil.constructHLLValue(log2m, 4, 21);

            // register index = 5
            final long rawValueC = ProbabilisticTestUtil.constructHLLValue(log2m, 5, 14);

            hllA.addRaw(rawValueA);
            hllA.addRaw(rawValueB);

            hllB.addRaw(rawValueBPrime);
            hllB.addRaw(rawValueC);

            hllA.union(hllB);
            // union should have three registers set, with partition B set to the
            // max of the two registers
            assertRegisterPresent(hllA, 3, (byte)11);
            assertRegisterPresent(hllA, 4, (byte)21/*max(21,13)*/);
            assertRegisterPresent(hllA, 5, (byte)14);
        }
        { // too-large unions should promote
            final HLL hllA = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            final HLL hllB = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);

            // fill up sets to maxCapacity
            for(int i=0; i<sparseThreshold; i++) {
                hllA.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, i, 1));
                hllB.addRaw(ProbabilisticTestUtil.constructHLLValue(log2m, (i + sparseThreshold)/*non-overlapping*/, 1));
            }

            hllA.union(hllB);

            assertEquals(hllA.getType(), HLLType.FULL);
        }
    }

    /**
     * Tests {@link HLL#clear()}.
     */
    @Test
    public void clearTest() {
        final HLL hll = new HLL(log2m, 5/*regwidth*/, 128/*explicitThreshold, arbitrary, unused*/, 256/*sparseThreshold, arbitrary, unused*/, HLLType.SPARSE);
        hll.addRaw(1L);
        hll.clear();
        assertEquals(hll.cardinality(), 0L);
    }

    /**
     * Tests {@link HLL#toBytes(ISchemaVersion)} and
     * {@link HLL#fromBytes(byte[])}.
     */
    @Test
    public void toFromBytesTest() {
        final int log2m = 11/*arbitrary*/;
        final int regwidth = 5/*arbitrary*/;
        final int sparseThreshold = 256/*arbitrary*/;
        final int shortWordLength = 16/*log2m + regwidth = 11 + 5*/;

        final ISchemaVersion schemaVersion = SerializationUtil.DEFAULT_SCHEMA_VERSION;
        final HLLType type = HLLType.SPARSE;
        final int padding = schemaVersion.paddingBytes(type);

        {// Should work on an empty element
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);
            final byte[] bytes = hll.toBytes(schemaVersion);

            // output should just be padding since no registers are used
            assertEquals(bytes.length, padding);

            final HLL inHLL = HLL.fromBytes(bytes);

            // assert register values correct
            assertElementsEqual(hll, inHLL);
        }
        {// Should work on a partially filled element
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);

            for(int i=0; i<3; i++) {
                final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, i, (i+9));
                hll.addRaw(rawValue);
            }

            final byte[] bytes = hll.toBytes(schemaVersion);

            assertEquals(bytes.length, padding + ProbabilisticTestUtil.getRequiredBytes(shortWordLength, 3/*registerCount*/));

            final HLL inHLL = HLL.fromBytes(bytes);

            // assert register values correct
            assertElementsEqual(hll, inHLL);
        }
        {// Should work on a full set
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);

            for(int i=0; i<sparseThreshold; i++) {
                final long rawValue = ProbabilisticTestUtil.constructHLLValue(log2m, i, (i % 9) + 1);
                hll.addRaw(rawValue);
            }

            final byte[] bytes = hll.toBytes(schemaVersion);

            // 'short words' should be 12 bits + 5 bits = 17 bits long
            assertEquals(bytes.length, padding + ProbabilisticTestUtil.getRequiredBytes(shortWordLength, sparseThreshold));

            final HLL inHLL = HLL.fromBytes(bytes);

            // assert register values correct
            assertElementsEqual(hll, inHLL);
        }
    }

    /**
     * Smoke tests the multisets by adding random values.
     */
    @Test
    public void randomValuesTest() {
        final int log2m = 11/*arbitrary*/;
        final int regwidth = 5/*arbitrary*/;
        final int sparseThreshold = 256/*arbitrary*/;

        for(int run=0; run<100; run++) {
            final HLL hll = new HLL(log2m, regwidth, 128/*explicitThreshold, arbitrary, unused*/, sparseThreshold, HLLType.SPARSE);

            final IntByteHashMap map = new IntByteHashMap();

            for(int i=0; i<sparseThreshold; i++) {
                final long rawValue = RandomizedTest.randomLong();

                final short registerIndex = ProbabilisticTestUtil.getRegisterIndex(rawValue, log2m);
                final byte registerValue = ProbabilisticTestUtil.getRegisterValue(rawValue, log2m);
                if(map.get(registerIndex) < registerValue) {
                    map.put(registerIndex, registerValue);
                }

                hll.addRaw(rawValue);
            }

            for (IntByteCursor c : map) {
                final byte expectedRegisterValue = map.get(c.key);
                assertRegisterPresent(hll, c.key, expectedRegisterValue);
            }
        }
    }

    //*************************************************************************
    // assertion helpers
    /**
     * Asserts that the register at the specified index is set to the specified
     * value.
     */
    private static void assertRegisterPresent(final HLL hll,
                                              final int registerIndex,
                                              final int registerValue) {
        final IntByteHashMap sparseProbabilisticStorage = hll.sparseProbabilisticStorage;
        assertEquals(sparseProbabilisticStorage.get(registerIndex), registerValue);
    }

    /**
     * Asserts that only the specified register is set and has the specified value.
     */
    private static void assertOneRegisterSet(final HLL hll,
                                             final int registerIndex,
                                             final byte registerValue) {
        final IntByteHashMap sparseProbabilisticStorage = hll.sparseProbabilisticStorage;
        assertEquals(sparseProbabilisticStorage.size(), 1);
        assertEquals(sparseProbabilisticStorage.get(registerIndex), registerValue);
    }

    /**
     * Asserts that all registers in the two {@link HLL} instances are identical.
     */
    private static void assertElementsEqual(final HLL hllA, final HLL hllB) {
        final IntByteHashMap sparseProbabilisticStorageA = hllA.sparseProbabilisticStorage;
        final IntByteHashMap sparseProbabilisticStorageB = hllB.sparseProbabilisticStorage;
        assertEquals(sparseProbabilisticStorageA.size(), sparseProbabilisticStorageB.size());
        for (IntByteCursor c : sparseProbabilisticStorageA) {
            assertEquals(sparseProbabilisticStorageA.get(c.key), 
                         sparseProbabilisticStorageB.get(c.key));
        }
    }
}
