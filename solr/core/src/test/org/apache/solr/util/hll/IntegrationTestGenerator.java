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

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;
import static org.apache.solr.util.hll.ProbabilisticTestUtil.*;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Generates test files for testing other implementations of HLL
 * serialization/deserialization, namely the PostgreSQL implementation.
 */
public class IntegrationTestGenerator {
    // ************************************************************************
    // directory to output the generated tests
    private static final String OUTPUT_DIRECTORY = "/tmp/hll_test/";

    // ------------------------------------------------------------------------
    // configurations for HLLs, should mirror settings in PostgreSQL impl. tests
    private static final int REGWIDTH = 5;
    private static final int LOG2M = 11;
    // NOTE:  This differs from the PostgreSQL impl. parameter 'expthresh'. This
    //        is a literal threshold to use in the promotion hierarchy, implying
    //        that both EXPLICIT representation should be used and it should
    //        NOT be automatically computed. This is done to ensure that the
    //        parameters of the test are very explicitly defined.
    private static final int EXPLICIT_THRESHOLD = 256;
    // NOTE:  This is not the PostgreSQL impl. parameter 'sparseon'. 'sparseon'
    //        is assumed to be true and this is a literal register-count threshold
    //        to use in the promotion hierarchy. This is done to ensure that the
    //        parameters of the test are very explicitly defined.
    private static final int SPARSE_THRESHOLD = 850;

    // ------------------------------------------------------------------------
    // computed constants
    private static final int REGISTER_COUNT = (1 << LOG2M);
    private static final int REGISTER_MAX_VALUE = (1 << REGWIDTH) - 1;

    // ========================================================================
    // Tests
    /**
     * Cumulatively adds random values to a FULL HLL through the small range
     * correction, uncorrected range, and large range correction of the HLL's
     * cardinality estimator.
     *
     * Format: cumulative add
     * Tests:
     * - FULL cardinality computation
     */
    private static void fullCardinalityCorrectionTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "cardinality_correction", TestType.ADD);

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.FULL);
        initLineAdd(output, hll, schemaVersion);

        // run through some values in the small range correction
        for(int i=0; i<((1 << LOG2M) - 1); i++) {
            final long rawValue = constructHLLValue(LOG2M, i, 1);
            cumulativeAddLine(output, hll, rawValue, schemaVersion);
        }

        // run up past some values in the uncorrected range
        for(int i=0; i<(1 << LOG2M); i++) {
            final long rawValue = constructHLLValue(LOG2M, i, 7);
            cumulativeAddLine(output, hll, rawValue, schemaVersion);
        }

        // run through some values in the large range correction
        for(int i=0; i<(1 << LOG2M); i++) {
            final long rawValue = constructHLLValue(LOG2M, i, 30);
            cumulativeAddLine(output, hll, rawValue, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Cumulatively adds random values to an EMPTY HLL.
     *
     * Format: cumulative add
     * Tests:
     * - EMPTY, EXPLICIT, SPARSE, PROBABILSTIC addition
     * - EMPTY to EXPLICIT promotion
     * - EXPLICIT to SPARSE promotion
     * - SPARSE to FULL promotion
     */
    private static void globalStepTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "comprehensive_promotion", TestType.ADD);

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.EMPTY);
        initLineAdd(output, hll, schemaVersion);

        for(int i=0; i<10000/*arbitrary*/; i++) {
            cumulativeAddLine(output, hll, randomLong(), schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Cumulatively unions "underpopulated" FULL HLLs into the
     * accumulator to verify the correct behavior from the PostgreSQL implementation.
     * The PostgreSQL implementation's representations of probabilistic HLLs should
     * depend exclusively on the chosen SPARSE-to-FULL cutoff.
     *
     * Format: cumulative union
     * Tests:
     * - EMPTY U "underpopulated" FULL => SPARSE
     * - SPARSE U "underpopulated" FULL => SPARSE
     * - SPARSE U "barely underpopulated" FULL => FULL
     */
    private static void sparseFullRepresentationTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "sparse_full_representation", TestType.UNION);

        final HLL emptyHLL1 = newHLL(HLLType.EMPTY);
        final HLL emptyHLL2 = newHLL(HLLType.EMPTY);

        cumulativeUnionLine(output, emptyHLL1, emptyHLL2, schemaVersion);

        // NOTE:  In this test the sparseReference will be the "expected" value
        //        from the C representation, since it doesn't choose representation
        //        based on original encoding, but rather on the promotion rules
        //        and the declared type of the "receiving" field.
        //        It is the manually-constructed union result.

        // "underpopulated" FULL U EMPTY => SPARSE
        final HLL fullHLL = newHLL(HLLType.FULL);
        fullHLL.addRaw(constructHLLValue(LOG2M, 0/*ix*/, 1/*val*/));

        final HLL sparseHLL = newHLL(HLLType.SPARSE);
        sparseHLL.addRaw(constructHLLValue(LOG2M, 0/*ix*/, 1/*val*/));

        output.write(stringCardinality(fullHLL) + "," + toByteA(fullHLL, schemaVersion) + "," + stringCardinality(sparseHLL) + "," + toByteA(sparseHLL, schemaVersion) + "\n");
        output.flush();

        // "underpopulated" FULL (small) U SPARSE (small) => SPARSE
        final HLL fullHLL2 = newHLL(HLLType.FULL);
        fullHLL2.addRaw(constructHLLValue(LOG2M, 1/*ix*/, 1/*val*/));

        sparseHLL.addRaw(constructHLLValue(LOG2M, 1/*ix*/, 1/*val*/));

        output.write(stringCardinality(fullHLL2) + "," + toByteA(fullHLL2, schemaVersion) + "," + stringCardinality(sparseHLL) + "," + toByteA(sparseHLL, schemaVersion) + "\n");
        output.flush();

        // "underpopulated" FULL (just on edge) U SPARSE (small) => FULL
        final HLL fullHLL3 = newHLL(HLLType.FULL);
        for(int i=2; i<(SPARSE_THRESHOLD + 1); i++) {
            fullHLL3.addRaw(constructHLLValue(LOG2M, i/*ix*/, 1/*val*/));
            sparseHLL.addRaw(constructHLLValue(LOG2M, i/*ix*/, 1/*val*/));
        }

        output.write(stringCardinality(fullHLL3) + "," + toByteA(fullHLL3, schemaVersion) + "," + stringCardinality(sparseHLL) + "," + toByteA(sparseHLL, schemaVersion) + "\n");
        output.flush();
    }

    /**
     * Cumulatively sets successive registers to:
     *
     *     <code>(registerIndex % REGISTER_MAX_VALUE) + 1</code>
     *
     * by adding specifically constructed values to a SPARSE HLL.
     * Does not induce promotion.
     *
     * Format: cumulative add
     * Tests:
     * - SPARSE addition (predictable)
     */
    private static void sparseStepTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "sparse_step", TestType.ADD);

        // the accumulator, starts empty sparse probabilistic
        final HLL hll = newHLL(HLLType.SPARSE);
        initLineAdd(output, hll, schemaVersion);

        for(int i=0; i<SPARSE_THRESHOLD; i++) {
            final long rawValue = constructHLLValue(LOG2M, i, ((i % REGISTER_MAX_VALUE) + 1));
            cumulativeAddLine(output, hll, rawValue, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Cumulatively sets random registers of a SPARSE HLL to
     * random values by adding random values. Does not induce promotion.
     *
     * Format: cumulative add
     * Tests:
     * - SPARSE addition (random)
     */
    private static void sparseRandomTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "sparse_random", TestType.ADD);

        final Random random = new Random(randomLong());

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.SPARSE);
        initLineAdd(output, hll, schemaVersion);

        for(int i=0; i<SPARSE_THRESHOLD; i++) {
            final int registerIndex = Math.abs(random.nextInt()) % REGISTER_COUNT;
            final int registerValue = ((Math.abs(random.nextInt()) % REGISTER_MAX_VALUE) + 1);
            final long rawValue = constructHLLValue(LOG2M, registerIndex, registerValue);

            cumulativeAddLine(output, hll, rawValue, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Cumulatively sets the first register (index 0) to value 2, the last
     * register (index m-1) to value 2, and then sets registers with indices in
     * the range 2 to (sparseCutoff + 2) to value 1 to trigger promotion.
     *
     * This tests for register alignment in the promotion from SPARSE
     * to FULL.
     *
     * Format: cumulative add
     * Tests:
     * - SPARSE addition
     * - SPARSE to FULL promotion
     */
    private static void sparseEdgeTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "sparse_edge", TestType.ADD);

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.SPARSE);
        initLineAdd(output, hll, schemaVersion);

        final long firstValue = constructHLLValue(LOG2M, 0, 2);
        cumulativeAddLine(output, hll, firstValue, schemaVersion);

        final long lastValue = constructHLLValue(LOG2M, (1 << LOG2M) - 1, 2);
        cumulativeAddLine(output, hll, lastValue, schemaVersion);

        for(int i=2; i<(SPARSE_THRESHOLD + 2); i++) {
            final long middleValue = constructHLLValue(LOG2M, i, 1);

            cumulativeAddLine(output, hll, middleValue, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Unions an EMPTY accumulator with EXPLICIT HLLs, each containing a
     * single random value.
     *
     * Format: cumulative union
     * Tests:
     * - EMPTY U EXPLICIT
     * - EXPLICIT U EXPLICIT
     * - EXPLICIT to SPARSE promotion
     * - SPARSE U EXPLICIT
     */
    private static void explicitPromotionTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "explicit_promotion", TestType.UNION);

        final Random random = new Random(randomLong());

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.EMPTY);
        final HLL emptyHLL = newHLL(HLLType.EMPTY);
        cumulativeUnionLine(output, hll, emptyHLL, schemaVersion);

        for(int i=0; i<(EXPLICIT_THRESHOLD+500)/*should be greater than promotion cutoff*/; i++) {
            // make an EXPLICIT set and populate with cardinality 1
            final HLL explicitHLL = newHLL(HLLType.EXPLICIT);
            explicitHLL.addRaw(random.nextLong());

            cumulativeUnionLine(output, hll, explicitHLL, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Unions an EMPTY accumulator with SPARSE HLLs, each
     * having one register set.
     *
     * Format: cumulative union
     * Tests:
     * - EMPTY U SPARSE
     * - SPARSE U SPARSE
     * - SPARSE promotion
     * - SPARSE U FULL
     */
    private static void sparseProbabilisticPromotionTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "sparse_promotion", TestType.UNION);

        final Random random = new Random(randomLong());

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.EMPTY);
        final HLL emptyHLL = newHLL(HLLType.EMPTY);
        cumulativeUnionLine(output, hll, emptyHLL, schemaVersion);


        for(int i=0; i<(SPARSE_THRESHOLD + 1000)/*should be greater than promotion cutoff*/; i++) {
            // make a SPARSE set and populate with cardinality 1
            final HLL sparseHLL = newHLL(HLLType.SPARSE);

            final int registerIndex = Math.abs(random.nextInt()) % REGISTER_COUNT;
            final int registerValue = ((Math.abs(random.nextInt()) % REGISTER_MAX_VALUE) + 1);
            final long rawValue = constructHLLValue(LOG2M, registerIndex, registerValue);
            sparseHLL.addRaw(rawValue);

            cumulativeUnionLine(output, hll, sparseHLL, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Unions an EMPTY accumulator with EXPLICIT HLLs, each having a single
     * random value, twice in a row to verify that the set properties are
     * satisfied.
     *
     * Format: cumulative union
     * Tests:
     * - EMPTY U EXPLICIT
     * - EXPLICIT U EXPLICIT
     */
    private static void explicitOverlapTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "explicit_explicit", TestType.UNION);

        final Random random = new Random(randomLong());

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.EMPTY);
        final HLL emptyHLL = newHLL(HLLType.EMPTY);

        cumulativeUnionLine(output, hll, emptyHLL, schemaVersion);

        for(int i=0; i<EXPLICIT_THRESHOLD; i++) {
            // make an EXPLICIT set and populate with cardinality 1
            final HLL explicitHLL = newHLL(HLLType.EXPLICIT);
            explicitHLL.addRaw(random.nextLong());

            // union it into the accumulator twice, to test overlap (cardinality should not change)
            cumulativeUnionLine(output, hll, explicitHLL, schemaVersion);
            cumulativeUnionLine(output, hll, explicitHLL, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Unions an EMPTY accumulator with SPARSE HLLs, each
     * having a single register set, twice in a row to verify that the set
     * properties are satisfied.
     *
     * Format: cumulative union
     * Tests:
     * - EMPTY U SPARSE
     * - SPARSE U SPARSE
     */
    private static void sparseProbabilisticOverlapTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "sparse_sparse", TestType.UNION);

        final Random random = new Random(randomLong());

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.EMPTY);
        final HLL emptyHLL = newHLL(HLLType.EMPTY);

        cumulativeUnionLine(output, hll, emptyHLL, schemaVersion);

        for(int i=0; i<SPARSE_THRESHOLD; i++) {
            // make a SPARSE set and populate with cardinality 1
            final HLL sparseHLL = newHLL(HLLType.SPARSE);
            final int registerIndex = Math.abs(random.nextInt()) % REGISTER_COUNT;
            final int registerValue = ((Math.abs(random.nextInt()) % REGISTER_MAX_VALUE) + 1);
            final long rawValue = constructHLLValue(LOG2M, registerIndex, registerValue);
            sparseHLL.addRaw(rawValue);

            cumulativeUnionLine(output, hll, sparseHLL, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Unions an EMPTY accumulator with FULL HLLs, each having
     * many registers set, twice in a row to verify that the set properties are
     * satisfied.
     *
     * Format: cumulative union
     * Tests:
     * - EMPTY U FULL
     * - FULL U FULL
     */
    private static void probabilisticUnionTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "probabilistic_probabilistic", TestType.UNION);

        final Random random = new Random(randomLong());

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.EMPTY);
        final HLL emptyHLL = newHLL(HLLType.EMPTY);
        cumulativeUnionLine(output, hll, emptyHLL, schemaVersion);

        for(int i=0; i<1000/*number of rows to generate*/; i++) {
            // make a FULL set and populate with
            final HLL fullHLL = newHLL(HLLType.FULL);
            final int elementCount = random.nextInt(10000/*arbitrary maximum cardinality*/);
            for(int j=0;j<elementCount;j++) {
                fullHLL.addRaw(random.nextLong());
            }

            cumulativeUnionLine(output, hll, fullHLL, schemaVersion);
        }

        output.flush();
        output.close();
    }

    /**
     * Unions an EMPTY accumulator with random HLLs.
     *
     * Format: cumulative union
     * Tests:
     * - hopefully all union possibilities
     */
    private static void globalUnionTest(final ISchemaVersion schemaVersion) throws IOException {
        final Writer output = openOutput(schemaVersion, "comprehensive", TestType.UNION);

        // the accumulator, starts empty
        final HLL hll = newHLL(HLLType.EMPTY);
        final HLL emptyHLL = newHLL(HLLType.EMPTY);

        cumulativeUnionLine(output, hll, emptyHLL, schemaVersion);

        for(int i=0; i<1000/*number of rows to generate*/; i++) {
            final HLL randomHLL = generateRandomHLL();
            cumulativeUnionLine(output, hll, randomHLL, schemaVersion);
        }

        output.flush();
        output.close();
    }

    // ========================================================================
    // Main
    public static void fullSuite(final ISchemaVersion schemaVersion) throws IOException {
        fullCardinalityCorrectionTest(schemaVersion);
        globalUnionTest(schemaVersion);
        globalStepTest(schemaVersion);
        probabilisticUnionTest(schemaVersion);
        explicitPromotionTest(schemaVersion);
        explicitOverlapTest(schemaVersion);
        sparseFullRepresentationTest(schemaVersion);
        sparseStepTest(schemaVersion);
        sparseRandomTest(schemaVersion);
        sparseEdgeTest(schemaVersion);
        sparseProbabilisticPromotionTest(schemaVersion);
        sparseProbabilisticOverlapTest(schemaVersion);
    }

    public static void main(String[] args) throws IOException {
        fullSuite(SerializationUtil.VERSION_ONE);
    }

    // ************************************************************************
    // Helpers
    /**
     * Shortcut for testing constructor, which uses the constants defined at
     * the top of the file as default parameters.
     *
     * @return a new {@link HLL} of specified type, which uses the parameters
     *         ({@link #LOG2M}, {@link #REGWIDTH}, {@link #EXPLICIT_THRESHOLD},
     *         and {@link #SPARSE_THRESHOLD}) specified above.
     */
    private static HLL newHLL(final HLLType type) {
        return newHLL(type);
    }

    /**
     * Returns the algorithm-specific cardinality of the specified {@link HLL}
     * as a {@link String} appropriate for comparison with the algorithm-specific
     * cardinality provided by the PostgreSQL implementation.
     *
     * @param  hll the HLL whose algorithm-specific cardinality is to be printed.
     *         This cannot be <code>null</code>.
     * @return the algorithm-specific cardinality of the instance as a PostgreSQL-
     *         compatible String. This will never be <code>null</code>
     */
    private static String stringCardinality(final HLL hll) {
        switch(hll.getType()) {
            case EMPTY:
                return "0";
            case EXPLICIT:/*promotion has not yet occurred*/
                return Long.toString(hll.cardinality());
            case SPARSE:
                return Double.toString(hll.sparseProbabilisticAlgorithmCardinality());
            case FULL:
                return Double.toString(hll.fullProbabilisticAlgorithmCardinality());
            default:
                throw new RuntimeException("Unknown HLL type " + hll.getType());
        }
    }

    /**
     * Generates a random HLL and populates it with random values.
     *
     * @return the populated HLL. This will never be <code>null</code>.
     */
    public static HLL generateRandomHLL() {
        final int randomTypeInt = randomIntBetween(0, HLLType.values().length - 1);
        final HLLType type;
        switch(randomTypeInt) {
            case 0:
                type = HLLType.EMPTY;
                break;
            case 1:
                type = HLLType.EXPLICIT;
                break;
            case 2:
                type = HLLType.FULL;
                break;
            case 3:
                type = HLLType.EMPTY;
                break;
            case 4:
                type = HLLType.SPARSE;
                break;
            default:
                throw new RuntimeException("Unassigned type int " + randomTypeInt);
        }

        final int cardinalityCap;
        final int cardinalityBaseline;

        switch(type) {
            case EMPTY:
                return newHLL(HLLType.EMPTY);
            case EXPLICIT:
                cardinalityCap = EXPLICIT_THRESHOLD;
                cardinalityBaseline = 1;
                break;
            case SPARSE:
                cardinalityCap = SPARSE_THRESHOLD;
                cardinalityBaseline = (EXPLICIT_THRESHOLD + 1);
                break;
            case FULL:
                cardinalityCap = 100000;
                cardinalityBaseline = (SPARSE_THRESHOLD*10);
                break;
            default:
                throw new RuntimeException("We should never be here.");
        }

        final HLL hll = newHLL(HLLType.EMPTY);
        for(int i=0; i<cardinalityBaseline; i++) {
            hll.addRaw(randomLong());
        }
        for(int i=0; i<randomInt(cardinalityCap - cardinalityBaseline); i++) {
            hll.addRaw(randomLong());
        }

        return hll;
    }

    /**
     * Opens a {@link Writer} and writes out an appropriate CSV header.
     *
     * @param  schemaVersion Schema version of the output. This cannot be
     *         <code>null</code>.
     * @param  description Description string used to build the filename.
     *         This cannot be <code>null</code>.
     * @param  type {@link TestType type} of the test file to be written.
     *         This cannot be <code>null</code>.
     * @return The opened {@link Writer writer}. This will never be <code>null</code>.
     */
    private static Writer openOutput(final ISchemaVersion schemaVersion, final String description, final TestType type) throws IOException {
        final String schemaVersionPrefix = "v"+ schemaVersion.schemaVersionNumber() + "_";
        final String header;
        final String filename;
        switch(type) {
            case ADD:
                header = "cardinality,raw_value,HLL\n";
                filename = schemaVersionPrefix + "cumulative_add_" + description + ".csv";
                break;
            case UNION:
                header = "cardinality,HLL,union_cardinality,union_HLL\n";
                filename = schemaVersionPrefix + "cumulative_union_" + description + ".csv";
                break;
            default:
                throw new RuntimeException("Unknown test type " + type);
        }

        final Writer output = Files.newBufferedWriter(
            Paths.get(OUTPUT_DIRECTORY, filename), StandardCharsets.UTF_8);
        output.write(header);
        output.flush();
        return output;
    }

    /**
     * Writes out a {@link TestType#ADD}-formatted test line.
     *
     * @param  output The output {@link Writer writer}. This cannot be <code>null</code>.
     * @param  hll The "accumulator" HLL instance. This cannot be <code>null</code>.
     * @param  rawValue The raw value added to the HLL.
     * @param  schemaVersion the schema with which to serialize the HLLs. This cannot
     *         be <code>null</code>.
     */
    private static void cumulativeAddLine(final Writer output, final HLL hll, final long rawValue, final ISchemaVersion schemaVersion) throws IOException {
        hll.addRaw(rawValue);
        final String accumulatorCardinality = stringCardinality(hll);

        output.write(accumulatorCardinality + "," + rawValue + "," + toByteA(hll, schemaVersion) + "\n");
        output.flush();
    }

    /**
     * Writes an initial line for a {@link TestType#ADD}-formatted test.
     *
     * @param  output The output {@link Writer writer}. This cannot be <code>null</code>.
     * @param  hll The "accumulator" HLL instance. This cannot be <code>null</code>.
     * @param  schemaVersion the schema with which to serialize the HLLs. This cannot
     *         be <code>null</code>.
     */
    private static void initLineAdd(final Writer output, final HLL hll, final ISchemaVersion schemaVersion) throws IOException {
        output.write(0 + "," + 0 + "," + toByteA(hll, schemaVersion) + "\n");
        output.flush();
    }

    /**
     * Writes out a {@link TestType#UNION}-formatted test line.
     *
     * @param  output The output {@link Writer writer}. This cannot be <code>null</code>.
     * @param  hll The "accumulator" HLL instance. This cannot be <code>null</code>.
     * @param  increment The "increment" HLL instance which will be unioned into
     *         the accumulator. This cannot be <code>null</code>.
     * @param  schemaVersion the schema with which to serialize the HLLs. This cannot
     *         be <code>null</code>.
     */
    private static void cumulativeUnionLine(final Writer output, final HLL hll, final HLL increment, final ISchemaVersion schemaVersion) throws IOException {
        hll.union(increment);

        final String incrementCardinality = stringCardinality(increment);
        final String accumulatorCardinality = stringCardinality(hll);
        output.write(incrementCardinality + "," + toByteA(increment, schemaVersion) + "," + accumulatorCardinality + "," + toByteA(hll, schemaVersion) + "\n");
        output.flush();
    }

    /**
     * Serializes a HLL to Postgres 9 'bytea' hex-format, for CSV ingest.
     *
     * @param  hll the HLL to serialize. This cannot be <code>null</code>.
     * @param  schemaVersion the schema with which to serialize the HLLs. This cannot
     *         be <code>null</code>.
     * @return a PostgreSQL 'bytea' string representing the HLL.
     */
    private static String toByteA(final HLL hll, final ISchemaVersion schemaVersion) {
        final byte[] bytes = hll.toBytes(schemaVersion);
        return ("\\x" + NumberUtil.toHex(bytes, 0, bytes.length));
    }

    /**
     * Indicates what kind of test output a test will generate.
     */
    private static enum TestType {
        /**
         * This type of test is characterized by values being added to an
         * accumulator HLL whose serialized representation (after the value is added)
         * is printed to each line along with the cardinality and added value.
         */
        ADD,
        /**
         * This type of test is characterized by HLLs being unioned into an
         * accumulator HLL whose serialized representation (after the HLL is
         * union'd) is printed to each line along with the cardinalities and the
         * serialized representation of the HLL union'd in.
         */
        UNION;
    }
}
