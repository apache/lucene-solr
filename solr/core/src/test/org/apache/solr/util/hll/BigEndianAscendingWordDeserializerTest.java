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

import java.util.Random;


import org.apache.solr.SolrTestCase;
import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;

/**
 * Unit and smoke tests for {@link BigEndianAscendingWordDeserializer}.
 */
public class BigEndianAscendingWordDeserializerTest extends SolrTestCase {
    /**
     * Error checking tests for constructor.
     */
    @Test
    public void constructorErrorTest() {
        // word length too small
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new BigEndianAscendingWordDeserializer(0/*wordLength, below minimum of 1*/, 0/*bytePadding, arbitrary*/, new byte[1]/*bytes, arbitrary, not used here*/);
        });
        assertTrue(e.getMessage().contains("Word length must be"));

        // word length too large
        e = expectThrows(IllegalArgumentException.class, () -> {
            new BigEndianAscendingWordDeserializer(65/*wordLength, above maximum of 64*/, 0/*bytePadding, arbitrary*/, new byte[1]/*bytes, arbitrary, not used here*/);
        });
        assertTrue(e.getMessage().contains("Word length must be"));

        // byte padding negative
        e = expectThrows(IllegalArgumentException.class, () -> {
            new BigEndianAscendingWordDeserializer(5/*wordLength, arbitrary*/, -1/*bytePadding, too small*/, new byte[1]/*bytes, arbitrary, not used here*/);
        });
        assertTrue(e.getMessage().contains("Byte padding must be"));
    }

    /**
     * Smoke test using 64-bit short words and special word values.
     */
    @Test
    public void smokeTest64BitWord() {
        final BigEndianAscendingWordSerializer serializer =
            new BigEndianAscendingWordSerializer(64/*wordLength*/,
                                                 5/*wordCount*/,
                                                 0/*bytePadding, arbitrary*/);

        // Check that the sign bit is being preserved.
        serializer.writeWord(-1L);
        serializer.writeWord(-112894714L);

        // Check "special" values
        serializer.writeWord(0L);
        serializer.writeWord(Long.MAX_VALUE);
        serializer.writeWord(Long.MIN_VALUE);

        final byte[] bytes = serializer.getBytes();

        final BigEndianAscendingWordDeserializer deserializer =
            new BigEndianAscendingWordDeserializer(64/*wordLength*/, 0/*bytePadding*/, bytes);

        assertEquals(deserializer.totalWordCount(), 5/*wordCount*/);

        assertEquals(deserializer.readWord(), -1L);
        assertEquals(deserializer.readWord(), -112894714L);
        assertEquals(deserializer.readWord(), 0L);
        assertEquals(deserializer.readWord(), Long.MAX_VALUE);
        assertEquals(deserializer.readWord(), Long.MIN_VALUE);
    }

    /**
     * A smoke/fuzz test for ascending (from zero) word values.
     */
    @Test
    public void ascendingSmokeTest() {
        for(int wordLength=5; wordLength<65; wordLength++) {
            runAscendingTest(wordLength, 3/*bytePadding, arbitrary*/, 100000/*wordCount, arbitrary*/);
        }
    }

    /**
     * A smoke/fuzz test for random word values.
     */
    @Test
    public void randomSmokeTest() {
        for(int wordLength=5; wordLength<65; wordLength++) {
            runRandomTest(wordLength, 3/*bytePadding, arbitrary*/, 100000/*wordCount, arbitrary*/);
        }
    }

    // ------------------------------------------------------------------------
    /**
     * Runs a test which serializes and deserializes random word values.
     *
     * @param wordLength the length of words to test
     * @param bytePadding the number of bytes padding the byte array
     * @param wordCount the number of word values to test
     */
    private static void runRandomTest(final int wordLength, final int bytePadding, final int wordCount) {
        final long seed = randomLong();
        final Random random = new Random(seed);
        final Random verificationRandom = new Random(seed);

        final long wordMask;
        if(wordLength == 64) {
            wordMask = ~0L;
        } else {
            wordMask = (1L << wordLength) - 1L;
        }

        final BigEndianAscendingWordSerializer serializer =
            new BigEndianAscendingWordSerializer(wordLength/*wordLength, arbitrary*/,
                                                 wordCount,
                                                 bytePadding/*bytePadding, arbitrary*/);

        for(int i=0; i<wordCount; i++) {
            final long value = random.nextLong() & wordMask;
            serializer.writeWord(value);
        }

        final byte[] bytes = serializer.getBytes();

        final BigEndianAscendingWordDeserializer deserializer =
            new BigEndianAscendingWordDeserializer(wordLength, bytePadding, bytes);

        assertEquals(deserializer.totalWordCount(), wordCount);
        for(int i=0; i<wordCount; i++) {
            assertEquals(deserializer.readWord(), (verificationRandom.nextLong() & wordMask));
        }
    }

    /**
     * Runs a test which serializes and deserializes ascending (from zero) word values.
     *
     * @param wordLength the length of words to test
     * @param bytePadding the number of bytes padding the byte array
     * @param wordCount the number of word values to test
     */
    private static void runAscendingTest(final int wordLength, final int bytePadding, final int wordCount) {
        final long wordMask;
        if(wordLength == 64) {
            wordMask = ~0L;
        } else {
            wordMask = (1L << wordLength) - 1L;
        }

        final BigEndianAscendingWordSerializer serializer =
            new BigEndianAscendingWordSerializer(wordLength/*wordLength, arbitrary*/,
                                                 wordCount,
                                                 bytePadding/*bytePadding, arbitrary*/);

        for(long i=0; i<wordCount; i++) {
            serializer.writeWord(i & wordMask);
        }

        final byte[] bytes = serializer.getBytes();

        final BigEndianAscendingWordDeserializer deserializer =
            new BigEndianAscendingWordDeserializer(wordLength, bytePadding, bytes);

        assertEquals(deserializer.totalWordCount(), wordCount);
        for(long i=0; i<wordCount; i++) {
            assertEquals(deserializer.readWord(), i & wordMask);
        }
    }
}
