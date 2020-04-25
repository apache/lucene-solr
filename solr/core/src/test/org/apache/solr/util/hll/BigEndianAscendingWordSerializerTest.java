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

import java.util.Arrays;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Unit tests for {@link BigEndianAscendingWordSerializer}.
 */
public class BigEndianAscendingWordSerializerTest extends SolrTestCase {
    /**
     * Error checking tests for constructor.
     */
    @Test
    public void constructorErrorTest() {
        // word length too small
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,  () -> {
            new BigEndianAscendingWordSerializer(0/*wordLength, below minimum of 1*/, 1/*wordCount, arbitrary*/, 0/*bytePadding, arbitrary*/);;
        });
        assertTrue(e.getMessage().contains("Word length must be"));

        // word length too large
        e = expectThrows(IllegalArgumentException.class,  () -> {
            new BigEndianAscendingWordSerializer(65/*wordLength, above max of 64*/, 1/*wordCount, arbitrary*/, 0/*bytePadding, arbitrary*/);
        });
        assertTrue(e.getMessage().contains("Word length must be"));

        // word count negative
        e = expectThrows(IllegalArgumentException.class,  () -> {
            new BigEndianAscendingWordSerializer(5/*wordLength, arbitrary*/, -1/*wordCount, too small*/, 0/*bytePadding, arbitrary*/);
        });
        assertTrue(e.getMessage().contains("Word count must be"));

        // byte padding negative
        e = expectThrows(IllegalArgumentException.class,  () -> {
            new BigEndianAscendingWordSerializer(5/*wordLength, arbitrary*/, 1/*wordCount, arbitrary*/, -1/*bytePadding, too small*/);
        });
        assertTrue(e.getMessage().contains("Byte padding must be"));
    }

    /**
     * Tests runtime exception thrown at premature call to {@link BigEndianAscendingWordSerializer#getBytes()}.
     */
    @Test
    public void earlyGetBytesTest() {
        final BigEndianAscendingWordSerializer serializer =
            new BigEndianAscendingWordSerializer(5/*wordLength, arbitrary*/,
                                                 1/*wordCount*/,
                                                 0/*bytePadding, arbitrary*/);

        // getBytes without enough writeWord should throw
        RuntimeException e = expectThrows(RuntimeException.class, serializer::getBytes);
        assertTrue(e.getMessage().contains("Not all words"));
    }

    /**
     */
    @Test
    public void smokeTestExplicitParams() {
        final int shortWordLength = 64/*longs used in LongSetSlab*/;

        {// Should work on an empty sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     0/*wordCount*/,
                                                     0/*bytePadding, none*/);

            assert(Arrays.equals(serializer.getBytes(), new byte[0]));
        }
        {// Should work on a byte-divisible sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     2/*wordCount*/,
                                                     0/*bytePadding, none*/);

            serializer.writeWord(0xBAAAAAAAAAAAAAACL);
            serializer.writeWord(0x8FFFFFFFFFFFFFF1L);

            // Bytes:
            // ======
            // 0xBA 0xAA 0xAA 0xAA 0xAA 0xAA 0xAA 0xAC
            // 0x8F 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xF1
            //
            // -70 -86 ...                        -84
            // -113 -1 ...                        -15
            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { -70, -86, -86, -86, -86, -86, -86, -84,
                                                      -113, -1, -1, -1, -1, -1, -1, -15 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
        {// Should pad the array correctly.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     1/*wordCount*/,
                                                     1/*bytePadding*/);

            serializer.writeWord(1);
            // 1 byte leading padding | value 1 | trailing padding
            // 0000 0000 | 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0001
            // 0x00 | 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01
            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 1 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
    }

    /**
     * Smoke test for typical parameters used in practice.
     */
    @Test
    public void smokeTestProbabilisticParams() {
        // XXX: revisit this
        final int shortWordLength = 5;
        {// Should work on an empty sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     0/*wordCount*/,
                                                     0/*bytePadding, none*/);

            assert(Arrays.equals(serializer.getBytes(), new byte[0]));
        }
        {// Should work on a non-byte-divisible sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     3/*wordCount*/,
                                                     0/*bytePadding, none*/);

            serializer.writeWord(9);
            serializer.writeWord(31);
            serializer.writeWord(1);

            // The values:
            // -----------
            // 9     |31    |1     |padding

            // Corresponding bits:
            // ------------------
            // 0100 1|111 11|00 001|0

            // And the hex/decimal (remember Java bytes are signed):
            // -----------------------------------------------------
            // 0100 1111 -> 0x4F -> 79
            // 1100 0010 -> 0xC2 -> -62

            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { 79, -62 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
        {// Should work on a byte-divisible sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     8/*wordCount*/,
                                                     0/*bytePadding, none*/);

            for(int i=1; i<9; i++) {
                serializer.writeWord(i);
            }

            // Values: 1-8
            // Corresponding bits:
            // ------------------
            // 00001
            // 00010
            // 00011
            // 00100
            // 00101
            // 00110
            // 00111
            // 01000

            // And the hex:
            // ------------
            // 0000 1000 => 0x08 => 8
            // 1000 0110 => 0x86 => -122
            // 0100 0010 => 0x62 => 66
            // 1001 1000 => 0x98 => -104
            // 1110 1000 => 0xE8 => -24

            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { 8, -122, 66, -104, -24 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
        {// Should pad the array correctly.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     1/*wordCount*/,
                                                     1/*bytePadding*/);

            serializer.writeWord(1);
            // 1 byte leading padding | value 1 | trailing padding
            // 0000 0000 | 0000 1|000
            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { 0, 8 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
    }

    /**
     * Smoke test for typical parameters used in practice.
     */
    @Test
    public void smokeTestSparseParams() {
        // XXX: revisit
        final int shortWordLength = 17;
        {// Should work on an empty sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     0/*wordCount*/,
                                                     0/*bytePadding, none*/);

            assert(Arrays.equals(serializer.getBytes(), new byte[0]));
        }
        {// Should work on a non-byte-divisible sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     3/*wordCount*/,
                                                     0/*bytePadding, none*/);

            serializer.writeWord(9);
            serializer.writeWord(42);
            serializer.writeWord(75);

            // The values:
            // -----------
            // 9                    |42                   |75                   |padding

            // Corresponding bits:
            // ------------------
            // 0000 0000 0000 0100 1|000 0000 0000 1010 10|00 0000 0000 1001 011|0 0000

            // And the hex/decimal (remember Java bytes are signed):
            // -----------------------------------------------------
            // 0000 0000 -> 0x00 -> 0
            // 0000 0100 -> 0x04 -> 4
            // 1000 0000 -> 0x80 -> -128
            // 0000 1010 -> 0x0A -> 10
            // 1000 0000 -> 0x80 -> -128
            // 0000 1001 -> 0x09 -> 9
            // 0110 0000 -> 0x60 -> 96

            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { 0, 4, -128, 10, -128, 9, 96 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
        {// Should work on a byte-divisible sequence, with no padding.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     8/*wordCount*/,
                                                     0/*bytePadding, none*/);

            for(int i=1; i<9; i++) {
                serializer.writeWord(i);
            }

            // Values: 1-8
            // Corresponding bits:
            // ------------------
            // 0000 0000 0000 0000 1
            // 000 0000 0000 0000 10
            // 00 0000 0000 0000 011
            // 0 0000 0000 0000 0100

            // 0000 0000 0000 0010 1
            // 000 0000 0000 0001 10
            // 00 0000 0000 0000 111
            // 0 0000 0000 0000 1000

            // And the hex:
            // ------------
            // 0000 0000 -> 0x00 -> 0
            // 0000 0000 -> 0x00 -> 0
            // 1000 0000 -> 0x80 -> -128
            // 0000 0000 -> 0x00 -> 0
            // 1000 0000 -> 0x80 -> -128
            // 0000 0000 -> 0x00 -> 0
            // 0110 0000 -> 0x60 -> 96
            // 0000 0000 -> 0x00 -> 0
            // 0100 0000 -> 0x40 -> 64
            // 0000 0000 -> 0x00 -> 0
            // 0010 1000 -> 0x28 -> 40
            // 0000 0000 -> 0x00 -> 0
            // 0001 1000 -> 0x18 -> 24
            // 0000 0000 -> 0x00 -> 0
            // 0000 1110 -> 0x0D -> 14
            // 0000 0000 -> 0x00 -> 0
            // 0000 1000 -> 0x08 -> 8

            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { 0, 0, -128, 0, -128, 0, 96, 0, 64, 0, 40, 0, 24, 0, 14, 0, 8 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
        {// Should pad the array correctly.
            final BigEndianAscendingWordSerializer serializer =
                new BigEndianAscendingWordSerializer(shortWordLength,
                                                     1/*wordCount*/,
                                                     1/*bytePadding*/);

            serializer.writeWord(1);
            // 1 byte leading padding | value 1 | trailing padding
            // 0000 0000 | 0000 0000 0000 0000 1|000 0000
            // 0x00 0x00 0x00 0x80
            final byte[] bytes = serializer.getBytes();
            final byte[] expectedBytes = new byte[] { 0, 0, 0, -128 };
            assertTrue(Arrays.equals(bytes, expectedBytes));
        }
    }
}
