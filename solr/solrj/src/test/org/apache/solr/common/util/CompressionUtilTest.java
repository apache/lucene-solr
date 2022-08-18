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

package org.apache.solr.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.DataFormatException;

public class CompressionUtilTest extends SolrTestCase {

    @Test
    public void isCompressedBytes() {
        assertFalse(CompressionUtil.isCompressedBytes(null));
        assertFalse(CompressionUtil.isCompressedBytes(new byte[0]));
        assertFalse(CompressionUtil.isCompressedBytes(new byte[1]));
        assertFalse(CompressionUtil.isCompressedBytes(new byte[2]));
        assertFalse(CompressionUtil.isCompressedBytes(new byte[3]));

        Random rd = random();
        byte[] arr = new byte[500];
        rd.nextBytes(arr);

        byte[] compressedBytes = CompressionUtil.compressBytes(arr);
        assertFalse(CompressionUtil.isCompressedBytes(arr));
        assertTrue(CompressionUtil.isCompressedBytes(compressedBytes));
    }

    @Test
    public void decompressCompressedBytes() throws DataFormatException {
        // "Some test data\n" as compressed bytes
        byte[] testBytes = new byte[]{120, 1, 11, -50, -49, 77, 85, 40, 73, 45, 46, 81, 72, 73, 44, 73, -28, 2, 0, 43, -36, 5, 57};
        byte[] decompressedBytes = CompressionUtil.decompressBytes(testBytes);
        assertEquals("Some test data\n", new String(decompressedBytes));
    }

    @Test
    public void compressBytes() {
        // "Some test data\n" as compressed bytes
        byte[] testBytes = new byte[]{120, 1, 11, -50, -49, 77, 85, 40, 73, 45, 46, 81, 72, 73, 44, 73, -28, 2, 0, 43, -36, 5, 57};
        byte[] compressedBytes = CompressionUtil.compressBytes("Some test data\n".getBytes());
        int decompressedSize = ByteBuffer.wrap(compressedBytes, compressedBytes.length - 8, 4).getInt();
        int xoredSize = ByteBuffer.wrap(compressedBytes, compressedBytes.length - 4, 4).getInt();
        assertEquals(xoredSize, decompressedSize ^ 2018370979);
        assertEquals("Some test data\n".getBytes().length, decompressedSize);
        assertArrayEquals(testBytes, ArrayUtil.copyOfSubArray(compressedBytes, 0, compressedBytes.length - 8));
    }
}