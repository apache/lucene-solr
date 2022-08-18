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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Utility class for dealing with LZ4 compressed data, such as state.json in Zookeeper if configured to be compressed
 */
public class CompressionUtil {

    private final static byte[] ZLIB_MAGIC = new byte[]{ 0x78, 0x1 };
    private final static int COMPRESSED_SIZE_MAGIC_NUMBER = 2018370979;

    /**
     * Uses the hex magic number for zlib compression '78 01' to check if the bytes are compressed
     * @param data - the bytes to check for compression
     * @return true if the data is zlib compressed based on the first 2 bytes, false otherwise
     */
    public static boolean isCompressedBytes(byte[] data) {
        if (data == null || data.length < 2) return false;
        return ZLIB_MAGIC[0] == data[0] && ZLIB_MAGIC[1] == data[1];
    }

    /**
     * Decompresses zlib compressed bytes, returning the uncompressed data as a byte[]
     * @param data the input zlib compressed data to decompress
     * @return the decompressed bytes
     * @throws DataFormatException - The data is not zlib compressed data
     */
    public static byte[] decompressBytes(byte[] data) throws DataFormatException {
        if (data == null) return null;
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(data, 0, data.length);
            // Attempt to get the decompressed size from trailing bytes, this will be present if compressed by Solr
            ByteBuffer bb = ByteBuffer.wrap(data, data.length - 8, 8);
            int decompressedSize = bb.getInt();
            int xoredSize = bb.getInt();
            if ((decompressedSize ^ COMPRESSED_SIZE_MAGIC_NUMBER) != xoredSize) {
                // Take best guess of decompressed size since it wasn't included in trailing bytes, assume a 5:1 ratio
                decompressedSize = 5 * data.length;
            }
            byte[] buf = new byte[decompressedSize];
            int actualDecompressedSize = 0;
            while (!inflater.finished()) {
                if (actualDecompressedSize >= buf.length) {
                    buf = Arrays.copyOf(buf, (int) (buf.length * 1.5));
                }
                actualDecompressedSize += inflater.inflate(buf, actualDecompressedSize, decompressedSize);
            }
            if (buf.length != actualDecompressedSize) {
                buf = Arrays.copyOf(buf, actualDecompressedSize);
            }
            return buf;
        } finally {
            inflater.end();
        }
    }

    /**
     * Compresses bytes into zlib compressed bytes using java native compression
     * @param data the input uncompressed data to be compressed
     * @return zlib compressed bytes
     */
    public static byte[] compressBytes(byte[] data) {
        Deflater compressor = new Deflater(Deflater.BEST_SPEED);
        try {
            compressor.setInput(data);
            compressor.finish();
            byte[] buf = new byte[data.length + 8];
            int compressedSize = 0;
            while (!compressor.finished()) {
                if (compressedSize >= buf.length) {
                    buf = Arrays.copyOf(buf, (int) (buf.length * 1.5));
                }
                compressedSize += compressor.deflate(buf, compressedSize, buf.length - compressedSize);
            }

            buf = Arrays.copyOf(buf, compressedSize + 8);

            // Include the decompressed size and xored decompressed size in trailing bytes, this makes decompression
            // efficient while also being compatible with alternative zlib implementations
            ByteBuffer.wrap(buf, compressedSize, 8).putInt(data.length).putInt(data.length ^ COMPRESSED_SIZE_MAGIC_NUMBER);
            return buf;
        } finally {
            compressor.end();
        }
    }
}
