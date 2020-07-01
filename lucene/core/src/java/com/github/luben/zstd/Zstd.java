/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.luben.zstd;

import java.nio.ByteBuffer;



public class Zstd {

    static {
        Native.load();
    }



    /**
     * Compresses buffer 'src' into buffer 'dst'.
     *
     * Destination buffer should be sized to handle worst cases situations (input
     * data not compressible). Worst case size evaluation is provided by function
     * ZSTD_compressBound().
     *
     * @param dst the destination buffer
     * @param src the source buffer
     * @param level compression level
     * @param checksumFlag flag to enable or disable checksum
     * @return  the number of bytes written into buffer 'dst' or an error code if
     *          it fails (which can be tested using ZSTD_isError())
     */
    public static long compress(byte[] dst, byte[] src, int level, boolean checksumFlag) {
        ZstdCompressCtx ctx = new ZstdCompressCtx();
        try {
            ctx.setLevel(level);
            ctx.setChecksum(checksumFlag);
            return (long) ctx.compress(dst, src);
        } finally {
            ctx.close();
        }
    }

    /**
     * Compresses buffer 'src' into buffer 'dst'.
     *
     * Destination buffer should be sized to handle worst cases situations (input
     * data not compressible). Worst case size evaluation is provided by function
     * ZSTD_compressBound().
     *
     * @param dst the destination buffer
     * @param src the source buffer
     * @param level compression level
     * @return  the number of bytes written into buffer 'dst' or an error code if
     *          it fails (which can be tested using ZSTD_isError())
     */
    public static long compress(byte[] dst, byte[] src, int level) {
        return compress(dst, src, level, false);
    }
    public static native String  getErrorName(long code);
    public static native long    getErrorCode(long code);
    public static native boolean isError(long code);
    public static native long errGeneric();
}