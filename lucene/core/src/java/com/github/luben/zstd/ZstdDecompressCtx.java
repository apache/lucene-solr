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
import java.util.Arrays;

public class ZstdDecompressCtx extends AutoCloseBase {

    static {
        Native.load();
    }

    private long nativePtr = 0;

    private native void init();

    private native void free();
    private byte[] bytes;
    private int len;
    private int offset;
    private int totLength;
    public boolean finish = false;


    public void finish()
    {
        finish = true;
    }

    public boolean end()
    {
        return finish;
    }
    public boolean needsInput()
    {
        if(finish == true)
            return false;
        return true;
    }
    /**
     * Create a context for faster compress operations
     * One such context is required for each thread - put this in a ThreadLocal.
     */
    public ZstdDecompressCtx() {
        init();
        if (0 == nativePtr) {
            throw new IllegalStateException("ZSTD_createDeCompressCtx failed");
        }
        storeFence();
    }

    void doClose() {
        if (nativePtr != 0) {
            free();
            nativePtr = 0;
        }
    }

    /**
     * Load decompression dictionary
     *
     * @param dict the dictionary or `null` to remove loaded dictionary
     */

    private native long loadDDict0(byte[] dict);



    /**
     * Decompresses byte array 'srcBuff' into byte array 'dstBuff' using this ZstdDecompressCtx.
     *
     * Destination buffer should be sized to be larger of equal to the originalSize.
     *
     * @param dstBuff the destination buffer
     * @param dstOffset the start offset of 'dstBuff'
     * @param dstSize the size of 'dstBuff'
     * @param srcBuff the source buffer
     * @param srcOffset the start offset of 'srcBuff'
     * @param srcSize the size of 'srcBuff'
     * @return the number of bytes decompressed into destination buffer (originalSize)
     */
    public int decompressByteArray(byte[] dstBuff, int dstOffset, int dstSize, byte[] srcBuff, int srcOffset, int srcSize) {
        if (nativePtr == 0) {
            throw new IllegalStateException("Decompression context is closed");
        }

        acquireSharedLock();

        try {

            long size = decompressByteArray0(dstBuff, dstOffset, dstSize, srcBuff, srcOffset, srcSize);
            if (Zstd.isError(size)) {
                throw new ZstdException(size);
            }
            if (size > Integer.MAX_VALUE) {
                throw new ZstdException(Zstd.errGeneric(), "Output size is greater than MAX_INT");
            }
            return (int) size;
        } finally {
            releaseSharedLock();
        }
    }

    private native long decompressByteArray0(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize);



    public int decompress(byte[] dst, byte[] src) {
        return decompressByteArray(dst, 0, dst.length, src, 0, src.length);
    }


}
