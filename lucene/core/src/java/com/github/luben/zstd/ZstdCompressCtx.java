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

public class ZstdCompressCtx extends AutoCloseBase {

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

    public void setInput(byte[] input, int off, int len) {
        if (off < 0 || len < 0 || off > input.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        bytes = input;
        offset = off;
        this.len =  len;
        this.totLength = off+len;
    }

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
    public ZstdCompressCtx() {
        init();
        if (0 == nativePtr) {
            throw new IllegalStateException("ZSTD_createCompressCtx failed");
        }
        storeFence();
    }

    void  doClose() {
        if (nativePtr != 0) {
            free();
            nativePtr = 0;
        }
    }


    public void reset()
    {
        nativePtr = 0;
        init();
        free();
    }
    /**
     * Set compression level
     * @param level compression level, default: 3
     */
    public ZstdCompressCtx setLevel(int level) {
        if (nativePtr == 0) {
            throw new IllegalStateException("Compression context is closed");
        }
        acquireSharedLock();
        setLevel0(level);
        releaseSharedLock();
        return this;
    }

    private native void setLevel0(int level);

    /**
     * Enable or disable compression checksums
     * @param checksumFlag A 32-bits checksum of content is written at end of frame, default: false
     */
    public ZstdCompressCtx setChecksum(boolean checksumFlag) {
        if (nativePtr == 0) {
            throw new IllegalStateException("Compression context is closed");
        }
        acquireSharedLock();
        setChecksum0(checksumFlag);
        releaseSharedLock();
        return this;
    }
    private native void setChecksum0(boolean checksumFlag);

    public int compressByteArray(byte[] dstBuff, int dstOffset, int dstSize, byte[] srcBuff, int srcOffset, int srcSize) {
        if (nativePtr == 0) {
            throw new IllegalStateException("Compression context is closed");
        }

        acquireSharedLock();

        try {
            long size = compressByteArray0(dstBuff, dstOffset, dstSize, srcBuff, srcOffset, srcSize);
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

    private native long compressByteArray0(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize);





    public int compress(byte[] dst, byte[] src) {
        return compressByteArray(dst, 0, dst.length, src, 0, src.length);
    }


}