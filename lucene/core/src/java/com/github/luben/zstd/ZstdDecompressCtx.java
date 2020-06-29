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
    private ZstdDictDecompress decompression_dict = null;

    private native void init();

    private native void free();
    private byte[] bytes;
    private int len;
    private int offset;
    private int totLength;
    public boolean finish = false;

    public ZstdDictDecompress getNeedsDictionary()
    {
        return decompression_dict;
    }
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
    public ZstdDecompressCtx loadDict(ZstdDictDecompress dict) {
        if (nativePtr == 0) {
            throw new IllegalStateException("Decompression context is closed");
        }
        acquireSharedLock();
        dict.acquireSharedLock();
        try {
            long result = loadDDictFast0(dict);
            if (Zstd.isError(result)) {
                throw new ZstdException(result);
            }
            // keep a reference to the dictionary so it's not garbage collected
            decompression_dict = dict;
        } finally {
            dict.releaseSharedLock();
            releaseSharedLock();
        }
        return this;
    }
    private native long loadDDictFast0(ZstdDictDecompress dict);

    /**
     * Load decompression dictionary.
     *
     * @param dict the dictionary or `null` to remove loaded dictionary
     */
    public ZstdDecompressCtx loadDict(byte[] dict) {
        if (nativePtr == 0) {
            throw new IllegalStateException("Compression context is closed");
        }
        acquireSharedLock();
        try {
            long result = loadDDict0(dict);
            if (Zstd.isError(result)) {
                throw new ZstdException(result);
            }
            decompression_dict = null;
        } finally {
            releaseSharedLock();
        }
        return this;
    }
    private native long loadDDict0(byte[] dict);

    /**
     * Decompresses buffer 'srcBuff' into buffer 'dstBuff' using this ZstdDecompressCtx.
     *
     * Destination buffer should be sized to be larger of equal to the originalSize.
     * This is a low-level function that does not take into account or affect the `limit`
     * or `position` of source or destination buffers.
     *
     * @param dstBuff the destination buffer - must be direct
     * @param dstOffset the start offset of 'dstBuff'
     * @param dstSize the size of 'dstBuff'
     * @param srcBuff the source buffer - must be direct
     * @param srcOffset the start offset of 'srcBuff'
     * @param srcSize the size of 'srcBuff'
     * @return the number of bytes decompressed into destination buffer (originalSize)
     */
    public int decompressDirectByteBuffer(ByteBuffer dstBuff, int dstOffset, int dstSize, ByteBuffer srcBuff, int srcOffset, int srcSize) {
        if (nativePtr == 0) {
            throw new IllegalStateException("Decompression context is closed");
        }
        if (!srcBuff.isDirect()) {
            throw new IllegalArgumentException("srcBuff must be a direct buffer");
        }
        if (!dstBuff.isDirect()) {
            throw new IllegalArgumentException("dstBuff must be a direct buffer");
        }

        acquireSharedLock();

        try {
            long size = decompressDirectByteBuffer0(dstBuff, dstOffset, dstSize, srcBuff, srcOffset, srcSize);
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

    private native long decompressDirectByteBuffer0(ByteBuffer dst, int dstOffset, int dstSize, ByteBuffer src, int srcOffset, int srcSize);

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

    /** Covenience methods */

    /**
     * Decompresses buffer 'srcBuff' into buffer 'dstBuff' using this ZstdDecompressCtx.
     *
     * Destination buffer should be sized to be larger of equal to the originalSize.
     * @param dstBuf the destination buffer - must be direct. It is assumed that the `position()` of this buffer marks the offset
     *               at which the decompressed data are to be written, and that the `limit()` of this buffer is the maximum
     *               decompressed data size to allow.
     *               <p>
     *               When this method returns successfully, its `position()` will be set to its current `position()` plus the
     *               decompressed size of the data.
     *               </p>
     * @param srcBuf the source buffer - must be direct. It is assumed that the `position()` of this buffer marks the beginning of the
     *               compressed data to be decompressed, and that the `limit()` of this buffer marks its end.
     *               <p>
     *               When this method returns successfully, its `position()` will be set to the initial `limit()`.
     *               </p>
     * @return the size of the decompressed data.
     */
    public int decompress(ByteBuffer dstBuf, ByteBuffer srcBuf) throws ZstdException {

        int size = decompressDirectByteBuffer(dstBuf,  // decompress into dstBuf
                dstBuf.position(),                      // write decompressed data at offset position()
                dstBuf.limit() - dstBuf.position(),     // write no more than limit() - position()
                srcBuf,                                 // read compressed data from srcBuf
                srcBuf.position(),                      // read starting at offset position()
                srcBuf.limit() - srcBuf.position());    // read no more than limit() - position()
        srcBuf.position(srcBuf.limit());
        dstBuf.position(dstBuf.position() + size);
        return size;
    }

    public ByteBuffer decompress(ByteBuffer srcBuf, int originalSize) throws ZstdException {
        ByteBuffer dstBuf = ByteBuffer.allocateDirect(originalSize);
        int size = decompressDirectByteBuffer(dstBuf, 0, originalSize, srcBuf, srcBuf.position(), srcBuf.limit());
        srcBuf.position(srcBuf.limit());
        // Since we allocated the buffer ourselves, we know it cannot be used to hold any further decompressed data,
        // so leave the position at zero where the caller surely wants it, ready to read
        return dstBuf;
    }

    public int decompress(byte[] dst, byte[] src) {
        return decompressByteArray(dst, 0, dst.length, src, 0, src.length);
    }

    public byte[] decompress(byte[] src, int originalSize) throws ZstdException {
        byte[] dst = new byte[originalSize];
        int size = decompress(dst, src);
        if (size != originalSize) {
            return Arrays.copyOfRange(dst, 0, size);
        } else {
            return dst;
        }
    }
}
