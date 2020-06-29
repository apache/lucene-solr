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


public class ZstdDictCompress extends SharedDictBase {

    static {
        Native.load();
    }

    private long nativePtr = 0;
    private int level = 3;

    private native void init(byte[] dict, int dict_offset, int dict_size, int level);

    private native void free();

    /**
     * Convenience constructor to create a new dictionary for use with fast compress
     *
     * @param dict  buffer containing dictionary to load/parse with exact length
     * @param level compression level
     */
    public ZstdDictCompress(byte[] dict, int level) {
        this(dict, 0, dict.length, level);
    }

    /**
     * Create a new dictionary for use with fast compress
     *
     * @param dict   buffer containing dictionary
     * @param offset the offset into the buffer to read from
     * @param length number of bytes to use from the buffer
     * @param level  compression level
     */
    public ZstdDictCompress(byte[] dict, int offset, int length, int level) {
        this.level = level;
        if (dict.length - offset < 0) {
            throw new IllegalArgumentException("Dictionary buffer is to short");
        }

        init(dict, offset, length, level);

        if (0 == nativePtr) {
            throw new IllegalStateException("ZSTD_createCDict failed");
        }
        // Ensures that even if ZstdDictCompress is created and published through a race, no thread could observe
        // nativePtr == 0.
        storeFence();
    }

    int level() {
        return level;
    }

    @Override
    void  doClose() {
        if (nativePtr != 0) {
            free();
            nativePtr = 0;
        }
    }
}
