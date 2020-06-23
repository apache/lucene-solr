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

package org.apache.lucene.util.compress;

public class ZstdException extends RuntimeException {
    private long code;

    /**
     * Construct a ZstdException from the result of a Zstd library call.
     *
     * The error code and message are automatically looked up using
     * Zstd.getErrorCode and Zstd.getErrorName.
     *
     * @param result the return value of a Zstd library call
     */
    public ZstdException(long result) {
        this(Zstd.getErrorCode(result), Zstd.getErrorName(result));
    }

    /**
     * Construct a ZstdException with a manually-specified error code and message.
     *
     * No transformation of either the code or message is done. It is advised
     * that one of the Zstd.err*() is used to obtain a stable error code.
     *
     * @param code a Zstd error code
     * @param message the exception's message
     */
    public ZstdException(long code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * Get the Zstd error code that caused the exception.
     *
     * This will likely correspond to one of the Zstd.err*() methods, but the
     * Zstd library may return error codes that are not yet stable. In such
     * cases, this method will return the code reported by Zstd, but it will
     * not correspond to any of the Zstd.err*() methods.
     *
     * @return a Zstd error code
     */
    public long getErrorCode() {
        return code;
    }
}
