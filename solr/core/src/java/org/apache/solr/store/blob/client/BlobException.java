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
package org.apache.solr.store.blob.client;

/**
 * Parent class of blob store related issues. Likely to change and maybe disappear but good enough for a PoC.
 */
public class BlobException extends Exception {
    public BlobException(Throwable cause) {
        super(cause);
    }

    public BlobException(String message) {
        super(message);
    }

    public BlobException(String message, Throwable cause) {
        super(message, cause);
    }
}
