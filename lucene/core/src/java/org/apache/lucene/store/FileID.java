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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * <p>A (hopefully) unique ID for a file</p>
 *
 * <p>This creates an ID for a file which can be compared to other IDs in order to attempt to
 * verify that both are the same file. This comparison is done on an best-effort basis and is
 * useful to ensure that a file has not been removed and recreated.</p>
 *
 * <p>Implementation details:</p>
 *
 * <ul>
 *   <li>On plattforms that support {@link BasicFileAttributes#fileKey} this will be used to verify that
 *   two {@link FileID}s refer to same file. On Unix-like systems, the key consists of the <em>device id</em>
 *   and <em>inode</em> allowing detection of recreated files at a very low error rate.</li>
 *   <li>On all other platforms, namely those that don't support {@link BasicFileAttributes#fileKey}, the
 *   creation time or modification time, if available, is used in an attempt to ensure two {@link FileID}s
 *   refer to the same file .</li>
 * </ul>
 */
public class FileID {
    private final FileTime creationTime;
    private final Object fileKey;

    public FileID(Path path) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        fileKey = attributes.fileKey();
        creationTime = fileKey == null ? attributes.creationTime() : null;
    }

    public boolean isSameFileAs(FileID other) {
        if (this.fileKey != null) {
            if (other.fileKey == null) {
                return false;
            }
            return this.fileKey.equals(other.fileKey);
        } else {
            return this.creationTime.equals(other.creationTime);
        }
    }

    @Override
    public String toString() {
        return String.format("FileID(fileKey=%s, creationTime=%s)", this.fileKey, this.creationTime);
    }
}
