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

package org.apache.solr.core.backup;

/**
 * Aggregate stats from multiple {@link ShardBackupMetadata}
 *
 * Counted stats may represent multiple shards within a given {@link BackupId}, or span multiple different {@link BackupId BackupIds}.
 */
public class AggregateBackupStats {
    private int numFiles = 0;
    private long totalSize = 0;

    public AggregateBackupStats() {
    }

    public void add(ShardBackupMetadata shardBackupMetadata) {
        numFiles += shardBackupMetadata.numFiles();
        totalSize += shardBackupMetadata.totalSize();
    }

    public int getNumFiles() {
        return numFiles;
    }

    public long getTotalSize() {
        return totalSize;
    }
}
