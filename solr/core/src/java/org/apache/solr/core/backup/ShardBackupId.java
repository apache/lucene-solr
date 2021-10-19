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

import java.util.Arrays;

/**
 * Represents the ID of a particular backup point for a particular shard.
 *
 * ShardBackupId's only need be unique within a given collection and backup location/name, so in practice they're formed
 * by combining the shard name with the {@link BackupId} in the form: "md_$SHARDNAME_BACKUPID".
 *
 * ShardBackupId's are most often used as a filename to store shard-level metadata for the backup.  See
 * {@link ShardBackupMetadata} for more information.
 *
 * @see ShardBackupMetadata
 */
public class ShardBackupId {
    private static final String FILENAME_SUFFIX = ".json";
    private final String shardName;
    private final BackupId containingBackupId;

    public ShardBackupId(String shardName, BackupId containingBackupId) {
        this.shardName = shardName;
        this.containingBackupId = containingBackupId;
    }

    public String getShardName() {
        return shardName;
    }

    public BackupId getContainingBackupId() {
        return containingBackupId;
    }

    public String getIdAsString() {
        return "md_" + shardName + "_" + containingBackupId.getId();
    }

    public String getBackupMetadataFilename() {
        return getIdAsString() + FILENAME_SUFFIX;
    }

    public static ShardBackupId from(String idString) {
        final String[] idComponents = idString.split("_");
        if (idComponents.length < 3 || ! idString.startsWith("md_")) {
            throw new IllegalArgumentException("Unable to parse invalid ShardBackupId: " + idString);
        }

        final String backupIdString = idComponents[idComponents.length - 1]; // Backup ID is always the last component
        final String shardId = String.join("_", Arrays.asList(idComponents).subList(1, idComponents.length - 1));
        final BackupId containingBackupId = new BackupId(Integer.parseInt(backupIdString));
        return new ShardBackupId(shardId, containingBackupId);
    }

    public static ShardBackupId fromShardMetadataFilename(String filenameString) {
        if (! filenameString.endsWith(FILENAME_SUFFIX)) {
            throw new IllegalArgumentException("'filenameString' arg [" + filenameString + "] does not appear to be a filename");
        }
        final String idString = filenameString.substring(0, filenameString.length() - FILENAME_SUFFIX.length());
        return from(idString);
    }
}
