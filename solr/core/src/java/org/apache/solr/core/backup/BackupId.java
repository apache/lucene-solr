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
 * Represents the ID of a particular backup.
 *
 * Backup IDs are used to track different backup points stored at the same backup-location under the same backup-name.
 *
 * Incremental backups can have any non-negative integer as an ID, and ID's are expected to increase sequentially.
 *
 * Traditional (now-deprecated) 'full-snapshot' backups only support a single backup point per name per location.  So
 * these all have the same ID value of {@link #TRADITIONAL_BACKUP}
 */
public class BackupId implements Comparable<BackupId>{
    public static final int TRADITIONAL_BACKUP = -1;

    public final int id;

    public BackupId(int id) {
        this.id = id;
    }

    public static BackupId zero() {
        return new BackupId(0);
    }

    public static BackupId traditionalBackup() {
        return new BackupId(TRADITIONAL_BACKUP);
    }

    public BackupId nextBackupId() {
        return new BackupId(id+1);
    }

    public int getId() {
        return id;
    }

    @Override
    public int compareTo(BackupId o) {
        return Integer.compare(this.id, o.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupId backupId = (BackupId) o;
        return id == backupId.id;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
