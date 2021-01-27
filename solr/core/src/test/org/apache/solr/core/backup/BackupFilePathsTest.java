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

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link BackupFilePaths}
 */
public class BackupFilePathsTest {

    @Test
    public void testGetBackupPropsName() {
        final BackupId initialId = BackupId.zero();
        final BackupId subsequentId = initialId.nextBackupId();

        assertEquals("backup_0.properties", BackupFilePaths.getBackupPropsName(initialId));
        assertEquals("backup_1.properties", BackupFilePaths.getBackupPropsName(subsequentId));
    }

    @Test
    public void testFindAllBackupIdCanReturnEmpty() {
        final List<BackupId> foundBackupIds = BackupFilePaths.findAllBackupIdsFromFileListing(new String[0]);
        assertTrue(foundBackupIds.isEmpty());
    }

    @Test
    public void testFindAllBackupPropertiesFiles() {
        final String[] backupFiles = new String[] {"aaa", "baa.properties", "backup.properties", "backup_1.properties",
                "backup_2.properties", "backup_neqewq.properties", "backup999.properties"};
        final List<BackupId> foundBackupIds = BackupFilePaths.findAllBackupIdsFromFileListing(backupFiles);

        assertEquals(2, foundBackupIds.size());
        assertEquals(new BackupId(1), foundBackupIds.get(0));
        assertEquals(new BackupId(2), foundBackupIds.get(1));
    }

    @Test
    public void testFindMostRecentBackupIdCanReturnEmpty() {
        Optional<BackupId> op = BackupFilePaths.findMostRecentBackupIdFromFileListing(new String[0]);
        assertFalse(op.isPresent());
    }

    @Test
    public void testFindMostRecentBackupPropertiesFile() {
        final String[] backupFiles = new String[] {"aaa", "baa.properties", "backup.properties", "backup_1.properties",
                "backup_2.properties", "backup_neqewq.properties", "backup999.properties"};
        final Optional<BackupId> filenameOption = BackupFilePaths.findMostRecentBackupIdFromFileListing(backupFiles);
        assertTrue(filenameOption.isPresent());
        assertEquals(new BackupId(2), filenameOption.get());
    }
}
