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
