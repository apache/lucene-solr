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

import org.apache.solr.core.backup.repository.BackupRepository;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.solr.core.backup.BackupId.TRADITIONAL_BACKUP;
import static org.apache.solr.core.backup.BackupManager.ZK_STATE_DIR;

/**
 * Utility class for getting paths related to backups, or parsing information out of those paths.
 */
public class BackupFilePaths {

    private static final Pattern BACKUP_PROPS_ID_PTN = Pattern.compile("backup_([0-9]+).properties");
    private BackupRepository repository;
    private URI backupLoc;

    /**
     * Create a BackupFilePaths object.
     *
     * @param repository the repository; used primarily to resolve URIs.
     * @param backupLoc the root location for a named backup.  For traditional backups this is expected to take the form
     *                  baseLocation/backupName.  For incremental backups this is expected to be of the form
     *                  baseLocation/backupName/collectionName.
     */
    public BackupFilePaths(BackupRepository repository, URI backupLoc) {
        this.repository = repository;
        this.backupLoc = backupLoc;
    }

    /**
     * Return a URI for the 'index' location, responsible for holding index files for all backups at this location.
     *
     * Only valid for incremental backups.
     */
    public URI getIndexDir() {
        return repository.resolveDirectory(backupLoc, "index");
    }

    /**
     * Return a URI for the 'shard_backup_metadata' location, which contains metadata files about each shard backup.
     *
     * Only valid for incremental backups.
     */
    public URI getShardBackupMetadataDir() {
        return repository.resolveDirectory(backupLoc, "shard_backup_metadata");
    }

    public URI getBackupLocation() {
        return backupLoc;
    }

    /**
     * Create all locations required to store an incremental backup.
     *
     * @throws IOException for issues encountered using repository to create directories
     */
    public void createIncrementalBackupFolders() throws IOException {
        repository.createDirectory(backupLoc);
        repository.createDirectory(getIndexDir());
        repository.createDirectory(getShardBackupMetadataDir());
    }

    /**
     * Get the directory name used to hold backed up ZK state
     *
     * Valid for both incremental and traditional backups.
     *
     * @param id the ID of the backup in question
     */
    public static String getZkStateDir(BackupId id) {
        if (id.id == TRADITIONAL_BACKUP) {
            return ZK_STATE_DIR;
        }
        return String.format(Locale.ROOT, "%s_%d", ZK_STATE_DIR, id.id);
    }

    /**
     * Get the filename of the top-level backup properties file
     *
     * Valid for both incremental and traditional backups.
     *
     * @param id the ID of the backup in question
     */
    public static String getBackupPropsName(BackupId id) {
        if (id.id == TRADITIONAL_BACKUP) {
            return BackupManager.TRADITIONAL_BACKUP_PROPS_FILE;
        }
        return getBackupPropsName(id.id);
    }

    /**
     * Identify all strings which appear to be the filename of a top-level backup properties file.
     *
     * Only valid for incremental backups.
     *
     * @param listFiles a list of strings, filenames which may or may not correspond to backup properties files
     */
    public static List<BackupId> findAllBackupIdsFromFileListing(String[] listFiles) {
        List<BackupId> result = new ArrayList<>();
        for (String file: listFiles) {
            Matcher m = BACKUP_PROPS_ID_PTN.matcher(file);
            if (m.find()) {
                result.add(new BackupId(Integer.parseInt(m.group(1))));
            }
        }

        return result;
    }

    /**
     * Identify the string from an array of filenames which represents the most recent top-level backup properties file.
     *
     * Only valid for incremental backups.
     *
     * @param listFiles a list of strings, filenames which may or may not correspond to backup properties files.
     */
    public static Optional<BackupId> findMostRecentBackupIdFromFileListing(String[] listFiles) {
        return findAllBackupIdsFromFileListing(listFiles).stream().max(Comparator.comparingInt(o -> o.id));
    }

    /**
     * Builds the URI for the backup location given the user-provided 'location' and backup 'name'.
     *
     * @param repository the backup repository, used to list files and resolve URI's.
     * @param location a URI representing the repository location holding each backup name
     * @param backupName the specific backup name to create a URI for
     */
    public static URI buildExistingBackupLocationURI(BackupRepository repository, URI location, String backupName) throws IOException {
        final URI backupNameUri = repository.resolveDirectory(location, backupName);
        final String[] entries = repository.listAll(backupNameUri);
        final boolean incremental = ! Arrays.stream(entries).anyMatch(entry -> entry.equals(BackupManager.TRADITIONAL_BACKUP_PROPS_FILE));
        if (incremental) {
            // Incremental backups have an additional URI path component representing the collection that was backed up.
            // This collection directory is the path assumed by other backup code.
            if (entries.length != 1) {
                throw new IllegalStateException("Incremental backup URI [" + backupNameUri + "] expected to hold a single directory. Number found: " + entries.length);
            }
            final String collectionName = entries[0];
            return repository.resolveDirectory(backupNameUri, entries[0]);
        } else {
            return backupNameUri;
        }
    }

    private static String getBackupPropsName(int id) {
        return String.format(Locale.ROOT, "backup_%d.properties", id);
    }
}
