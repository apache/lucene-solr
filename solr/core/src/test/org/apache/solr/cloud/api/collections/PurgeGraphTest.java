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

package org.apache.solr.cloud.api.collections;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.api.collections.DeleteBackupCmd.PurgeGraph;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.hasItems;
/**
 * Unit tests for {@link PurgeGraph}
 */
public class PurgeGraphTest extends SolrTestCaseJ4 {

    private BackupRepository repository;
    private URI baseLocationUri;
    private BackupFilePaths backupPaths;

    @Before
    public void setUpRepo() throws Exception {
        repository = new LocalFileSystemRepository();
        baseLocationUri = repository.createDirectoryURI(createTempDir("backup_files_" + UUID.randomUUID().toString()).toAbsolutePath().toString());
        backupPaths = new BackupFilePaths(repository, baseLocationUri);

        backupPaths.createIncrementalBackupFolders();
    }

    @Test
    public void testGraphBuildingOnNoBackups() throws Exception {
        PurgeGraph purgeGraph = new PurgeGraph();
        purgeGraph.build(repository, backupPaths.getBackupLocation());
        purgeGraph.findDeletableNodes(repository, backupPaths);

        assertEquals(0, purgeGraph.backupIdDeletes.size());
        assertEquals(0, purgeGraph.shardBackupMetadataDeletes.size());
        assertEquals(0, purgeGraph.indexFileDeletes.size());
    }

    @Test
    public void testUnreferencedIndexFilesAreDeleted() throws Exception {
        // Backup 0 files
        createBackupIdFile(0, "shard1", "shard2");
        createShardMetadataFile(0, "shard1", "uniqName1", "uniqName2");
        createShardMetadataFile(0, "shard2", "uniqName3", "uniqName4");
        // Backup 1 files
        createBackupIdFile(1, "shard1", "shard2");
        createShardMetadataFile(1, "shard1", "uniqName5", "uniqName6");
        createShardMetadataFile(1, "shard2", "uniqName7", "uniqName8");
        // Valid, referenced index files
        createUniquelyNamedIndexFile("uniqName1", "uniqName2", "uniqName3", "uniqName4", "uniqName5", "uniqName6", "uniqName7", "uniqName8");
        // Single orphaned index file
        createUniquelyNamedIndexFile("someUnreferencedName");

        PurgeGraph purgeGraph = new PurgeGraph();
        purgeGraph.build(repository, backupPaths.getBackupLocation());

        assertEquals(0, purgeGraph.backupIdDeletes.size());
        assertEquals(0, purgeGraph.shardBackupMetadataDeletes.size());
        assertEquals(1, purgeGraph.indexFileDeletes.size());
        assertEquals("someUnreferencedName", purgeGraph.indexFileDeletes.get(0));
    }

    // TODO - this seems a bit extreme - should this really occur by default?
    @Test
    public void testEntireBackupPointFlaggedForDeletionIfAnyIndexFilesMissing() throws Exception {
        // Backup 0 files
        createBackupIdFile(0, "shard1", "shard2");
        createShardMetadataFile(0, "shard1", "uniqName1", "uniqName2");
        createShardMetadataFile(0, "shard2", "uniqName3", "uniqName4");
        // Valid, referenced index files - 'uniqName3' is missing!
        createUniquelyNamedIndexFile("uniqName1", "uniqName2", "uniqName4");

        PurgeGraph purgeGraph = new PurgeGraph();
        purgeGraph.build(repository, backupPaths.getBackupLocation());

        // All files associated with backup '0' should be flagged for deletion since the required file 'uniqName3' is missing.
        assertEquals(1, purgeGraph.backupIdDeletes.size());
        assertThat(purgeGraph.backupIdDeletes, hasItems("backup_0.properties"));
        assertEquals(2, purgeGraph.shardBackupMetadataDeletes.size());
        assertThat(purgeGraph.shardBackupMetadataDeletes, hasItems("md_shard1_0.json", "md_shard2_0.json"));
        assertEquals(3, purgeGraph.indexFileDeletes.size());
        assertThat(purgeGraph.indexFileDeletes, hasItems("uniqName1", "uniqName2", "uniqName4"));

        // If a subsequent backup relies on an index file (uniqName4) that was previously only used by the invalid backup '0', that file will not be flagged for deletion.
//        createBackupIdFile(1, "shard1", "shard2");
//        createShardMetadataFile(1, "shard1", "uniqName5", "uniqName6");
//        createShardMetadataFile(1, "shard2", "uniqName4");
//        createUniquelyNamedIndexFile("uniqName5", "uniqName6");
//
//        assertEquals(1, purgeGraph.backupIdDeletes.size());
//        assertThat(purgeGraph.backupIdDeletes, hasItems("backup_0.properties"));
//        assertEquals(2, purgeGraph.shardBackupMetadataDeletes.size());
//        assertThat(purgeGraph.shardBackupMetadataDeletes, hasItems("md_shard1_0.json", "md_shard2_0.json"));
//        // NOTE that 'uniqName4' is NOT marked for deletion
//        assertEquals(2, purgeGraph.indexFileDeletes.size());
//        assertThat(purgeGraph.indexFileDeletes, hasItems("uniqName1", "uniqName2"));
    }

    @Test
    public void testUnreferencedShardMetadataFilesAreDeleted() throws Exception {
        // Backup 0 files
        createBackupIdFile(0, "shard1", "shard2");
        createShardMetadataFile(0, "shard1", "uniqName1", "uniqName2");
        createShardMetadataFile(0, "shard2", "uniqName3", "uniqName4");
        // Extra shard unreferenced by backup_0.properties
        createShardMetadataFile(0, "shard3", "uniqName5", "uniqName6");
        createUniquelyNamedIndexFile("uniqName1", "uniqName2", "uniqName3", "uniqName4", "uniqName5", "uniqName6");

        PurgeGraph purgeGraph = new PurgeGraph();
        purgeGraph.build(repository, backupPaths.getBackupLocation());

        assertEquals(0, purgeGraph.backupIdDeletes.size());
        assertEquals(1, purgeGraph.shardBackupMetadataDeletes.size());
        assertThat(purgeGraph.shardBackupMetadataDeletes, hasItems("md_shard3_0.json"));
        assertEquals(2, purgeGraph.indexFileDeletes.size());
        assertThat(purgeGraph.indexFileDeletes, hasItems("uniqName5", "uniqName6"));

        // If a subsequent backup relies on an index file (uniqName5) that was previously only used by the orphaned 'shard3' metadata file, that file should no longer be flagged for deletion
//        createBackupIdFile(1, "shard1", "shard2");
//        createShardMetadataFile(1, "shard1", "uniqName7");
//        createShardMetadataFile(1, "shard2", "uniqName5", "uniqName8");
//
//        purgeGraph = new PurgeGraph();
//        purgeGraph.build(repository, backupPaths.getBackupLocation());
//
//        assertEquals(0, purgeGraph.backupIdDeletes.size());
//        assertEquals(1, purgeGraph.shardBackupMetadataDeletes.size());
//        assertThat(purgeGraph.shardBackupMetadataDeletes, hasItems("md_shard3_0.json"));
//        assertEquals(1, purgeGraph.indexFileDeletes.size());
//        assertThat(purgeGraph.indexFileDeletes, hasItems("uniqName6"));
    }

    private void createBackupIdFile(int backupId, String... shardNames) throws Exception {
        final BackupProperties createdProps = BackupProperties.create("someBackupName", "someCollectionName",
                "someExtCollectionName", "someConfigName");
        for (String shardName : shardNames) {
            createdProps.putAndGetShardBackupIdFor(shardName, backupId);
        }

        URI dest = repository.resolve(backupPaths.getBackupLocation(), BackupFilePaths.getBackupPropsName(new BackupId(backupId)));
        try (Writer propsWriter = new OutputStreamWriter(repository.createOutput(dest), StandardCharsets.UTF_8)) {
            createdProps.store(propsWriter);
        }
    }

    private void createShardMetadataFile(int backupId, String shardName, String... uniqueIndexFileNames) throws Exception {
        final ShardBackupMetadata createdShardMetadata = ShardBackupMetadata.empty();
        for (String uniqueIndexFileName : uniqueIndexFileNames) {
            createdShardMetadata.addBackedFile(uniqueIndexFileName, uniqueIndexFileName + "_local", new Checksum(1L, 1));
        }
        createdShardMetadata.store(repository, backupPaths.getShardBackupMetadataDir(), new ShardBackupId(shardName, new BackupId(backupId)));
    }

    private void createUniquelyNamedIndexFile(String... uniqNames) throws Exception {
        for (String uniqName : uniqNames) {
            final String randomContent = "some value";
            final URI indexFileUri = repository.resolve(backupPaths.getIndexDir(), uniqName);
            try (OutputStream os = repository.createOutput(indexFileUri)) {
                os.write(randomContent.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
