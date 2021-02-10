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
package org.apache.solr.handler;

import org.apache.lucene.index.IndexCommit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;

@LogLevel("org.apache.solr.cloud.api.collections.BackupCmd=DEBUG;org.apache.solr.handler.admin.BackupCoreOp=DEBUG;org.apache.solr.core.backup.repository.LocalFileSystemRepository=DEBUG")
public class TestIncrementalCoreBackup extends SolrTestCaseJ4 {
    @Before // unique core per test
    public void coreInit() throws Exception {
        initCore("solrconfig.xml", "schema.xml");
    }
    @After // unique core per test
    public void coreDestroy() throws Exception {
        deleteCore();
    }

    @Test
    public void testBackupWithDocsNotSearchable() throws Exception {
        //See SOLR-11616 to see when this issue can be triggered

        assertU(adoc("id", "1"));
        assertU(commit());

        assertU(adoc("id", "2"));

        assertU(commit("openSearcher", "false"));
        assertQ(req("q", "*:*"), "//result[@numFound='1']");
        assertQ(req("q", "id:1"), "//result[@numFound='1']");
        assertQ(req("q", "id:2"), "//result[@numFound='0']");

        //call backup
        final URI location = createAndBootstrapLocationForBackup();
        final ShardBackupId shardBackupId = new ShardBackupId("shard1", BackupId.zero());

        final CoreContainer cores = h.getCoreContainer();
        cores.getAllowPaths().add(Paths.get(location));
        try (final CoreAdminHandler admin = new CoreAdminHandler(cores)) {
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, shardBackupId.getIdAsString())
                            , resp);
            assertNull("Backup should have succeeded", resp.getException());
            simpleBackupCheck(location, shardBackupId);
        }
    }

    public void testBackupBeforeFirstCommit() throws Exception {

        // even w/o a user sending any data, the SolrCore initialiation logic should have automatically created
        // an "empty" commit point that can be backed up...
        final IndexCommit empty = h.getCore().getDeletionPolicy().getLatestCommit();
        assertNotNull(empty);

        // white box sanity check that the commit point of the "reader" available from SolrIndexSearcher
        // matches the commit point that IDPW claims is the "latest"
        //
        // this is important to ensure that backup/snapshot behavior is consistent with user expection
        // when using typical commit + openSearcher
        assertEquals(empty, h.getCore().withSearcher(s -> s.getIndexReader().getIndexCommit()));

        assertEquals(1L, empty.getGeneration());
        assertNotNull(empty.getSegmentsFileName());
        final String initialEmptyIndexSegmentFileName = empty.getSegmentsFileName();

        final CoreContainer cores = h.getCoreContainer();
        final CoreAdminHandler admin = new CoreAdminHandler(cores);
        final URI location = createAndBootstrapLocationForBackup();

        final ShardBackupId firstShardBackup = new ShardBackupId("shard1", BackupId.zero());
        { // first a backup before we've ever done *anything*...
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, firstShardBackup.getIdAsString()),
                            resp);
            assertNull("Backup should have succeeded", resp.getException());
            simpleBackupCheck(location, firstShardBackup, initialEmptyIndexSegmentFileName);
        }

        { // Empty (named) snapshot..
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATESNAPSHOT.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "empty_snapshotA"),
                            resp);
            assertNull("Snapshot A should have succeeded", resp.getException());
        }

        assertU(adoc("id", "1")); // uncommitted

        final ShardBackupId secondShardBackupId = new ShardBackupId("shard1", new BackupId(1));
        { // second backup w/uncommited docs
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, secondShardBackupId.getIdAsString()),
                            resp);
            assertNull("Backup should have succeeded", resp.getException());
            simpleBackupCheck(location, secondShardBackupId, initialEmptyIndexSegmentFileName);
        }

        { // Second empty (named) snapshot..
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATESNAPSHOT.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "empty_snapshotB"),
                            resp);
            assertNull("Snapshot A should have succeeded", resp.getException());
        }

        // Committing the doc now should not affect the existing backups or snapshots...
        assertU(commit());

        for (ShardBackupId shardBackupId: Arrays.asList(firstShardBackup, secondShardBackupId)) {
            simpleBackupCheck(location, shardBackupId, initialEmptyIndexSegmentFileName);
        }

        // Make backups from each of the snapshots and check they are still empty as well...
        {
            final ShardBackupId thirdShardBackup = new ShardBackupId("shard1", new BackupId(2));
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "empty_snapshotA",
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, thirdShardBackup.getIdAsString()),
                            resp);
            assertNull("Backup from snapshot empty_snapshotA should have succeeded", resp.getException());
            simpleBackupCheck(location, thirdShardBackup, initialEmptyIndexSegmentFileName);
        }
        {
            final ShardBackupId fourthShardBackup = new ShardBackupId("shard1", new BackupId(3));
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "empty_snapshotB",
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, fourthShardBackup.getIdAsString()),
                            resp);
            assertNull("Backup from snapshot empty_snapshotB should have succeeded", resp.getException());
            simpleBackupCheck(location, fourthShardBackup, initialEmptyIndexSegmentFileName);
        }
        admin.close();
    }

    /**
     * Tests that a softCommit does not affect what data is in a backup
     */
    public void testBackupAfterSoftCommit() throws Exception {

        // sanity check empty index...
        assertQ(req("q", "id:42"), "//result[@numFound='0']");
        assertQ(req("q", "id:99"), "//result[@numFound='0']");
        assertQ(req("q", "*:*"), "//result[@numFound='0']");

        // hard commit one doc...
        assertU(adoc("id", "99"));
        assertU(commit());
        assertQ(req("q", "id:99"), "//result[@numFound='1']");
        assertQ(req("q", "*:*"), "//result[@numFound='1']");

        final IndexCommit oneDocCommit = h.getCore().getDeletionPolicy().getLatestCommit();
        assertNotNull(oneDocCommit);
        final String oneDocSegmentFile = oneDocCommit.getSegmentsFileName();

        final CoreContainer cores = h.getCoreContainer();
        final CoreAdminHandler admin = new CoreAdminHandler(cores);
        final URI location = createAndBootstrapLocationForBackup();

        final ShardBackupId firstShardBackupId = new ShardBackupId("shard1", BackupId.zero());
        { // take an initial 'backup1a' containing our 1 document
            final SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "name", "backup1a",
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, firstShardBackupId.getIdAsString()),
                            resp);
            assertNull("Backup should have succeeded", resp.getException());
            simpleBackupCheck(location, firstShardBackupId, oneDocSegmentFile);
        }

        { // and an initial "snapshot1a' that should eventually match
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATESNAPSHOT.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "snapshot1a"),
                            resp);
            assertNull("Snapshot 1A should have succeeded", resp.getException());
        }

        // now we add our 2nd doc, and make it searchable, but we do *NOT* hard commit it to the index dir...
        assertU(adoc("id", "42"));
        assertU(commit("softCommit", "true", "openSearcher", "true"));

        assertQ(req("q", "id:99"), "//result[@numFound='1']");
        assertQ(req("q", "id:42"), "//result[@numFound='1']");
        assertQ(req("q", "*:*"), "//result[@numFound='2']");


        final ShardBackupId secondShardBackupId = new ShardBackupId("shard1", new BackupId(1));
        { // we now have an index with two searchable docs, but a new 'backup1b' should still
            // be identical to the previous backup...
            final SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, secondShardBackupId.getIdAsString()),
                            resp);
            assertNull("Backup should have succeeded", resp.getException());
            simpleBackupCheck(location, secondShardBackupId, oneDocSegmentFile);
        }

        { // and a second "snapshot1b' should also still be identical
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATESNAPSHOT.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "snapshot1b"),
                            resp);
            assertNull("Snapshot 1B should have succeeded", resp.getException());
        }

        // Hard Committing the 2nd doc now should not affect the existing backups or snapshots...
        assertU(commit());
        simpleBackupCheck(location, firstShardBackupId, oneDocSegmentFile); // backup1a
        simpleBackupCheck(location, secondShardBackupId, oneDocSegmentFile); // backup1b

        final ShardBackupId thirdShardBackupId = new ShardBackupId("shard1", new BackupId(2));
        { // But we should be able to confirm both docs appear in a new backup (not based on a previous snapshot)
            final SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, thirdShardBackupId.getIdAsString()),
                            resp);
            assertNull("Backup should have succeeded", resp.getException());
            // TODO This doesn't actually check that backup has both docs!  Can we do better than this without doing a full restore?
            // Maybe validate the new segments_X file at least to show that it's picked up the latest commit?
            simpleBackupCheck(location, thirdShardBackupId);
        }

        // if we go back and create backups from our earlier snapshots they should still only
        // have 1 expected doc...
        // Make backups from each of the snapshots and check they are still empty as well...
        final ShardBackupId fourthShardBackupId = new ShardBackupId("shard1", new BackupId(3));
        {
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "snapshot1a",
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, fourthShardBackupId.getIdAsString()),
                            resp);
            assertNull("Backup of snapshot1a should have succeeded", resp.getException());
            simpleBackupCheck(location, fourthShardBackupId, oneDocSegmentFile);
        }
        final ShardBackupId fifthShardBackupId = new ShardBackupId("shard1", new BackupId(4));
        {
            SolrQueryResponse resp = new SolrQueryResponse();
            admin.handleRequestBody
                    (req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                            "core", DEFAULT_TEST_COLLECTION_NAME,
                            "commitName", "snapshot1b",
                            "location", location.getPath(),
                            CoreAdminParams.SHARD_BACKUP_ID, fifthShardBackupId.getIdAsString()),
                            resp);
            assertNull("Backup of snapshot1b should have succeeded", resp.getException());
            simpleBackupCheck(location, fifthShardBackupId, oneDocSegmentFile);
        }

        admin.close();
    }

    /**
     * Check that the backup metadata file exists, and the corresponding index files can be found.
     */
    private static void simpleBackupCheck(URI locationURI, ShardBackupId shardBackupId, String... expectedIndexFiles) throws IOException {
        try(BackupRepository backupRepository = h.getCoreContainer().newBackupRepository(null)) {
            final BackupFilePaths backupFilePaths = new BackupFilePaths(backupRepository, locationURI);

            // Ensure that the overall file structure looks correct.
            assertTrue(backupRepository.exists(locationURI));
            assertTrue(backupRepository.exists(backupFilePaths.getIndexDir()));
            assertTrue(backupRepository.exists(backupFilePaths.getShardBackupMetadataDir()));
            final String metadataFilename = shardBackupId.getBackupMetadataFilename();
            final URI shardBackupMetadataURI = backupRepository.resolve(backupFilePaths.getShardBackupMetadataDir(), metadataFilename);
            assertTrue(backupRepository.exists(shardBackupMetadataURI));

            // Ensure that all files listed in the shard-meta file are stored in the index dir
            final ShardBackupMetadata backupMetadata = ShardBackupMetadata.from(backupRepository,
                    backupFilePaths.getShardBackupMetadataDir(), shardBackupId);
            for (String indexFileName : backupMetadata.listUniqueFileNames()) {
                final URI indexFileURI = backupRepository.resolve(backupFilePaths.getIndexDir(), indexFileName);
                assertTrue("Expected " + indexFileName + " to exist in " + backupFilePaths.getIndexDir(), backupRepository.exists(indexFileURI));
            }


            // Ensure that the expected filenames (if any are provided) exist
            for (String expectedIndexFile : expectedIndexFiles) {
                assertTrue("Expected backup to hold a renamed copy of " + expectedIndexFile,
                        backupMetadata.listOriginalFileNames().contains(expectedIndexFile));
            }
        }
    }

    private URI createAndBootstrapLocationForBackup() throws IOException {
        final File locationFile = createTempDir().toFile();
        final String location = locationFile.getAbsolutePath();

        h.getCoreContainer().getAllowPaths().add(locationFile.toPath());
        try (BackupRepository backupRepo = h.getCoreContainer().newBackupRepository(null)) {
            final BackupFilePaths backupFilePaths = new BackupFilePaths(backupRepo, backupRepo.createURI(location));
            backupFilePaths.createIncrementalBackupFolders();
            return backupRepo.createURI(location);
        }
    }
}

