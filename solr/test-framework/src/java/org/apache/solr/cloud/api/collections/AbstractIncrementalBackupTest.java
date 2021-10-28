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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.TrackingBackupRepository;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.core.TrackingBackupRepository.copiedFiles;

/**
 * Used to test the incremental method of backup/restoration (as opposed to the deprecated 'full snapshot' method).
 *
 * For a similar test harness for snapshot backup/restoration see {@link AbstractCloudBackupRestoreTestCase}
 */
public abstract class AbstractIncrementalBackupTest extends SolrCloudTestCase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static long docsSeed; // see indexDocs()
    protected static final int NUM_SHARDS = 2;//granted we sometimes shard split to get more
    protected static final int REPL_FACTOR = 2;
    protected static final String BACKUPNAME_PREFIX = "mytestbackup";
    protected static final String BACKUP_REPO_NAME = "trackingBackupRepository";

    protected String testSuffix = "test1";

    @BeforeClass
    public static void createCluster() throws Exception {
        docsSeed = random().nextLong();
        System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    }

    @Before
    public void setUpTrackingRepo() {
        TrackingBackupRepository.clear();
    }

    /**
     * @return The name of the collection to use.
     */
    public abstract String getCollectionNamePrefix();

    public String getCollectionName(){
        return getCollectionNamePrefix() + "_" + testSuffix;
    }

    public void setTestSuffix(String testSuffix) {
        this.testSuffix = testSuffix;
    }

    /**
     * @return The absolute path for the backup location.
     *         Could return null.
     */
    public abstract String getBackupLocation();

    @Test
    public void testSimple() throws Exception {
        setTestSuffix("testbackupincsimple");
        final String backupCollectionName = getCollectionName();
        final String restoreCollectionName = backupCollectionName + "_restore";

        CloudSolrClient solrClient = cluster.getSolrClient();

        CollectionAdminRequest
                .createCollection(backupCollectionName, "conf1", NUM_SHARDS, 1)
                .process(solrClient);
        int totalIndexedDocs = indexDocs(backupCollectionName, true);
        String backupName = BACKUPNAME_PREFIX + testSuffix;
        try (BackupRepository repository = cluster.getJettySolrRunner(0).getCoreContainer()
                .newBackupRepository(Optional.of(BACKUP_REPO_NAME))) {
            String backupLocation = repository.getBackupLocation(getBackupLocation());
            long t = System.nanoTime();
            int expectedDocsForFirstBackup = totalIndexedDocs;
            CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
                    .setLocation(backupLocation)
                    .setIncremental(true)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .processAndWait(cluster.getSolrClient(), 100);
            long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t);
            log.info("Created backup with {} docs, took {}ms", totalIndexedDocs, timeTaken);
            totalIndexedDocs += indexDocs(backupCollectionName, true);

            t = System.nanoTime();
            CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
                    .setLocation(backupLocation)
                    .setIncremental(true)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .processAndWait(cluster.getSolrClient(), 100);
            timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t);
            long numFound = cluster.getSolrClient().query(backupCollectionName,
                    new SolrQuery("*:*")).getResults().getNumFound();
            log.info("Created backup with {} docs, took {}ms", numFound, timeTaken);

            t = System.nanoTime();
            randomlyPrecreateRestoreCollection(restoreCollectionName, "conf1", NUM_SHARDS, 1);
            CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
                    .setBackupId(0)
                    .setLocation(backupLocation)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .processAndWait(solrClient, 500);
            timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t);
            log.info("Restored from backup, took {}ms", timeTaken);
            t = System.nanoTime();
            AbstractDistribZkTestBase.waitForRecoveriesToFinish(
                restoreCollectionName, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), false, 3);
            timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t);
            log.info("Restored collection healthy, took {}ms", timeTaken);
            numFound = cluster.getSolrClient().query(restoreCollectionName,
                    new SolrQuery("*:*")).getResults().getNumFound();
            assertEquals(expectedDocsForFirstBackup, numFound);
        }
    }

    @Test
    public void testRestoreToOriginalCollection() throws Exception {
        setTestSuffix("testbackuprestoretooriginal");
        final String backupCollectionName = getCollectionName();
        final String backupName = BACKUPNAME_PREFIX + testSuffix;

        // Bootstrap the backup collection with seed docs
        CollectionAdminRequest
                .createCollection(backupCollectionName, "conf1", NUM_SHARDS, REPL_FACTOR)
                .setMaxShardsPerNode(-1)
                .process(cluster.getSolrClient());
        final int firstBatchNumDocs = indexDocs(backupCollectionName, true);

        // Backup and immediately add more docs to the collection
        try (BackupRepository repository = cluster.getJettySolrRunner(0).getCoreContainer()
                .newBackupRepository(Optional.of(BACKUP_REPO_NAME))) {
            final String backupLocation = repository.getBackupLocation(getBackupLocation());
            final RequestStatusState result = CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
                    .setLocation(backupLocation)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .processAndWait(cluster.getSolrClient(), 10 * 1000);
            assertEquals(RequestStatusState.COMPLETED, result);
        }
        final int secondBatchNumDocs = indexDocs(backupCollectionName, true);
        final int maxDocs = secondBatchNumDocs + firstBatchNumDocs;
        assertEquals(maxDocs, getNumDocsInCollection(backupCollectionName));

        // Restore original docs and validate that doc count is correct
        try (BackupRepository repository = cluster.getJettySolrRunner(0).getCoreContainer()
                .newBackupRepository(Optional.of(BACKUP_REPO_NAME))) {
            final String backupLocation = repository.getBackupLocation(getBackupLocation());
            final RequestStatusState result = CollectionAdminRequest.restoreCollection(backupCollectionName, backupName)
                    .setLocation(backupLocation)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .processAndWait(cluster.getSolrClient(), 20 * 1000);
            assertEquals(RequestStatusState.COMPLETED, result);
        }
        assertEquals(firstBatchNumDocs, getNumDocsInCollection(backupCollectionName));

    }

    @Test
    @Slow
    @SuppressWarnings("rawtypes")
    public void testBackupIncremental() throws Exception {
        setTestSuffix("testbackupinc");

        CloudSolrClient solrClient = cluster.getSolrClient();

        CollectionAdminRequest
            .createCollection(getCollectionName(), "conf1", NUM_SHARDS, REPL_FACTOR)
            .setMaxShardsPerNode(-1)
            .process(solrClient);

        indexDocs(getCollectionName(), false);

        String backupName = BACKUPNAME_PREFIX + testSuffix;
        try (BackupRepository repository = cluster.getJettySolrRunner(0).getCoreContainer()
                .newBackupRepository(Optional.of(BACKUP_REPO_NAME))) {
            String backupLocation = repository.getBackupLocation(getBackupLocation());
            URI fullBackupLocationURI = repository.resolveDirectory(repository.createDirectoryURI(backupLocation), backupName, getCollectionName());
            BackupFilePaths backupPaths = new BackupFilePaths(repository, fullBackupLocationURI);
            IncrementalBackupVerifier verifier = new IncrementalBackupVerifier(repository, backupLocation, backupName, getCollectionName(), 3);

            backupRestoreThenCheck(solrClient, verifier);
            indexDocs(getCollectionName(), false);
            backupRestoreThenCheck(solrClient, verifier);

            // adding more commits to trigger merging segments
            for (int i = 0; i < 15; i++) {
                indexDocs(getCollectionName(), 5,false);
            }

            backupRestoreThenCheck(solrClient, verifier);
            indexDocs(getCollectionName(), false);
            backupRestoreThenCheck(solrClient, verifier);

            // test list backups
            CollectionAdminResponse resp =
                    CollectionAdminRequest.listBackup(backupName)
                            .setBackupLocation(backupLocation)
                            .setBackupRepository(BACKUP_REPO_NAME)
                            .process(cluster.getSolrClient());
            ArrayList backups = (ArrayList) resp.getResponse().get("backups");
            assertEquals(3, backups.size());

            // test delete backups
            resp = CollectionAdminRequest.deleteBackupByRecency(backupName, 4)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .setLocation(backupLocation)
                    .process(cluster.getSolrClient());
            assertEquals(null, resp.getResponse().get("deleted"));

            resp =  CollectionAdminRequest.deleteBackupByRecency(backupName, 3)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .setLocation(backupLocation)
                    .process(cluster.getSolrClient());
            assertEquals(null, resp.getResponse().get("deleted"));

            resp = CollectionAdminRequest.deleteBackupByRecency(backupName, 2)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .setLocation(backupLocation)
                    .process(cluster.getSolrClient());
            assertEquals(1, resp.getResponse()._get("deleted[0]/backupId", null));

            resp = CollectionAdminRequest.deleteBackupById(backupName, 3)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .setLocation(backupLocation)
                    .process(cluster.getSolrClient());
            assertEquals(3, resp.getResponse()._get("deleted[0]/backupId", null));


            simpleRestoreAndCheckDocCount(solrClient, backupLocation, backupName);

            // test purge backups
            // purging first since there may corrupted files were uploaded
            resp = CollectionAdminRequest.deleteBackupPurgeUnusedFiles(backupName)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .setLocation(backupLocation)
                    .process(cluster.getSolrClient());

            addDummyFileToIndex(repository, backupPaths.getIndexDir(), "dummy-files-1");
            addDummyFileToIndex(repository, backupPaths.getIndexDir(), "dummy-files-2");
            resp = CollectionAdminRequest.deleteBackupPurgeUnusedFiles(backupName)
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .setLocation(backupLocation)
                    .process(cluster.getSolrClient());
            assertEquals(2, ((NamedList)resp.getResponse().get("deleted")).get("numIndexFiles"));

            new UpdateRequest()
                    .deleteByQuery("*:*")
                    .commit(cluster.getSolrClient(), getCollectionName());
            indexDocs(getCollectionName(), false);
            // corrupt index files
            corruptIndexFiles();
            try {
                log.info("Create backup after corrupt index files");
                CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(getCollectionName(), backupName)
                        .setLocation(backupLocation)
                        .setIncremental(true)
                        .setMaxNumberBackupPoints(3)
                        .setRepositoryName(BACKUP_REPO_NAME);
                if (random().nextBoolean()) {
                    RequestStatusState state = backup.processAndWait(cluster.getSolrClient(), 1000);
                    if (state != RequestStatusState.FAILED) {
                        fail("This backup should be failed");
                    }
                } else {
                    CollectionAdminResponse rsp = backup.process(cluster.getSolrClient());
                    fail("This backup should be failed");
                }
            } catch (Exception e) {
                // expected
                e.printStackTrace();
            }
        }
    }

    protected void corruptIndexFiles() throws IOException {
        Collection<Slice> slices = getCollectionState(getCollectionName()).getSlices();
        Slice slice = slices.iterator().next();
        JettySolrRunner leaderNode = cluster.getReplicaJetty(slice.getLeader());

        SolrCore solrCore = leaderNode.getCoreContainer().getCore(slice.getLeader().getCoreName());
        Set<String> fileNames = new HashSet<>(solrCore.getDeletionPolicy().getLatestCommit().getFileNames());
        File indexFolder = new File(solrCore.getIndexDir());
        File fileGetCorrupted = Stream.of(Objects.requireNonNull(indexFolder.listFiles()))
                .filter(x -> fileNames.contains(x.getName()))
                .findAny().get();
        try (FileInputStream fis = new FileInputStream(fileGetCorrupted)){
            byte[] contents = IOUtils.readFully(fis, (int) fileGetCorrupted.length());
            contents[contents.length - CodecUtil.footerLength() - 1] += 1;
            contents[contents.length - CodecUtil.footerLength() - 2] += 1;
            contents[contents.length - CodecUtil.footerLength() - 3] += 1;
            contents[contents.length - CodecUtil.footerLength() - 4] += 1;
            try (FileOutputStream fos = new FileOutputStream(fileGetCorrupted)) {
                IOUtils.write(contents, fos);
            }
        } finally {
            solrCore.close();
        }
    }

    private void addDummyFileToIndex(BackupRepository repository, URI indexDir, String fileName) throws IOException {
        try (OutputStream os = repository.createOutput(repository.resolve(indexDir, fileName))){
            os.write(100);
            os.write(101);
            os.write(102);
        }
    }

    private void backupRestoreThenCheck(CloudSolrClient solrClient,
                                        IncrementalBackupVerifier verifier) throws Exception {
        verifier.incrementalBackupThenVerify();

        if( random().nextBoolean() )
            simpleRestoreAndCheckDocCount(solrClient, verifier.backupLocation, verifier.backupName);
    }

    private void simpleRestoreAndCheckDocCount(CloudSolrClient solrClient, String backupLocation, String backupName) throws Exception{
        Map<String, Integer> origShardToDocCount = AbstractCloudBackupRestoreTestCase.getShardToDocCountMap(solrClient, getCollectionState(getCollectionName()));

        String restoreCollectionName = getCollectionName() + "_restored";

        randomlyPrecreateRestoreCollection(restoreCollectionName, "conf1", NUM_SHARDS, REPL_FACTOR);
        CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
                .setLocation(backupLocation)
                .setRepositoryName(BACKUP_REPO_NAME)
                .process(solrClient);

        AbstractDistribZkTestBase.waitForRecoveriesToFinish(
                restoreCollectionName, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), true, 30);

        // check num docs are the same
        assertEquals(origShardToDocCount, AbstractCloudBackupRestoreTestCase.getShardToDocCountMap(solrClient, getCollectionState(restoreCollectionName)));

        // this methods may get invoked multiple times, collection must be cleanup
        CollectionAdminRequest.deleteCollection(restoreCollectionName).process(solrClient);
    }


    private void indexDocs(String collectionName, int numDocs, boolean useUUID) throws Exception {
        Random random = new Random(docsSeed);

        List<SolrInputDocument> docs = new ArrayList<>(numDocs);
        for (int i=0; i<numDocs; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", (useUUID ? java.util.UUID.randomUUID().toString() : i));
            doc.addField("shard_s", "shard" + (1 + random.nextInt(NUM_SHARDS))); // for implicit router
            docs.add(doc);
        }

        CloudSolrClient client = cluster.getSolrClient();
        client.add(collectionName, docs); //batch
        client.commit(collectionName);

        log.info("Indexed {} docs to collection: {}", numDocs, collectionName);
    }

    private int indexDocs(String collectionName, boolean useUUID) throws Exception {
        Random random = new Random(docsSeed);// use a constant seed for the whole test run so that we can easily re-index.
        int numDocs = random.nextInt(100) + 5;
        indexDocs(collectionName, numDocs, useUUID);
        return numDocs;
    }

    private void randomlyPrecreateRestoreCollection(String restoreCollectionName, String configName, int numShards, int numReplicas) throws Exception {
        if (random().nextBoolean()) {
            CollectionAdminRequest.createCollection(restoreCollectionName, configName, numShards, numReplicas)
                    .setMaxShardsPerNode(-1)
                    .process(cluster.getSolrClient());
            cluster.waitForActiveCollection(restoreCollectionName, numShards, numShards*numReplicas);
        }
    }

    private long getNumDocsInCollection(String collectionName) throws Exception {
        return new QueryRequest(new SolrQuery("*:*")).process(cluster.getSolrClient(), collectionName).getResults().getNumFound();
    }

    private class IncrementalBackupVerifier {
        private BackupRepository repository;
        private URI backupURI;
        private String backupLocation;
        private String backupName;
        private BackupFilePaths incBackupFiles;

        private Map<String, Collection<String>> lastShardCommitToBackupFiles = new HashMap<>();
        // the first generation after calling backup is zero
        private int numBackup = -1;
        private int maxNumberOfBackupToKeep = 4;

        IncrementalBackupVerifier(BackupRepository repository, String backupLocation,
                                  String backupName, String collection, int maxNumberOfBackupToKeep) {
            this.repository = repository;
            this.backupLocation = backupLocation;
            this.backupURI = repository.resolveDirectory(repository.createURI(backupLocation), backupName, collection);
            this.incBackupFiles = new BackupFilePaths(repository, this.backupURI);
            this.backupName = backupName;
            this.maxNumberOfBackupToKeep = maxNumberOfBackupToKeep;
        }

        @SuppressWarnings("rawtypes")
        private void backupThenWait() throws SolrServerException, IOException {
            CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(getCollectionName(), backupName)
                    .setLocation(backupLocation)
                    .setIncremental(true)
                    .setMaxNumberBackupPoints(maxNumberOfBackupToKeep)
                    .setRepositoryName(BACKUP_REPO_NAME);
            if (random().nextBoolean()) {
                try {
                    RequestStatusState state = backup.processAndWait(cluster.getSolrClient(), 1000);
                    assertEquals(RequestStatusState.COMPLETED, state);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                numBackup++;
            } else {
                CollectionAdminResponse rsp = backup.process(cluster.getSolrClient());
                assertEquals(0, rsp.getStatus());
                NamedList resp = (NamedList) rsp.getResponse().get("response");
                numBackup++;
                assertEquals(numBackup, resp.get("backupId"));;
            }
        }

        void incrementalBackupThenVerify() throws IOException, SolrServerException {
            int numCopiedFiles = copiedFiles().size();
            backupThenWait();
            List<URI> newFilesCopiedOver = copiedFiles().subList(numCopiedFiles, copiedFiles().size());
            verify(newFilesCopiedOver);
        }

        ShardBackupMetadata getLastShardBackupId(String shardName) throws IOException {
            ShardBackupId shardBackupId = BackupProperties
                    .readFromLatest(repository, backupURI)
                    .flatMap(bp -> bp.getShardBackupIdFor(shardName))
                    .get();
            return ShardBackupMetadata.from(repository, new BackupFilePaths(repository, backupURI).getShardBackupMetadataDir(), shardBackupId);
        }

        private void assertIndexInputEquals(IndexInput in1, IndexInput in2) throws IOException {
            assertEquals(in1.length(), in2.length());
            for (int i = 0; i < in1.length(); i++) {
                assertEquals(in1.readByte(), in2.readByte());
            }
        }

        private void assertFolderAreSame(URI uri1, URI uri2) throws IOException {
            String[] files1 = repository.listAll(uri1);
            String[] files2 = repository.listAll(uri2);
            Arrays.sort(files1);
            Arrays.sort(files2);

            try {
                assertArrayEquals(files1, files2);
            } catch (AssertionError e) {
                e.printStackTrace();
            }

            for (int i = 0; i < files1.length; i++) {
                URI file1Uri = repository.resolve(uri1, files1[i]);
                URI file2Uri = repository.resolve(uri2, files2[i]);
                assertEquals(repository.getPathType(file1Uri), repository.getPathType(file2Uri));
                if (repository.getPathType(file1Uri) == BackupRepository.PathType.DIRECTORY) {
                    assertFolderAreSame(file1Uri, file2Uri);
                } else {
                    try (IndexInput in1 = repository.openInput(uri1, files1[i], IOContext.READONCE);
                         IndexInput in2 = repository.openInput(uri1, files1[i], IOContext.READONCE)) {
                        assertIndexInputEquals(in1, in2);
                    }
                }
            }
        }

        public void verify(List<URI> newFilesCopiedOver) throws IOException {
            //Verify zk files are reuploaded to a appropriate each time a backup is called
            //TODO make a little change to zk files and make sure that backed up files match with zk data
            BackupId prevBackupId = new BackupId(Math.max(0, numBackup - 1));

            URI backupPropertiesFile = repository.resolve(backupURI, "backup_"+numBackup+".properties");
            URI zkBackupFolder = repository.resolve(backupURI, "zk_backup_"+numBackup);
            assertTrue(repository.exists(backupPropertiesFile));
            assertTrue(repository.exists(zkBackupFolder));
            assertFolderAreSame(repository.resolveDirectory(backupURI, BackupFilePaths.getZkStateDir(prevBackupId)), zkBackupFolder);

            // verify indexes file
            for(Slice slice : getCollectionState(getCollectionName()).getSlices()) {
                Replica leader = slice.getLeader();
                final ShardBackupMetadata shardBackupMetadata = getLastShardBackupId(slice.getName());
                assertNotNull(shardBackupMetadata);

                try (SolrCore solrCore = cluster.getReplicaJetty(leader).getCoreContainer().getCore(leader.getCoreName())) {
                    Directory dir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
                    try {
                        URI indexDir = incBackupFiles.getIndexDir();
                        IndexCommit lastCommit = solrCore.getDeletionPolicy().getLatestCommit();

                        Collection<String> newBackupFiles = newIndexFilesComparedToLastBackup(slice.getName(), lastCommit).stream()
                                .map(indexFile -> {
                                    Optional<ShardBackupMetadata.BackedFile> backedFile = shardBackupMetadata.getFile(indexFile);
                                    assertTrue(backedFile.isPresent());
                                    return backedFile.get().uniqueFileName;
                                })
                                .collect(Collectors.toList());

                        lastCommit.getFileNames().forEach(
                                f -> {
                                    Optional<ShardBackupMetadata.BackedFile> backedFile = shardBackupMetadata.getFile(f);
                                    assertTrue(backedFile.isPresent());
                                    String uniqueFileName = backedFile.get().uniqueFileName;

                                    if (newBackupFiles.contains(uniqueFileName)) {
                                        assertTrue(newFilesCopiedOver.contains(repository.resolve(indexDir, uniqueFileName)));
                                    }

                                    try {
                                        Checksum localChecksum = repository.checksum(dir, f);
                                        Checksum remoteChecksum = backedFile.get().fileChecksum;
                                        assertEquals(localChecksum.checksum, remoteChecksum.checksum);
                                        assertEquals(localChecksum.size, remoteChecksum.size);
                                    } catch (IOException e) {
                                        throw new AssertionError(e);
                                    }
                                }
                        );

                        assertEquals("Incremental backup stored more files than needed", lastCommit.getFileNames().size(), shardBackupMetadata.listOriginalFileNames().size());
                    } finally {
                        solrCore.getDirectoryFactory().release(dir);
                    }
                }
            }
        }

        private Collection<String> newIndexFilesComparedToLastBackup(String shardName, IndexCommit currentCommit) throws IOException {
            Collection<String> oldFiles = lastShardCommitToBackupFiles.put(shardName, currentCommit.getFileNames());
            if (oldFiles == null)
                oldFiles = new ArrayList<>();

            List<String> newFiles = new ArrayList<>(currentCommit.getFileNames());
            newFiles.removeAll(oldFiles);
            return newFiles;
        }
    }
}
