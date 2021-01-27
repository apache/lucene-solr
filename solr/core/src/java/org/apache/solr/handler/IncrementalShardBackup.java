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

import org.apache.commons.math3.util.Precision;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

/**
 * Responsible for orchestrating the actual incremental backup process.
 *
 * If this is the first backup for a collection, all files are uploaded.  But if previous backups exist, uses the most recent
 * {@link ShardBackupMetadata} file to determine which files already exist in the repository and can be skipped.
 */
public class IncrementalShardBackup {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private SolrCore solrCore;

    private BackupFilePaths incBackupFiles;
    private BackupRepository backupRepo;

    private ShardBackupId prevShardBackupId;
    private ShardBackupId shardBackupId;
    private Optional<String> commitNameOption;

    /**
     *
     * @param prevShardBackupId previous ShardBackupMetadata file which will be used for skipping
     *                             uploading index files already present in this file.
     * @param shardBackupId file where all meta data of this backup will be stored to.
     */
    public IncrementalShardBackup(BackupRepository backupRepo, SolrCore solrCore, BackupFilePaths incBackupFiles,
                                  ShardBackupId prevShardBackupId, ShardBackupId shardBackupId,
                                  Optional<String> commitNameOption) {
        this.backupRepo = backupRepo;
        this.solrCore = solrCore;
        this.incBackupFiles = incBackupFiles;
        this.prevShardBackupId = prevShardBackupId;
        this.shardBackupId = shardBackupId;
        this.commitNameOption = commitNameOption;
    }

    @SuppressWarnings({"rawtypes"})
    public NamedList backup() throws Exception {
        final IndexCommit indexCommit = getAndSaveIndexCommit();
        try {
            return backup(indexCommit);
        } finally {
            solrCore.getDeletionPolicy().releaseCommitPoint(indexCommit.getGeneration());
        }
    }

    /**
     * Returns {@link IndexDeletionPolicyWrapper#getAndSaveLatestCommit} unless a particular commitName was requested.
     * <p>
     * Note:
     * <ul>
     *  <li>This method does error handling when the commit can't be found and wraps them in {@link SolrException}
     *  </li>
     *  <li>If this method returns, the result will be non null, and the caller <em>MUST</em>
     *      call {@link IndexDeletionPolicyWrapper#releaseCommitPoint} when finished
     *  </li>
     * </ul>
     */
    private IndexCommit getAndSaveIndexCommit() throws IOException {
        if (commitNameOption.isPresent()) {
            return SnapShooter.getAndSaveNamedIndexCommit(solrCore, commitNameOption.get());
        }

        final IndexDeletionPolicyWrapper delPolicy = solrCore.getDeletionPolicy();
        final IndexCommit commit = delPolicy.getAndSaveLatestCommit();
        if (null == commit) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Index does not yet have any commits for core " +
                    solrCore.getName());
        }
        if (log.isDebugEnabled())   {
            log.debug("Using latest commit: generation={}", commit.getGeneration());
        }
        return commit;
    }

    private IndexCommit getAndSaveLatestIndexCommit(IndexDeletionPolicyWrapper delPolicy) {
        final IndexCommit commit = delPolicy.getAndSaveLatestCommit();
        if (null == commit) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Index does not yet have any commits for core " +
                    solrCore.getName());
        }
        if (log.isDebugEnabled())   {
            log.debug("Using latest commit: generation={}", commit.getGeneration());
        }
        return commit;
    }

    // note: remember to reserve the indexCommit first so it won't get deleted concurrently
    @SuppressWarnings({"rawtypes"})
    protected NamedList backup(final IndexCommit indexCommit) throws Exception {
        assert indexCommit != null;
        URI backupLocation = incBackupFiles.getBackupLocation();
        log.info("Creating backup snapshot at {} shardBackupMetadataFile:{}", backupLocation, shardBackupId);
        NamedList<Object> details = new NamedList<>();
        details.add("startTime", Instant.now().toString());

        Collection<String> files = indexCommit.getFileNames();
        Directory dir = solrCore.getDirectoryFactory().get(solrCore.getIndexDir(),
                DirectoryFactory.DirContext.DEFAULT, solrCore.getSolrConfig().indexConfig.lockType);
        try {
            BackupStats stats = incrementalCopy(files, dir);
            details.add("indexFileCount", stats.fileCount);
            details.add("uploadedIndexFileCount", stats.uploadedFileCount);
            details.add("indexSizeMB", stats.getIndexSizeMB());
            details.add("uploadedIndexFileMB", stats.getTotalUploadedMB());
        } finally {
            solrCore.getDirectoryFactory().release(dir);
        }

        CloudDescriptor cd = solrCore.getCoreDescriptor().getCloudDescriptor();
        if (cd != null) {
            details.add("shard", cd.getShardId());
        }

        details.add("endTime", Instant.now().toString());
        details.add("shardBackupId", shardBackupId.getIdAsString());
        log.info("Done creating backup snapshot at {} shardBackupMetadataFile:{}", backupLocation, shardBackupId);
        return details;
    }

    private ShardBackupMetadata getPrevBackupPoint() throws IOException {
        if (prevShardBackupId == null) {
            return ShardBackupMetadata.empty();
        }
        return ShardBackupMetadata.from(backupRepo, incBackupFiles.getShardBackupMetadataDir(), prevShardBackupId);
    }

    private BackupStats incrementalCopy(Collection<String> indexFiles, Directory dir) throws IOException {
        ShardBackupMetadata oldBackupPoint = getPrevBackupPoint();
        ShardBackupMetadata currentBackupPoint = ShardBackupMetadata.empty();
        URI indexDir = incBackupFiles.getIndexDir();
        BackupStats backupStats = new BackupStats();

        for(String fileName : indexFiles) {
            Optional<ShardBackupMetadata.BackedFile> opBackedFile = oldBackupPoint.getFile(fileName);
            Checksum originalFileCS = backupRepo.checksum(dir, fileName);

            if (opBackedFile.isPresent()) {
                ShardBackupMetadata.BackedFile backedFile = opBackedFile.get();
                Checksum existedFileCS = backedFile.fileChecksum;
                if (existedFileCS.equals(originalFileCS)) {
                    currentBackupPoint.addBackedFile(opBackedFile.get());
                    backupStats.skippedUploadingFile(existedFileCS);
                    continue;
                }
            }

            String backedFileName = UUID.randomUUID().toString();
            backupRepo.copyIndexFileFrom(dir, fileName, indexDir, backedFileName);

            currentBackupPoint.addBackedFile(backedFileName, fileName, originalFileCS);
            backupStats.uploadedFile(originalFileCS);
        }

        currentBackupPoint.store(backupRepo, incBackupFiles.getShardBackupMetadataDir(), shardBackupId);
        return backupStats;
    }

    private static class BackupStats {
        private int fileCount;
        private int uploadedFileCount;
        private long indexSize;
        private long totalUploadedBytes;

        public void uploadedFile(Checksum file) {
            fileCount++;
            uploadedFileCount++;
            indexSize += file.size;
            totalUploadedBytes += file.size;
        }

        public void skippedUploadingFile(Checksum existedFile) {
            fileCount++;
            indexSize += existedFile.size;
        }

        public double getIndexSizeMB() {
            return Precision.round(indexSize / (1024.0 * 1024), 3);
        }

        public double getTotalUploadedMB() {
            return Precision.round(totalUploadedBytes / (1024.0 * 1024), 3);
        }
    }
}
