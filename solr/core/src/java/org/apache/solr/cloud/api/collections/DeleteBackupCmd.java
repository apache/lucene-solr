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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.AggregateBackupStats;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.BackupFilePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.core.backup.BackupManager.COLLECTION_NAME_PROP;
import static org.apache.solr.core.backup.BackupManager.START_TIME_PROP;

/**
 * An overseer command used to delete files associated with incremental backups.
 *
 * This assumes use of the incremental backup format, and not the (now deprecated) traditional 'full-snapshot' format.
 * The deletion can either delete a specific {@link BackupId}, or delete everything except the most recent N backup
 * points.
 *
 * While this functionality is structured as an overseer command, the message type isn't currently handled by the
 * overseer or recorded to the overseer queue by any APIs.  This integration will be added in the forthcoming SOLR-15101
 */
public class DeleteBackupCmd implements OverseerCollectionMessageHandler.Cmd {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final OverseerCollectionMessageHandler ocmh;

    DeleteBackupCmd(OverseerCollectionMessageHandler ocmh) {
        this.ocmh = ocmh;
    }

    @Override
    public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
        String backupLocation = message.getStr(CoreAdminParams.BACKUP_LOCATION);
        String backupName = message.getStr(NAME);
        String repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);
        int backupId = message.getInt(CoreAdminParams.BACKUP_ID, -1);
        int lastNumBackupPointsToKeep = message.getInt(CoreAdminParams.MAX_NUM_BACKUP, -1);
        if (backupId == -1 && lastNumBackupPointsToKeep == -1) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    String.format(Locale.ROOT, "%s or %s param must be provided", CoreAdminParams.BACKUP_ID, CoreAdminParams.MAX_NUM_BACKUP));
        }
        CoreContainer cc = ocmh.overseer.getCoreContainer();
        try (BackupRepository repository = cc.newBackupRepository(repo)) {
            URI location = repository.createURI(backupLocation);
            final URI backupPath = BackupFilePaths.buildExistingBackupLocationURI(repository, location, backupName);
            if (repository.exists(repository.resolve(backupPath, BackupManager.TRADITIONAL_BACKUP_PROPS_FILE))) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The backup name [" + backupName + "] at " +
                        "location [" + location + "] holds a non-incremental (legacy) backup, but " +
                        "backup-deletion is only supported on incremental backups");
            }

            if (backupId != -1){
                deleteBackupId(repository, backupPath, backupId, results);
            } else {
                keepNumberOfBackup(repository, backupPath, lastNumBackupPointsToKeep, results);
            }
        }
    }

    /**
     * Keep most recent {@code  maxNumBackup} and delete the rest.
     */
    void keepNumberOfBackup(BackupRepository repository, URI backupPath,
                            int maxNumBackup,
                            @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
        List<BackupId> backupIds = BackupFilePaths.findAllBackupIdsFromFileListing(repository.listAllOrEmpty(backupPath));
        if (backupIds.size() <= maxNumBackup) {
            return;
        }

        Collections.sort(backupIds);
        List<BackupId> backupIdDeletes = backupIds.subList(0, backupIds.size() - maxNumBackup);
        deleteBackupIds(backupPath, repository, new HashSet<>(backupIdDeletes), results);
    }

    void deleteBackupIds(URI backupPath, BackupRepository repository,
                         Set<BackupId> backupIdsDeletes,
                         @SuppressWarnings({"rawtypes"}) NamedList results) throws IOException {
        BackupFilePaths incBackupFiles = new BackupFilePaths(repository, backupPath);
        URI shardBackupMetadataDir = incBackupFiles.getShardBackupMetadataDir();

        Set<String> referencedIndexFiles = new HashSet<>();
        List<ShardBackupId> shardBackupIdFileDeletes = new ArrayList<>();


        List<ShardBackupId> shardBackupIds = Arrays.stream(repository.listAllOrEmpty(shardBackupMetadataDir))
                .map(sbi -> ShardBackupId.fromShardMetadataFilename(sbi))
                .collect(Collectors.toList());
        for (ShardBackupId shardBackupId : shardBackupIds) {
            final BackupId backupId = shardBackupId.getContainingBackupId();

            if (backupIdsDeletes.contains(backupId)) {
                shardBackupIdFileDeletes.add(shardBackupId);
            } else {
                ShardBackupMetadata shardBackupMetadata = ShardBackupMetadata.from(repository, shardBackupMetadataDir, shardBackupId);
                if (shardBackupMetadata != null)
                    referencedIndexFiles.addAll(shardBackupMetadata.listUniqueFileNames());
            }
        }


        Map<BackupId, AggregateBackupStats> backupIdToCollectionBackupPoint = new HashMap<>();
        List<String> unusedFiles = new ArrayList<>();
        for (ShardBackupId shardBackupIdToDelete : shardBackupIdFileDeletes) {
            BackupId backupId = shardBackupIdToDelete.getContainingBackupId();
            ShardBackupMetadata shardBackupMetadata = ShardBackupMetadata.from(repository, shardBackupMetadataDir, shardBackupIdToDelete);
            if (shardBackupMetadata == null)
                continue;

            backupIdToCollectionBackupPoint
                    .putIfAbsent(backupId, new AggregateBackupStats());
            backupIdToCollectionBackupPoint.get(backupId).add(shardBackupMetadata);

            for (String uniqueIndexFile : shardBackupMetadata.listUniqueFileNames()) {
                if (!referencedIndexFiles.contains(uniqueIndexFile)) {
                    unusedFiles.add(uniqueIndexFile);
                }
            }
        }

        repository.delete(incBackupFiles.getShardBackupMetadataDir(),
                shardBackupIdFileDeletes.stream().map(ShardBackupId::getIdAsString).collect(Collectors.toList()), true);
        repository.delete(incBackupFiles.getIndexDir(), unusedFiles, true);
        try {
            for (BackupId backupId : backupIdsDeletes) {
                repository.deleteDirectory(repository.resolve(backupPath, BackupFilePaths.getZkStateDir(backupId)));
            }
        } catch (FileNotFoundException e) {
            //ignore this
        }

        //add details to result before deleting backupPropFiles
        addResult(backupPath, repository, backupIdsDeletes, backupIdToCollectionBackupPoint, results);
        repository.delete(backupPath, backupIdsDeletes.stream().map(id -> BackupFilePaths.getBackupPropsName(id)).collect(Collectors.toList()), true);
    }

    @SuppressWarnings("unchecked")
    private void addResult(URI backupPath, BackupRepository repository,
                           Set<BackupId> backupIdDeletes,
                           Map<BackupId, AggregateBackupStats> backupIdToCollectionBackupPoint,
                           @SuppressWarnings({"rawtypes"}) NamedList results) {

        String collectionName = null;
        @SuppressWarnings({"rawtypes"})
        List<NamedList> shardBackupIdDetails = new ArrayList<>();
        results.add("deleted", shardBackupIdDetails);
        for (BackupId backupId : backupIdDeletes) {
            NamedList<Object> backupIdResult = new NamedList<>();

            try {
                BackupProperties props = BackupProperties.readFrom(repository, backupPath, BackupFilePaths.getBackupPropsName(backupId));
                backupIdResult.add(START_TIME_PROP, props.getStartTime());
                if (collectionName == null) {
                    collectionName = props.getCollection();
                    results.add(COLLECTION_NAME_PROP, collectionName);
                }
            } catch (IOException e) {
                //prop file not found
            }

            AggregateBackupStats cbp = backupIdToCollectionBackupPoint.getOrDefault(backupId, new AggregateBackupStats());
            backupIdResult.add("backupId", backupId.getId());
            backupIdResult.add("size", cbp.getTotalSize());
            backupIdResult.add("numFiles", cbp.getNumFiles());
            shardBackupIdDetails.add(backupIdResult);
        }
    }

    private void deleteBackupId(BackupRepository repository, URI backupPath,
                                int bid, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
        BackupId backupId = new BackupId(bid);
        if (!repository.exists(repository.resolve(backupPath, BackupFilePaths.getBackupPropsName(backupId)))) {
            return;
        }

        deleteBackupIds(backupPath, repository, Collections.singleton(backupId), results);
    }
}
