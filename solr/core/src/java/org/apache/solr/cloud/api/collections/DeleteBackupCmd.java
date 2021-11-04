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
import org.apache.solr.core.backup.AggregateBackupStats;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.core.backup.BackupManager.COLLECTION_NAME_PROP;
import static org.apache.solr.core.backup.BackupManager.START_TIME_PROP;

/**
 * An overseer command used to delete files associated with incremental backups.
 *
 * This assumes use of the incremental backup format, and not the (now deprecated) traditional 'full-snapshot' format.
 * The deletion can either delete a specific {@link BackupId}, delete everything except the most recent N backup
 * points, or can be used to trigger a "garbage collection" of unused index files in the backup repository.
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
        int lastNumBackupPointsToKeep = message.getInt(CoreAdminParams.MAX_NUM_BACKUP_POINTS, -1);
        boolean purge = message.getBool(CoreAdminParams.BACKUP_PURGE_UNUSED, false);
        if (backupId == -1 && lastNumBackupPointsToKeep == -1 && !purge) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format(Locale.ROOT, "%s, %s or %s param must be provided", CoreAdminParams.BACKUP_ID, CoreAdminParams.MAX_NUM_BACKUP_POINTS,
                    CoreAdminParams.BACKUP_PURGE_UNUSED));
        }
        CoreContainer cc = ocmh.overseer.getCoreContainer();
        try (BackupRepository repository = cc.newBackupRepository(Optional.ofNullable(repo))) {
            URI location = repository.createDirectoryURI(backupLocation);
            final URI backupPath = BackupFilePaths.buildExistingBackupLocationURI(repository, location, backupName);
            if (repository.exists(repository.resolve(backupPath, BackupManager.TRADITIONAL_BACKUP_PROPS_FILE))) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The backup name [" + backupName + "] at " +
                        "location [" + location + "] holds a non-incremental (legacy) backup, but " +
                        "backup-deletion is only supported on incremental backups");
            }

            if (purge) {
                purge(repository, backupPath, results);
            } else if (backupId != -1){
                deleteBackupId(repository, backupPath, backupId, results);
            } else {
                keepNumberOfBackup(repository, backupPath, lastNumBackupPointsToKeep, results);
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    /**
     * Clean up {@code backupPath} by removing all index files, shard-metadata files, and backup property files that are
     * unreachable, uncompleted or corrupted.
     */
    void purge(BackupRepository repository, URI backupPath, @SuppressWarnings({"rawtypes"}) NamedList result) throws IOException {
        PurgeGraph purgeGraph = new PurgeGraph();
        purgeGraph.build(repository, backupPath);

        BackupFilePaths backupPaths = new BackupFilePaths(repository, backupPath);
        repository.delete(backupPaths.getIndexDir(), purgeGraph.indexFileDeletes, true);
        repository.delete(backupPaths.getShardBackupMetadataDir(), purgeGraph.shardBackupMetadataDeletes, true);
        repository.delete(backupPath, purgeGraph.backupIdDeletes, true);

        @SuppressWarnings({"rawtypes"})
        NamedList details = new NamedList();
        details.add("numBackupIds", purgeGraph.backupIdDeletes.size());
        details.add("numShardBackupIds", purgeGraph.shardBackupMetadataDeletes.size());
        details.add("numIndexFiles", purgeGraph.indexFileDeletes.size());
        result.add("deleted", details);
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

    void deleteBackupIds(URI backupUri, BackupRepository repository,
                         Set<BackupId> backupIdsDeletes,
                         @SuppressWarnings({"rawtypes"}) NamedList results) throws IOException {
        BackupFilePaths incBackupFiles = new BackupFilePaths(repository, backupUri);
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
                shardBackupIdFileDeletes.stream().map(ShardBackupId::getBackupMetadataFilename).collect(Collectors.toList()), true);
        repository.delete(incBackupFiles.getIndexDir(), unusedFiles, true);
        try {
            for (BackupId backupId : backupIdsDeletes) {
                repository.deleteDirectory(repository.resolveDirectory(backupUri, BackupFilePaths.getZkStateDir(backupId)));
            }
        } catch (FileNotFoundException e) {
            //ignore this
        }

        //add details to result before deleting backupPropFiles
        addResult(backupUri, repository, backupIdsDeletes, backupIdToCollectionBackupPoint, results);
        repository.delete(backupUri, backupIdsDeletes.stream().map(id -> BackupFilePaths.getBackupPropsName(id)).collect(Collectors.toList()), true);
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
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Backup ID [" + bid + "] not found; cannot be deleted");
        }

        deleteBackupIds(backupPath, repository, Collections.singleton(backupId), results);
    }

    final static class PurgeGraph {
        // graph
        Map<String, Node> backupIdNodeMap = new HashMap<>();
        Map<String, Node> shardBackupMetadataNodeMap = new HashMap<>();
        Map<String, Node> indexFileNodeMap = new HashMap<>();

        // delete queues
        List<String> backupIdDeletes = new ArrayList<>();
        List<String> shardBackupMetadataDeletes = new ArrayList<>();
        List<String> indexFileDeletes = new ArrayList<>();

        public void build(BackupRepository repository, URI backupPath) throws IOException {
            BackupFilePaths backupPaths = new BackupFilePaths(repository, backupPath);
            buildLogicalGraph(repository, backupPaths);

            findDeletableNodes(repository, backupPaths);
        }

        public void findDeletableNodes(BackupRepository repository, BackupFilePaths backupPaths) {
            // mark nodes as existing
            visitExistingNodes(repository.listAllOrEmpty(backupPaths.getShardBackupMetadataDir()),
                    shardBackupMetadataNodeMap, shardBackupMetadataDeletes);
            // this may be a long running commands
            visitExistingNodes(repository.listAllOrEmpty(backupPaths.getIndexDir()),
                    indexFileNodeMap, indexFileDeletes);

            // TODO Is this propagation logic really necessary?
            // The intention seems to be that if some index files are only referenced by a shard-metadata file that
            // is itself orphaned, then propagating the "orphaned" status down to each of these index files will allow
            // them to be deleted.
            //
            // But that doesn't seem to hold up under closer inspection.
            //
            // The graph is populated by following links out from the set of valid backup-id's.  All files (shard-
            // metadata, or index) that trace back to a valid backup-ID will appear in the graph.  If a shard-metadata
            // file becomes orphaned, it will be ignored during graph construction and any index files that it alone
            // references will not appear in the graph.  Since those index files will be unrepresented in the graph, the
            // 'visitExistingNodes' calls above should be sufficient to detect them as orphaned.
            //
            // This all assumes though that propagation is intended to solve the scenario I think it does.  If that
            // assumption is wrong, then this whole comment is wrong.

            // for nodes which are not existing, propagate that information to other nodes
            shardBackupMetadataNodeMap.values().forEach(Node::propagateNotExisting);
            indexFileNodeMap.values().forEach(Node::propagateNotExisting);

            addDeleteNodesToQueue(backupIdNodeMap, backupIdDeletes);
            addDeleteNodesToQueue(shardBackupMetadataNodeMap, shardBackupMetadataDeletes);
            addDeleteNodesToQueue(indexFileNodeMap, indexFileDeletes);
        }

        /**
         * Visiting files (nodes) actually present in physical layer,
         * if it does not present in the {@code nodeMap}, it should be deleted by putting into the {@code deleteQueue}
         */
        private void visitExistingNodes(String[] existingNodeKeys, Map<String, Node> nodeMap, List<String> deleteQueue) {
            for (String nodeKey : existingNodeKeys) {
                Node node = nodeMap.get(nodeKey);

                if (node == null) {
                    deleteQueue.add(nodeKey);
                } else {
                    node.existing = true;
                }
            }
        }

        private <T> void addDeleteNodesToQueue(Map<T, Node> tNodeMap, List<T> deleteQueue) {
            tNodeMap.forEach((key, value) -> {
                if (value.delete) {
                    deleteQueue.add(key);
                }
            });
        }

        Node getBackupIdNode(String backupPropsName) {
            return backupIdNodeMap.computeIfAbsent(backupPropsName, bid -> {
                Node node = new Node();
                node.existing = true;
                return node;
            });
        }

        Node getShardBackupIdNode(String shardBackupId) {
            return shardBackupMetadataNodeMap.computeIfAbsent(shardBackupId, s -> new Node());
        }

        Node getIndexFileNode(String indexFile) {
            return indexFileNodeMap.computeIfAbsent(indexFile, s -> new IndexFileNode());
        }

        void addEdge(Node node1, Node node2) {
            node1.addNeighbor(node2);
            node2.addNeighbor(node1);
        }

        private void buildLogicalGraph(BackupRepository repository, BackupFilePaths backupPaths) throws IOException {
            final URI baseBackupPath = backupPaths.getBackupLocation();

            List<BackupId> backupIds = BackupFilePaths.findAllBackupIdsFromFileListing(repository.listAllOrEmpty(baseBackupPath));
            for (BackupId backupId : backupIds) {
                BackupProperties backupProps = BackupProperties.readFrom(repository, baseBackupPath,
                        BackupFilePaths.getBackupPropsName(backupId));

                Node backupIdNode = getBackupIdNode(BackupFilePaths.getBackupPropsName(backupId));
                for (String shardBackupMetadataFilename : backupProps.getAllShardBackupMetadataFiles()) {
                    Node shardBackupMetadataNode = getShardBackupIdNode(shardBackupMetadataFilename);
                    addEdge(backupIdNode, shardBackupMetadataNode);


                    ShardBackupMetadata shardBackupMetadata = ShardBackupMetadata.from(repository, backupPaths.getShardBackupMetadataDir(),
                            ShardBackupId.fromShardMetadataFilename(shardBackupMetadataFilename));
                    if (shardBackupMetadata == null)
                        continue;

                    for (String indexFile : shardBackupMetadata.listUniqueFileNames()) {
                        Node indexFileNode = getIndexFileNode(indexFile);
                        addEdge(indexFileNode, shardBackupMetadataNode);
                    }
                }
            }
        }
    }

    //ShardBackupMetadata, BackupId
    static class Node {
        List<Node> neighbors;
        boolean delete = false;
        boolean existing = false;

        void addNeighbor(Node node) {
            if (neighbors == null) {
                neighbors = new ArrayList<>();
            }
            neighbors.add(node);
        }

        void propagateNotExisting() {
            if (existing)
                return;

            if (neighbors != null)
                neighbors.forEach(Node::propagateDelete);
        }

        void propagateDelete() {
            if (delete || !existing)
                return;

            delete = true;
            if (neighbors != null) {
                neighbors.forEach(Node::propagateDelete);
            }
        }
    }

    //IndexFile
    final static class IndexFileNode extends Node {
        int refCount = 0;

        @Override
        void addNeighbor(Node node) {
            super.addNeighbor(node);
            refCount++;
        }

        @Override
        void propagateDelete() {
            if (delete || !existing)
                return;

            refCount--;
            if (refCount == 0) {
                delete = true;
            }
        }
    }
}
