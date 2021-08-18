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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.SnapshotStatus;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.component.ShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

public class BackupCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public BackupCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {

    String extCollectionName = message.getStr(COLLECTION_PROP);
    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collectionName;
    if (followAliases) {
      collectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }
    String backupName = message.getStr(NAME);
    String repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);
    boolean incremental = message.getBool(CoreAdminParams.BACKUP_INCREMENTAL, true);
    String configName = ocmh.zkStateReader.readConfigName(collectionName);

    BackupProperties backupProperties = BackupProperties.create(backupName, collectionName,
            extCollectionName, configName);

    CoreContainer cc = ocmh.overseer.getCoreContainer();
    try (BackupRepository repository = cc.newBackupRepository(Optional.ofNullable(repo))) {

      // Backup location
      URI location = repository.createDirectoryURI(message.getStr(CoreAdminParams.BACKUP_LOCATION));
      final URI backupUri = createAndValidateBackupPath(repository, incremental, location, backupName, collectionName);

      BackupManager backupMgr = (incremental) ?
              BackupManager.forIncrementalBackup(repository, ocmh.zkStateReader, backupUri) :
              BackupManager.forBackup(repository, ocmh.zkStateReader, backupUri);

      String strategy = message.getStr(CollectionAdminParams.INDEX_BACKUP_STRATEGY, CollectionAdminParams.COPY_FILES_STRATEGY);
      switch (strategy) {
        case CollectionAdminParams.COPY_FILES_STRATEGY: {
          if (incremental) {
            try {
              incrementalCopyIndexFiles(backupUri, collectionName, message, results, backupProperties, backupMgr);
            } catch (SolrException e) {
              log.error("Error happened during incremental backup for collection: {}", collectionName, e);
              ocmh.cleanBackup(repository, backupUri, backupMgr.getBackupId());
              throw e;
            }
          } else {
            copyIndexFiles(backupUri, collectionName, message, results);
          }
          break;
        }
        case CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY: {
          break;
        }
      }

      log.info("Starting to backup ZK data for backupName={}", backupName);

      //Download the configs
      backupMgr.downloadConfigDir(configName);

      //Save the collection's state. Can be part of the monolithic clusterstate.json or a individual state.json
      //Since we don't want to distinguish we extract the state and back it up as a separate json
      DocCollection collectionState = ocmh.zkStateReader.getClusterState().getCollection(collectionName);
      backupMgr.writeCollectionState(collectionName, collectionState);
      backupMgr.downloadCollectionProperties(collectionName);

      //TODO: Add MD5 of the configset. If during restore the same name configset exists then we can compare checksums to see if they are the same.
      //if they are not the same then we can throw an error or have an 'overwriteConfig' flag
      //TODO save numDocs for the shardLeader. We can use it to sanity check the restore.

      backupMgr.writeBackupProperties(backupProperties);

      log.info("Completed backing up ZK data for backupName={}", backupName);

      int maxNumBackup = message.getInt(CoreAdminParams.MAX_NUM_BACKUP_POINTS, -1);
      if (incremental && maxNumBackup != -1) {
        ocmh.deleteBackup(repository, backupUri, maxNumBackup, results);
      }
    }
  }

  private URI createAndValidateBackupPath(BackupRepository repository, boolean incremental, URI location, String backupName, String collection) throws IOException{
    final URI backupNamePath = repository.resolveDirectory(location, backupName);

    if ( (!incremental) && repository.exists(backupNamePath)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The backup directory already exists: " + backupNamePath);
    }

    if (! repository.exists(backupNamePath)) {
      repository.createDirectory(backupNamePath);
    } else if (incremental){
      final String[] directoryContents = repository.listAll(backupNamePath);
      if (directoryContents.length == 1) {
        String directoryContentsName = directoryContents[0];
        // Strip the trailing '/' if it exists
        if (directoryContentsName.endsWith("/")) {
          directoryContentsName = directoryContentsName.substring(0, directoryContentsName.length()-1);
        }
        if (!directoryContentsName.equals(collection)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "The backup [" + backupName + "] at location [" + location +
              "] cannot be used to back up [" + collection + "], as it already holds a different collection [" +
              directoryContents[0] + "]");
        }
      }
    }

    if (! incremental) {
      return backupNamePath;
    }

    // Incremental backups have an additional directory named after the collection that needs created
    final URI backupPathWithCollection = repository.resolveDirectory(backupNamePath, collection);
    if (! repository.exists(backupPathWithCollection)) {
      repository.createDirectory(backupPathWithCollection);
    }
    BackupFilePaths incBackupFiles = new BackupFilePaths(repository, backupPathWithCollection);
    incBackupFiles.createIncrementalBackupFolders();
    return backupPathWithCollection;
  }

  private Replica selectReplicaWithSnapshot(CollectionSnapshotMetaData snapshotMeta, Slice slice) {
    // The goal here is to choose the snapshot of the replica which was the leader at the time snapshot was created.
    // If that is not possible, we choose any other replica for the given shard.
    Collection<CoreSnapshotMetaData> snapshots = snapshotMeta.getReplicaSnapshotsForShard(slice.getName());

    Optional<CoreSnapshotMetaData> leaderCore = snapshots.stream().filter(CoreSnapshotMetaData::isLeader).findFirst();
    if (leaderCore.isPresent()) {
      if (log.isInfoEnabled())  {
        log.info("Replica {} was the leader when snapshot {} was created.", leaderCore.get().getCoreName(), snapshotMeta.getName());
      }
      Replica r = slice.getReplica(leaderCore.get().getCoreName());
      if ((r != null) && !r.getState().equals(State.DOWN)) {
        return r;
      }
    }

    Optional<Replica> r = slice.getReplicas().stream()
            .filter(x -> x.getState() != State.DOWN && snapshotMeta.isSnapshotExists(slice.getName(), x))
            .findFirst();

    if (!r.isPresent()) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
              "Unable to find any live replica with a snapshot named " + snapshotMeta.getName() + " for shard " + slice.getName());
    }

    return r.get();
  }

  private void incrementalCopyIndexFiles(URI backupUri, String collectionName, ZkNodeProps request,
                                         NamedList<Object> results, BackupProperties backupProperties,
                                         BackupManager backupManager) throws IOException {
    String backupName = request.getStr(NAME);
    String asyncId = request.getStr(ASYNC);
    String repoName = request.getStr(CoreAdminParams.BACKUP_REPOSITORY);
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();

    log.info("Starting backup of collection={} with backupName={} at location={}", collectionName, backupName,
            backupUri);

    Optional<BackupProperties> previousProps = backupManager.tryReadBackupProperties();
    final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);

    Collection<Slice> slices = ocmh.zkStateReader.getClusterState().getCollection(collectionName).getActiveSlices();
    for (Slice slice : slices) {
      // Note - Actually this can return a null value when there is no leader for this shard.
      Replica replica = slice.getLeader();
      if (replica == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "No 'leader' replica available for shard " + slice.getName() + " of collection " + collectionName);
      }
      String coreName = replica.getStr(CORE_NAME_PROP);

      ModifiableSolrParams params = coreBackupParams(backupUri, repoName, slice, coreName, true /* incremental backup */);
      params.set(CoreAdminParams.BACKUP_INCREMENTAL, true);
      previousProps.flatMap(bp -> bp.getShardBackupIdFor(slice.getName()))
              .ifPresent(prevBackupPoint -> params.set(CoreAdminParams.PREV_SHARD_BACKUP_ID, prevBackupPoint.getIdAsString()));

      ShardBackupId shardBackupId = backupProperties.putAndGetShardBackupIdFor(slice.getName(),
              backupManager.getBackupId().getId());
      params.set(CoreAdminParams.SHARD_BACKUP_ID, shardBackupId.getIdAsString());

      shardRequestTracker.sendShardRequest(replica.getNodeName(), params, shardHandler);
      log.debug("Sent backup request to core={} for backupName={}", coreName, backupName);
    }
    log.debug("Sent backup requests to all shard leaders for backupName={}", backupName);

    String msgOnError = "Could not backup all shards";
    shardRequestTracker.processResponses(results, shardHandler, true, msgOnError);
    if (results.get("failure") != null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, msgOnError);
    }

    //Aggregating result from different shards
    @SuppressWarnings({"rawtypes"})
    NamedList aggRsp = aggregateResults(results, collectionName, backupManager, backupProperties, slices);
    results.add("response", aggRsp);
  }

  @SuppressWarnings({"rawtypes"})
  private NamedList aggregateResults(NamedList results, String collectionName,
                                     BackupManager backupManager,
                                     BackupProperties backupProps,
                                     Collection<Slice> slices) {
    NamedList<Object> aggRsp = new NamedList<>();
    aggRsp.add("collection", collectionName);
    aggRsp.add("numShards", slices.size());
    aggRsp.add("backupId", backupManager.getBackupId().id);
    aggRsp.add("indexVersion", backupProps.getIndexVersion());
    aggRsp.add("startTime", backupProps.getStartTime());

    double indexSizeMB = 0;
    NamedList shards = (NamedList) results.get("success");
    for (int i = 0; i < shards.size(); i++) {
      NamedList shardResp = (NamedList)((NamedList)shards.getVal(i)).get("response");
      if (shardResp == null)
        continue;
      indexSizeMB += (double) shardResp.get("indexSizeMB");
    }
    aggRsp.add("indexSizeMB", indexSizeMB);
    return aggRsp;
  }

  private ModifiableSolrParams coreBackupParams(URI backupPath, String repoName, Slice slice, String coreName, boolean incremental) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString());
    params.set(NAME, slice.getName());
    params.set(CoreAdminParams.BACKUP_REPOSITORY, repoName);
    params.set(CoreAdminParams.BACKUP_LOCATION, backupPath.toASCIIString()); // note: index dir will be here then the "snapshot." + slice name
    params.set(CORE_NAME_PROP, coreName);
    params.set(CoreAdminParams.BACKUP_INCREMENTAL, incremental);
    return params;
  }

  @SuppressWarnings({"unchecked"})
  private void copyIndexFiles(URI backupPath, String collectionName, ZkNodeProps request, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
    String backupName = request.getStr(NAME);
    String asyncId = request.getStr(ASYNC);
    String repoName = request.getStr(CoreAdminParams.BACKUP_REPOSITORY);
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient());

    String commitName = request.getStr(CoreAdminParams.COMMIT_NAME);
    Optional<CollectionSnapshotMetaData> snapshotMeta = Optional.empty();
    if (commitName != null) {
      SolrZkClient zkClient = ocmh.zkStateReader.getZkClient();
      snapshotMeta = SolrSnapshotManager.getCollectionLevelSnapshot(zkClient, collectionName, commitName);
      if (!snapshotMeta.isPresent()) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Snapshot with name " + commitName
                + " does not exist for collection " + collectionName);
      }
      if (snapshotMeta.get().getStatus() != SnapshotStatus.Successful) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Snapshot with name " + commitName + " for collection " + collectionName
                + " has not completed successfully. The status is " + snapshotMeta.get().getStatus());
      }
    }

    log.info("Starting backup of collection={} with backupName={} at location={}", collectionName, backupName,
            backupPath);

    Collection<String> shardsToConsider = Collections.emptySet();
    if (snapshotMeta.isPresent()) {
      shardsToConsider = snapshotMeta.get().getShards();
    }

    final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
    for (Slice slice : ocmh.zkStateReader.getClusterState().getCollection(collectionName).getActiveSlices()) {
      Replica replica = null;

      if (snapshotMeta.isPresent()) {
        if (!shardsToConsider.contains(slice.getName())) {
          log.warn("Skipping the backup for shard {} since it wasn't part of the collection {} when snapshot {} was created.",
                  slice.getName(), collectionName, snapshotMeta.get().getName());
          continue;
        }
        replica = selectReplicaWithSnapshot(snapshotMeta.get(), slice);
      } else {
        // Note - Actually this can return a null value when there is no leader for this shard.
        replica = slice.getLeader();
        if (replica == null) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "No 'leader' replica available for shard " + slice.getName() + " of collection " + collectionName);
        }
      }

      String coreName = replica.getStr(CORE_NAME_PROP);

      ModifiableSolrParams params = coreBackupParams(backupPath, repoName, slice, coreName, false /*non-incremental backup */);
      if (snapshotMeta.isPresent()) {
        params.set(CoreAdminParams.COMMIT_NAME, snapshotMeta.get().getName());
      }

      shardRequestTracker.sendShardRequest(replica.getNodeName(), params, shardHandler);
      log.debug("Sent backup request to core={} for backupName={}", coreName, backupName);
    }
    log.debug("Sent backup requests to all shard leaders for backupName={}", backupName);

    shardRequestTracker.processResponses(results, shardHandler, true, "Could not backup all shards");
  }
}
