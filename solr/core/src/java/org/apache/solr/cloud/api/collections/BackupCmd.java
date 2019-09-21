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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import org.apache.lucene.util.Version;
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
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.SnapshotStatus;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.component.ShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public BackupCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
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

    Instant startTime = Instant.now();

    CoreContainer cc = ocmh.overseer.getCoreContainer();
    BackupRepository repository = cc.newBackupRepository(Optional.ofNullable(repo));
    BackupManager backupMgr = new BackupManager(repository, ocmh.zkStateReader);

    // Backup location
    URI location = repository.createURI(message.getStr(CoreAdminParams.BACKUP_LOCATION));
    URI backupPath = repository.resolve(location, backupName);

    //Validating if the directory already exists.
    if (repository.exists(backupPath)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The backup directory already exists: " + backupPath);
    }

    // Create a directory to store backup details.
    repository.createDirectory(backupPath);

    String strategy = message.getStr(CollectionAdminParams.INDEX_BACKUP_STRATEGY, CollectionAdminParams.COPY_FILES_STRATEGY);
    switch (strategy) {
      case CollectionAdminParams.COPY_FILES_STRATEGY: {
        copyIndexFiles(backupPath, collectionName, message, results);
        break;
      }
      case CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY: {
        break;
      }
    }

    log.info("Starting to backup ZK data for backupName={}", backupName);

    //Download the configs
    String configName = ocmh.zkStateReader.readConfigName(collectionName);
    backupMgr.downloadConfigDir(location, backupName, configName);

    //Save the collection's state. Can be part of the monolithic clusterstate.json or a individual state.json
    //Since we don't want to distinguish we extract the state and back it up as a separate json
    DocCollection collectionState = ocmh.zkStateReader.getClusterState().getCollection(collectionName);
    backupMgr.writeCollectionState(location, backupName, collectionName, collectionState);

    Properties properties = new Properties();

    properties.put(BackupManager.BACKUP_NAME_PROP, backupName);
    properties.put(BackupManager.COLLECTION_NAME_PROP, collectionName);
    properties.put(BackupManager.COLLECTION_ALIAS_PROP, extCollectionName);
    properties.put(CollectionAdminParams.COLL_CONF, configName);
    properties.put(BackupManager.START_TIME_PROP, startTime.toString());
    properties.put(BackupManager.INDEX_VERSION_PROP, Version.LATEST.toString());
    //TODO: Add MD5 of the configset. If during restore the same name configset exists then we can compare checksums to see if they are the same.
    //if they are not the same then we can throw an error or have an 'overwriteConfig' flag
    //TODO save numDocs for the shardLeader. We can use it to sanity check the restore.

    backupMgr.writeBackupProperties(location, backupName, properties);

    backupMgr.downloadCollectionProperties(location, backupName, collectionName);

    log.info("Completed backing up ZK data for backupName={}", backupName);
  }

  private Replica selectReplicaWithSnapshot(CollectionSnapshotMetaData snapshotMeta, Slice slice) {
    // The goal here is to choose the snapshot of the replica which was the leader at the time snapshot was created.
    // If that is not possible, we choose any other replica for the given shard.
    Collection<CoreSnapshotMetaData> snapshots = snapshotMeta.getReplicaSnapshotsForShard(slice.getName());

    Optional<CoreSnapshotMetaData> leaderCore = snapshots.stream().filter(x -> x.isLeader()).findFirst();
    if (leaderCore.isPresent()) {
      log.info("Replica {} was the leader when snapshot {} was created.", leaderCore.get().getCoreName(), snapshotMeta.getName());
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

  private void copyIndexFiles(URI backupPath, String collectionName, ZkNodeProps request, NamedList results) throws Exception {
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

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.BACKUPCORE.toString());
      params.set(NAME, slice.getName());
      params.set(CoreAdminParams.BACKUP_REPOSITORY, repoName);
      params.set(CoreAdminParams.BACKUP_LOCATION, backupPath.toASCIIString()); // note: index dir will be here then the "snapshot." + slice name
      params.set(CORE_NAME_PROP, coreName);
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
