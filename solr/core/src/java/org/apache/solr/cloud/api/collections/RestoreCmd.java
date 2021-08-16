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


import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.DocCollection.STATE_FORMAT;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_TYPE;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

public class RestoreCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public RestoreCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    try (RestoreContext restoreContext = new RestoreContext(message, ocmh)) {
      if (state.hasCollection(restoreContext.restoreCollectionName)) {
        RestoreOnExistingCollection restoreOnExistingCollection = new RestoreOnExistingCollection(restoreContext);
        restoreOnExistingCollection.process(restoreContext, results);
      } else {
        RestoreOnANewCollection restoreOnANewCollection = new RestoreOnANewCollection(message, restoreContext.backupCollectionState);
        restoreOnANewCollection.validate(restoreContext.backupCollectionState, restoreContext.nodeList.size());
        restoreOnANewCollection.process(results, restoreContext);
      }
    }
  }

  private void requestReplicasToRestore(@SuppressWarnings({"rawtypes"}) NamedList results, DocCollection restoreCollection,
                                        ClusterState clusterState,
                                        BackupProperties backupProperties,
                                        URI backupPath,
                                        String repo,
                                        ShardHandler shardHandler,
                                        String asyncId) {
    ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
    // Copy data from backed up index to each replica
    for (Slice slice : restoreCollection.getSlices()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.RESTORECORE.toString());
      Optional<ShardBackupId> shardBackupId = backupProperties.getShardBackupIdFor(slice.getName());
      if (shardBackupId.isPresent()) {
        params.set(CoreAdminParams.SHARD_BACKUP_ID, shardBackupId.get().getIdAsString());
      } else {
        params.set(NAME, "snapshot." + slice.getName());
      }
      params.set(CoreAdminParams.BACKUP_LOCATION, backupPath.toASCIIString());
      params.set(CoreAdminParams.BACKUP_REPOSITORY, repo);
      shardRequestTracker.sliceCmd(clusterState, params, null, slice, shardHandler);
    }
    shardRequestTracker.processResponses(new NamedList<>(), shardHandler, true, "Could not restore core");
  }

  private int getInt(ZkNodeProps message, String propertyName, Integer count, int defaultValue) {
    Integer value = message.getInt(propertyName, count);
    return value!=null ? value:defaultValue;
  }

  /**
   * Encapsulates the parsing and access for common parameters restore parameters and values
   */
  private static class RestoreContext implements Closeable {

    final String restoreCollectionName;
    final String backupName;
    final String backupCollection;
    final String asyncId;
    final String repo;
    final String restoreConfigName;
    final int backupId;
    final URI location;
    final URI backupPath;
    final List<String> nodeList;

    final CoreContainer container;
    final BackupRepository repository;
    final ZkStateReader zkStateReader;
    final BackupManager backupManager;
    final BackupProperties backupProperties;
    final DocCollection backupCollectionState;
    final ShardHandler shardHandler;

    private RestoreContext(ZkNodeProps message, OverseerCollectionMessageHandler ocmh) throws IOException {
      this.restoreCollectionName = message.getStr(COLLECTION_PROP);
      this.backupName = message.getStr(NAME); // of backup
      this.asyncId = message.getStr(ASYNC);
      this.repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);
      this.backupId = message.getInt(CoreAdminParams.BACKUP_ID, -1);

      this.container = ocmh.overseer.getCoreContainer();
      this.repository = this.container.newBackupRepository(Optional.ofNullable(repo));

      this.location = repository.createDirectoryURI(message.getStr(CoreAdminParams.BACKUP_LOCATION));
      final URI backupNameUri = repository.resolveDirectory(location, backupName);
      final String[] entries = repository.listAll(backupNameUri);
      final boolean incremental = ! Arrays.stream(entries).anyMatch(entry -> entry.equals(BackupManager.TRADITIONAL_BACKUP_PROPS_FILE));
      this.backupPath = (incremental) ?
              repository.resolveDirectory(backupNameUri, entries[0]) : // incremental backups have an extra path component representing the backed up collection
              backupNameUri;
      this.zkStateReader = ocmh.zkStateReader;
      this.backupManager = backupId == -1 ?
              BackupManager.forRestore(repository, zkStateReader, backupPath) :
              BackupManager.forRestore(repository, zkStateReader, backupPath, backupId);

      this.backupProperties = this.backupManager.readBackupProperties();
      this.backupCollection = this.backupProperties.getCollection();
      this.restoreConfigName = message.getStr(CollectionAdminParams.COLL_CONF, this.backupProperties.getConfigName());
      this.backupCollectionState = this.backupManager.readCollectionState(this.backupCollection);

      this.shardHandler = ocmh.shardHandlerFactory.getShardHandler();
      this.nodeList = Assign.getLiveOrLiveAndCreateNodeSetList(
              zkStateReader.getClusterState().getLiveNodes(), message, OverseerCollectionMessageHandler.RANDOM);
    }

    @Override
    public void close() throws IOException {
      if (this.repository != null) {
        this.repository.close();
      }
    }
  }

  /**
   * Restoration 'strategy' that takes responsibility for creating the collection to restore to.
   *
   * @see RestoreOnExistingCollection
   */
  private class RestoreOnANewCollection {
    private int numNrtReplicas;
    private int numTlogReplicas;
    private int numPullReplicas;
    private int totalReplicasPerShard;
    private int maxShardsPerNode;
    private ZkNodeProps message;

    private RestoreOnANewCollection(ZkNodeProps message, DocCollection backupCollectionState) {
      this.message = message;

      if (message.get(REPLICATION_FACTOR) != null) {
        this.numNrtReplicas = message.getInt(REPLICATION_FACTOR, 0);
      } else if (message.get(NRT_REPLICAS) != null) {
        this.numNrtReplicas = message.getInt(NRT_REPLICAS, 0);
      } else {
        //replicationFactor and nrtReplicas is always in sync after SOLR-11676
        //pick from cluster state of the backed up collection
        this.numNrtReplicas = backupCollectionState.getReplicationFactor();
      }
      this.numTlogReplicas = getInt(message, TLOG_REPLICAS, backupCollectionState.getNumTlogReplicas(), 0);
      this.numPullReplicas = getInt(message, PULL_REPLICAS, backupCollectionState.getNumPullReplicas(), 0);
      this.totalReplicasPerShard = numNrtReplicas + numTlogReplicas + numPullReplicas;
      assert totalReplicasPerShard > 0;

      this.maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, backupCollectionState.getMaxShardsPerNode());
    }

    public void process(@SuppressWarnings("rawtypes") NamedList results, RestoreContext rc) throws Exception {
      // Avoiding passing RestoreContext around
      uploadConfig(rc.backupProperties.getConfigName(),
              rc.restoreConfigName,
              rc.zkStateReader,
              rc.backupManager);

      log.info("Starting restore into collection={} with backup_name={} at location={}", rc.restoreCollectionName, rc.backupName,
              rc.location);
      createCoreLessCollection(rc.restoreCollectionName,
              rc.restoreConfigName,
              rc.backupCollectionState,
              rc.zkStateReader.getClusterState(),
              rc.backupProperties);
      // note: when createCollection() returns, the collection exists (no race)

      // Restore collection properties
      rc.backupManager.uploadCollectionProperties(rc.restoreCollectionName);

      DocCollection restoreCollection = rc.zkStateReader.getClusterState().getCollection(rc.restoreCollectionName);
      markAllShardsAsConstruction(restoreCollection);
      // TODO how do we leverage the RULE / SNITCH logic in createCollection?
      ClusterState clusterState = rc.zkStateReader.getClusterState();

      List<String> sliceNames = new ArrayList<>();
      restoreCollection.getSlices().forEach(x -> sliceNames.add(x.getName()));

      List<ReplicaPosition> replicaPositions = getReplicaPositions(restoreCollection, rc.nodeList, clusterState, sliceNames);

      createSingleReplicaPerShard(results, restoreCollection, rc.asyncId, clusterState, replicaPositions);
      Object failures = results.get("failure");
      if (failures != null && ((SimpleOrderedMap) failures).size() > 0) {
        log.error("Restore failed to create initial replicas.");
        ocmh.cleanupCollection(rc.restoreCollectionName, new NamedList<>());
        return;
      }

      //refresh the location copy of collection state
      restoreCollection = rc.zkStateReader.getClusterState().getCollection(rc.restoreCollectionName);
      requestReplicasToRestore(results, restoreCollection, clusterState, rc.backupProperties, rc.backupPath, rc.repo, rc.shardHandler, rc.asyncId);
      requestReplicasToApplyBufferUpdates(restoreCollection, rc.asyncId, rc.shardHandler);
      markAllShardsAsActive(restoreCollection);
      addReplicasToShards(results, clusterState, restoreCollection, replicaPositions, rc.asyncId);
      restoringAlias(rc.backupProperties);

      log.info("Completed restoring collection={} backupName={}", restoreCollection, rc.backupName);

    }

    private void validate(DocCollection backupCollectionState, int availableNodeCount) {
      int numShards = backupCollectionState.getActiveSlices().size();
      int totalReplicasPerShard = numNrtReplicas + numTlogReplicas + numPullReplicas;
      assert totalReplicasPerShard > 0;

      int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, backupCollectionState.getMaxShardsPerNode());
      if (maxShardsPerNode != -1 && (numShards * totalReplicasPerShard) > (availableNodeCount * maxShardsPerNode)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            String.format(Locale.ROOT, "Solr cloud with available number of nodes:%d is insufficient for"
                    + " restoring a collection with %d shards, total replicas per shard %d and maxShardsPerNode %d."
                    + " Consider increasing maxShardsPerNode value OR number of available nodes.",
                availableNodeCount, numShards, totalReplicasPerShard, maxShardsPerNode));
      }
    }

    private void uploadConfig(String configName, String restoreConfigName, ZkStateReader zkStateReader, BackupManager backupMgr) throws IOException {
      if (zkStateReader.getConfigManager().configExists(restoreConfigName)) {
        log.info("Using existing config {}", restoreConfigName);
        //TODO add overwrite option?
      } else {
        log.info("Uploading config {}", restoreConfigName);
        backupMgr.uploadConfigDir(configName, restoreConfigName);
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createCoreLessCollection(String restoreCollectionName,
                                          String restoreConfigName,
                                          DocCollection backupCollectionState,
                                          ClusterState clusterState,
                                          BackupProperties backupProperties) throws Exception {
      // TODO JEGERLOW Where does STATE_FORMAT get read into for restores in 8.x?  Do I need to change BackupProperties to have it?
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATE.toString());
      propMap.put("fromApi", "true"); // mostly true.  Prevents autoCreated=true in the collection state.
      if (backupProperties.getStateFormat() == null) {
        propMap.put(STATE_FORMAT, "2");
      }
      propMap.put(REPLICATION_FACTOR, numNrtReplicas);
      propMap.put(NRT_REPLICAS, numNrtReplicas);
      propMap.put(TLOG_REPLICAS, numTlogReplicas);
      propMap.put(PULL_REPLICAS, numPullReplicas);
      propMap.put(MAX_SHARDS_PER_NODE, maxShardsPerNode);

      // inherit settings from input API, defaulting to the backup's setting.  Ex: replicationFactor
      for (String collProp : OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.keySet()) {
        Object val = message.getProperties().getOrDefault(collProp, backupCollectionState.get(collProp));
        if (val != null && propMap.get(collProp) == null) {
          propMap.put(collProp, val);
        }
      }

      propMap.put(NAME, restoreCollectionName);
      propMap.put(OverseerCollectionMessageHandler.CREATE_NODE_SET, OverseerCollectionMessageHandler.CREATE_NODE_SET_EMPTY); //no cores
      propMap.put(CollectionAdminParams.COLL_CONF, restoreConfigName);

      // router.*
      Map<String, Object> routerProps = (Map<String, Object>) backupCollectionState.getProperties().get(DocCollection.DOC_ROUTER);
      for (Map.Entry<String, Object> pair : routerProps.entrySet()) {
        propMap.put(DocCollection.DOC_ROUTER + "." + pair.getKey(), pair.getValue());
      }

      Set<String> sliceNames = backupCollectionState.getActiveSlicesMap().keySet();
      if (backupCollectionState.getRouter() instanceof ImplicitDocRouter) {
        propMap.put(OverseerCollectionMessageHandler.SHARDS_PROP, StrUtils.join(sliceNames, ','));
      } else {
        propMap.put(OverseerCollectionMessageHandler.NUM_SLICES, sliceNames.size());
        // ClusterStateMutator.createCollection detects that "slices" is in fact a slice structure instead of a
        //   list of names, and if so uses this instead of building it.  We clear the replica list.
        Collection<Slice> backupSlices = backupCollectionState.getActiveSlices();
        Map<String, Slice> newSlices = new LinkedHashMap<>(backupSlices.size());
        for (Slice backupSlice : backupSlices) {
          newSlices.put(backupSlice.getName(),
              new Slice(backupSlice.getName(), Collections.emptyMap(), backupSlice.getProperties(),restoreCollectionName));
        }
        propMap.put(OverseerCollectionMessageHandler.SHARDS_PROP, newSlices);
      }

      ocmh.commandMap.get(CREATE).call(clusterState, new ZkNodeProps(propMap), new NamedList());
      // note: when createCollection() returns, the collection exists (no race)
    }

    //Mark all shards in CONSTRUCTION STATE while we restore the data
    private void markAllShardsAsConstruction(DocCollection restoreCollection) throws KeeperException, InterruptedException {
      //TODO might instead createCollection accept an initial state?  Is there a race?
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.State.CONSTRUCTION.toString());
      }
      propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollection.getName());
      ocmh.overseer.offerStateUpdate(Utils.toJSON(new ZkNodeProps(propMap)));
    }

    private List<ReplicaPosition> getReplicaPositions(DocCollection restoreCollection, List<String> nodeList, ClusterState clusterState, List<String> sliceNames) throws IOException, InterruptedException {
      Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
          .forCollection(restoreCollection.getName())
          .forShard(sliceNames)
          .assignNrtReplicas(numNrtReplicas)
          .assignTlogReplicas(numTlogReplicas)
          .assignPullReplicas(numPullReplicas)
          .onNodes(nodeList)
          .build();
      Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(ocmh.cloudManager);
      Assign.AssignStrategy assignStrategy = assignStrategyFactory.create(clusterState, restoreCollection);
      return assignStrategy.assign(ocmh.cloudManager, assignRequest);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createSingleReplicaPerShard(NamedList results,
                                             DocCollection restoreCollection,
                                             String asyncId,
                                             ClusterState clusterState, List<ReplicaPosition> replicaPositions) throws Exception {
      CountDownLatch countDownLatch = new CountDownLatch(restoreCollection.getSlices().size());

      //Create one replica per shard and copy backed up data to it
      for (Slice slice : restoreCollection.getSlices()) {
        String sliceName = slice.getName();
        log.info("Adding replica for shard={} collection={} ", sliceName, restoreCollection);
        HashMap<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, CREATESHARD);
        propMap.put(COLLECTION_PROP, restoreCollection.getName());
        propMap.put(SHARD_ID_PROP, sliceName);

        if (numNrtReplicas >= 1) {
          propMap.put(REPLICA_TYPE, Replica.Type.NRT.name());
        } else if (numTlogReplicas >= 1) {
          propMap.put(REPLICA_TYPE, Replica.Type.TLOG.name());
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unexpected number of replicas, replicationFactor, " +
              Replica.Type.NRT + " or " + Replica.Type.TLOG + " must be greater than 0");
        }

        // Get the first node matching the shard to restore in
        String node;
        for (ReplicaPosition replicaPosition : replicaPositions) {
          if (Objects.equals(replicaPosition.shard, slice.getName())) {
            node = replicaPosition.node;
            propMap.put(CoreAdminParams.NODE, node);
            replicaPositions.remove(replicaPosition);
            break;
          }
        }

        // add async param
        if (asyncId != null) {
          propMap.put(ASYNC, asyncId);
        }
        ocmh.addPropertyParams(message, propMap);
        final NamedList addReplicaResult = new NamedList();
        ocmh.addReplica(clusterState, new ZkNodeProps(propMap), addReplicaResult, () -> {
          Object addResultFailure = addReplicaResult.get("failure");
          if (addResultFailure != null) {
            SimpleOrderedMap failure = (SimpleOrderedMap) results.get("failure");
            if (failure == null) {
              failure = new SimpleOrderedMap();
              results.add("failure", failure);
            }
            failure.addAll((NamedList) addResultFailure);
          } else {
            SimpleOrderedMap success = (SimpleOrderedMap) results.get("success");
            if (success == null) {
              success = new SimpleOrderedMap();
              results.add("success", success);
            }
            success.addAll((NamedList) addReplicaResult.get("success"));
          }
          countDownLatch.countDown();
        });
      }

      boolean allIsDone = countDownLatch.await(1, TimeUnit.HOURS);
      if (!allIsDone) {
        throw new TimeoutException("Initial replicas were not created within 1 hour. Timing out.");
      }
    }

    private void requestReplicasToApplyBufferUpdates(DocCollection restoreCollection, String asyncId, ShardHandler shardHandler) {
      ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);

      for (Slice s : restoreCollection.getSlices()) {
        for (Replica r : s.getReplicas()) {
          String nodeName = r.getNodeName();
          String coreNodeName = r.getCoreName();
          Replica.State stateRep = r.getState();

          log.debug("Calling REQUESTAPPLYUPDATES on: nodeName={}, coreNodeName={}, state={}", nodeName, coreNodeName, stateRep);

          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES.toString());
          params.set(CoreAdminParams.NAME, coreNodeName);

          shardRequestTracker.sendShardRequest(nodeName, params, shardHandler);
        }

        shardRequestTracker.processResponses(new NamedList(), shardHandler, true,
            "REQUESTAPPLYUPDATES calls did not succeed");
      }
    }

    //Mark all shards in ACTIVE STATE
    private void markAllShardsAsActive(DocCollection restoreCollection) throws KeeperException, InterruptedException {
      HashMap<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollection.getName());
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.State.ACTIVE.toString());
      }
      ocmh.overseer.offerStateUpdate((Utils.toJSON(new ZkNodeProps(propMap))));
    }

    private void addReplicasToShards(@SuppressWarnings({"rawtypes"}) NamedList results,
                                     ClusterState clusterState,
                                     DocCollection restoreCollection,
                                     List<ReplicaPosition> replicaPositions,
                                     String asyncId) throws Exception {
      int totalReplicasPerShard = numNrtReplicas + numTlogReplicas + numPullReplicas;
      if (totalReplicasPerShard > 1) {
        if (log.isInfoEnabled()) {
          log.info("Adding replicas to restored collection={}", restoreCollection.getName());
        }
        for (Slice slice : restoreCollection.getSlices()) {

          //Add the remaining replicas for each shard, considering it's type
          int createdNrtReplicas = 0, createdTlogReplicas = 0, createdPullReplicas = 0;

          // We already created either a NRT or an TLOG replica as leader
          if (numNrtReplicas > 0) {
            createdNrtReplicas++;
          } else if (numTlogReplicas > 0) {
            createdTlogReplicas++;
          }

          for (int i = 1; i < totalReplicasPerShard; i++) {
            Replica.Type typeToCreate;
            if (createdNrtReplicas < numNrtReplicas) {
              createdNrtReplicas++;
              typeToCreate = Replica.Type.NRT;
            } else if (createdTlogReplicas < numTlogReplicas) {
              createdTlogReplicas++;
              typeToCreate = Replica.Type.TLOG;
            } else {
              createdPullReplicas++;
              typeToCreate = Replica.Type.PULL;
              assert createdPullReplicas <= numPullReplicas : "Unexpected number of replicas";
            }

            if (log.isDebugEnabled()) {
              log.debug("Adding replica for shard={} collection={} of type {} ", slice.getName(), restoreCollection, typeToCreate);
            }
            HashMap<String, Object> propMap = new HashMap<>();
            propMap.put(COLLECTION_PROP, restoreCollection.getName());
            propMap.put(SHARD_ID_PROP, slice.getName());
            propMap.put(REPLICA_TYPE, typeToCreate.name());

            // Get the first node matching the shard to restore in
            String node;
            for (ReplicaPosition replicaPosition : replicaPositions) {
              if (Objects.equals(replicaPosition.shard, slice.getName())) {
                node = replicaPosition.node;
                propMap.put(CoreAdminParams.NODE, node);
                replicaPositions.remove(replicaPosition);
                break;
              }
            }

            // add async param
            if (asyncId != null) {
              propMap.put(ASYNC, asyncId);
            }
            ocmh.addPropertyParams(message, propMap);

            ocmh.addReplica(clusterState, new ZkNodeProps(propMap), results, null);
          }
        }
      }
    }

    private void restoringAlias(BackupProperties properties) {
      String backupCollection = properties.getCollection();
      String backupCollectionAlias = properties.getCollectionAlias();
      if (backupCollectionAlias != null && !backupCollectionAlias.equals(backupCollection)) {
        log.debug("Restoring alias {} -> {}", backupCollectionAlias, backupCollection);
        ocmh.zkStateReader.aliasesManager
                .applyModificationAndExportToZk(a -> a.cloneWithCollectionAlias(backupCollectionAlias, backupCollection));
      }
    }
  }

  /**
   * Restoration 'strategy' that ensures the collection being restored to already exists.
   *
   * @see RestoreOnANewCollection
   */
  private class RestoreOnExistingCollection {

    private RestoreOnExistingCollection(RestoreContext rc) {
      int numShardsOfBackup = rc.backupCollectionState.getSlices().size();
      int numShards = rc.zkStateReader.getClusterState().getCollection(rc.restoreCollectionName).getSlices().size();

      if (numShardsOfBackup != numShards) {
        String msg = String.format(Locale.ROOT, "Unable to restoring since number of shards in backup " +
                "and specified collection does not match, numShardsOfBackup:%d numShardsOfCollection:%d", numShardsOfBackup, numShards);
        throw new SolrException(ErrorCode.BAD_REQUEST, msg);
      }
    }

    public void process(RestoreContext rc, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
      ClusterState clusterState = rc.zkStateReader.getClusterState();
      DocCollection restoreCollection = clusterState.getCollection(rc.restoreCollectionName);

      enableReadOnly(clusterState, restoreCollection);
      try {
        requestReplicasToRestore(results, restoreCollection, clusterState, rc.backupProperties,
                rc.backupPath, rc.repo, rc.shardHandler, rc.asyncId);
      } finally {
        disableReadOnly(clusterState, restoreCollection);
      }
    }

    private void disableReadOnly(ClusterState clusterState, DocCollection restoreCollection) throws Exception {
      ZkNodeProps params = new ZkNodeProps(
              Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toString(),
              ZkStateReader.COLLECTION_PROP, restoreCollection.getName(),
              ZkStateReader.READ_ONLY, null
      );
      ocmh.modifyCollection(clusterState, params, new NamedList<>());
    }

    private void enableReadOnly(ClusterState clusterState, DocCollection restoreCollection) throws Exception {
      ZkNodeProps params = new ZkNodeProps(
              Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toString(),
              ZkStateReader.COLLECTION_PROP, restoreCollection.getName(),
              ZkStateReader.READ_ONLY, "true"
      );
      ocmh.modifyCollection(clusterState, params, new NamedList<>());
    }
  }
}
