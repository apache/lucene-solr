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


import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.cloud.Overseer;
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
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.component.ShardHandler;
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
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    // TODO maybe we can inherit createCollection's options/code

    String restoreCollectionName = message.getStr(COLLECTION_PROP);
    String backupName = message.getStr(NAME); // of backup
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();
    String asyncId = message.getStr(ASYNC);
    String repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);
    Map<String, String> requestMap = new HashMap<>();

    CoreContainer cc = ocmh.overseer.getCoreContainer();
    BackupRepository repository = cc.newBackupRepository(Optional.ofNullable(repo));

    URI location = repository.createURI(message.getStr(CoreAdminParams.BACKUP_LOCATION));
    URI backupPath = repository.resolve(location, backupName);
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    BackupManager backupMgr = new BackupManager(repository, zkStateReader);

    Properties properties = backupMgr.readBackupProperties(location, backupName);
    String backupCollection = properties.getProperty(BackupManager.COLLECTION_NAME_PROP);
    DocCollection backupCollectionState = backupMgr.readCollectionState(location, backupName, backupCollection);

    // Get the Solr nodes to restore a collection.
    final List<String> nodeList = Assign.getLiveOrLiveAndCreateNodeSetList(
        zkStateReader.getClusterState().getLiveNodes(), message, OverseerCollectionMessageHandler.RANDOM);

    int numShards = backupCollectionState.getActiveSlices().size();
    
    int numNrtReplicas = getInt(message, NRT_REPLICAS, backupCollectionState.getNumNrtReplicas(), 0);
    if (numNrtReplicas == 0) {
      numNrtReplicas = getInt(message, REPLICATION_FACTOR, backupCollectionState.getReplicationFactor(), 0);
    }
    int numTlogReplicas = getInt(message, TLOG_REPLICAS, backupCollectionState.getNumTlogReplicas(), 0);
    int numPullReplicas = getInt(message, PULL_REPLICAS, backupCollectionState.getNumPullReplicas(), 0);
    int totalReplicasPerShard = numNrtReplicas + numTlogReplicas + numPullReplicas;
    
    int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, backupCollectionState.getMaxShardsPerNode());
    int availableNodeCount = nodeList.size();
    if ((numShards * totalReplicasPerShard) > (availableNodeCount * maxShardsPerNode)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT, "Solr cloud with available number of nodes:%d is insufficient for"
              + " restoring a collection with %d shards, total replicas per shard %d and maxShardsPerNode %d."
              + " Consider increasing maxShardsPerNode value OR number of available nodes.",
              availableNodeCount, numShards, totalReplicasPerShard, maxShardsPerNode));
    }

    //Upload the configs
    String configName = (String) properties.get(OverseerCollectionMessageHandler.COLL_CONF);
    String restoreConfigName = message.getStr(OverseerCollectionMessageHandler.COLL_CONF, configName);
    if (zkStateReader.getConfigManager().configExists(restoreConfigName)) {
      log.info("Using existing config {}", restoreConfigName);
      //TODO add overwrite option?
    } else {
      log.info("Uploading config {}", restoreConfigName);
      backupMgr.uploadConfigDir(location, backupName, configName, restoreConfigName);
    }

    log.info("Starting restore into collection={} with backup_name={} at location={}", restoreCollectionName, backupName,
        location);

    //Create core-less collection
    {
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATE.toString());
      propMap.put("fromApi", "true"); // mostly true.  Prevents autoCreated=true in the collection state.
      if (properties.get(STATE_FORMAT) == null) {
        propMap.put(STATE_FORMAT, "2");
      }

      // inherit settings from input API, defaulting to the backup's setting.  Ex: replicationFactor
      for (String collProp : OverseerCollectionMessageHandler.COLL_PROPS.keySet()) {
        Object val = message.getProperties().getOrDefault(collProp, backupCollectionState.get(collProp));
        if (val != null) {
          propMap.put(collProp, val);
        }
      }

      propMap.put(NAME, restoreCollectionName);
      propMap.put(OverseerCollectionMessageHandler.CREATE_NODE_SET, OverseerCollectionMessageHandler.CREATE_NODE_SET_EMPTY); //no cores
      propMap.put(OverseerCollectionMessageHandler.COLL_CONF, restoreConfigName);

      // router.*
      @SuppressWarnings("unchecked")
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
              new Slice(backupSlice.getName(), Collections.emptyMap(), backupSlice.getProperties()));
        }
        propMap.put(OverseerCollectionMessageHandler.SHARDS_PROP, newSlices);
      }

      ocmh.commandMap.get(CREATE).call(zkStateReader.getClusterState(), new ZkNodeProps(propMap), new NamedList());
      // note: when createCollection() returns, the collection exists (no race)
    }

    DocCollection restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());

    //Mark all shards in CONSTRUCTION STATE while we restore the data
    {
      //TODO might instead createCollection accept an initial state?  Is there a race?
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.State.CONSTRUCTION.toString());
      }
      propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollectionName);
      inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
    }

    // TODO how do we leverage the RULE / SNITCH logic in createCollection?

    ClusterState clusterState = zkStateReader.getClusterState();

    List<String> sliceNames = new ArrayList<>();
    restoreCollection.getSlices().forEach(x -> sliceNames.add(x.getName()));
    PolicyHelper.SessionWrapper sessionWrapper = null;

    try {
      List<ReplicaPosition> replicaPositions = Assign.identifyNodes(
          ocmh.cloudManager, clusterState,
          nodeList, restoreCollectionName,
          message, sliceNames,
          numNrtReplicas, numTlogReplicas, numPullReplicas);
      sessionWrapper = PolicyHelper.getLastSessionWrapper(true);
      //Create one replica per shard and copy backed up data to it
      for (Slice slice : restoreCollection.getSlices()) {
        log.debug("Adding replica for shard={} collection={} ", slice.getName(), restoreCollection);
        HashMap<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, CREATESHARD);
        propMap.put(COLLECTION_PROP, restoreCollectionName);
        propMap.put(SHARD_ID_PROP, slice.getName());

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

        ocmh.addReplica(clusterState, new ZkNodeProps(propMap), new NamedList(), null);
      }

      //refresh the location copy of collection state
      restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

      //Copy data from backed up index to each replica
      for (Slice slice : restoreCollection.getSlices()) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.RESTORECORE.toString());
        params.set(NAME, "snapshot." + slice.getName());
        params.set(CoreAdminParams.BACKUP_LOCATION, backupPath.toASCIIString());
        params.set(CoreAdminParams.BACKUP_REPOSITORY, repo);

        ocmh.sliceCmd(clusterState, params, null, slice, shardHandler, asyncId, requestMap);
      }
      ocmh.processResponses(new NamedList(), shardHandler, true, "Could not restore core", asyncId, requestMap);

      //Mark all shards in ACTIVE STATE
      {
        HashMap<String, Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollectionName);
        for (Slice shard : restoreCollection.getSlices()) {
          propMap.put(shard.getName(), Slice.State.ACTIVE.toString());
        }
        inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
      }

      //refresh the location copy of collection state
      restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

      if (totalReplicasPerShard > 1) {
        log.info("Adding replicas to restored collection={}", restoreCollection);
        for (Slice slice : restoreCollection.getSlices()) {

          //Add the remaining replicas for each shard, considering it's type
          int createdNrtReplicas = 0, createdTlogReplicas = 0, createdPullReplicas = 0;

          // We already created either a NRT or an TLOG replica as leader
          if (numNrtReplicas > 0) {
            createdNrtReplicas++;
          } else if (createdTlogReplicas > 0) {
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
              assert createdPullReplicas <= numPullReplicas: "Unexpected number of replicas";
            }

            log.debug("Adding replica for shard={} collection={} of type {} ", slice.getName(), restoreCollection, typeToCreate);
            HashMap<String, Object> propMap = new HashMap<>();
            propMap.put(COLLECTION_PROP, restoreCollectionName);
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

            ocmh.addReplica(zkStateReader.getClusterState(), new ZkNodeProps(propMap), results, null);
          }
        }
      }

      log.info("Completed restoring collection={} backupName={}", restoreCollection, backupName);
    } finally {
      if (sessionWrapper != null) sessionWrapper.release();
    }
  }

  private int getInt(ZkNodeProps message, String propertyName, Integer count, int defaultValue) {
    Integer value = message.getInt(propertyName, count);
    return value!=null ? value:defaultValue;
  }
}
