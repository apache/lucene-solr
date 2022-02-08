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


import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.CREATE_NODE_SET;
import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.SKIP_CREATE_REPLICA_IN_CLUSTER_STATE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.TIMEOUT;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddReplicaCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * When AddReplica is called with this set to true, then we do not try to find node assignments
   * for the add replica API. If set to true, a valid "node" should be specified.
   */
  public static final String SKIP_NODE_ASSIGNMENT = "skipNodeAssignment";

  private final OverseerCollectionMessageHandler ocmh;

  public AddReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    addReplica(state, message, results, null);
  }

  @SuppressWarnings({"unchecked"})
  List<ZkNodeProps> addReplica(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results, Runnable onComplete)
      throws IOException, InterruptedException, KeeperException {
    if (log.isDebugEnabled()) {
      log.debug("addReplica() : {}", Utils.toJSONString(message));
    }

    String extCollectionName = message.getStr(COLLECTION_PROP);
    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String shard = message.getStr(SHARD_ID_PROP);

    final String collectionName;
    if (followAliases) {
      collectionName =  ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }

    DocCollection coll = clusterState.getCollection(collectionName);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collectionName + " does not exist");
    }
    if (coll.getSlice(shard) == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Collection: " + collectionName + " shard: " + shard + " does not exist");
    }

    boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);
    final String asyncId = message.getStr(ASYNC);

    String node = message.getStr(CoreAdminParams.NODE);
    String createNodeSetStr = message.getStr(CREATE_NODE_SET);

    if (node != null && createNodeSetStr != null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Both 'node' and 'createNodeSet' parameters cannot be specified together.");
    }

    int timeout = message.getInt(TIMEOUT, 10 * 60); // 10 minutes
    boolean parallel = message.getBool("parallel", false);

    Replica.Type replicaType = Replica.Type.valueOf(message.getStr(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name()).toUpperCase(Locale.ROOT));
    EnumMap<Replica.Type, Integer> replicaTypesVsCount = new EnumMap<>(Replica.Type.class);
    replicaTypesVsCount.put(Replica.Type.NRT, message.getInt(NRT_REPLICAS, replicaType == Replica.Type.NRT ? 1 : 0));
    replicaTypesVsCount.put(Replica.Type.TLOG, message.getInt(TLOG_REPLICAS, replicaType == Replica.Type.TLOG ? 1 : 0));
    replicaTypesVsCount.put(Replica.Type.PULL, message.getInt(PULL_REPLICAS, replicaType == Replica.Type.PULL ? 1 : 0));

    int totalReplicas = 0;
    for (Map.Entry<Replica.Type, Integer> entry : replicaTypesVsCount.entrySet()) {
      totalReplicas += entry.getValue();
    }
    if (totalReplicas > 1)  {
      if (message.getStr(CoreAdminParams.NAME) != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot create " + totalReplicas + " replicas if 'name' parameter is specified");
      }
      if (message.getStr(CoreAdminParams.CORE_NODE_NAME) != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot create " + totalReplicas + " replicas if 'coreNodeName' parameter is specified");
      }
    }

    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper = new AtomicReference<>();
    List<CreateReplica> createReplicas;
    try {
      createReplicas = buildReplicaPositions(ocmh.cloudManager, clusterState, collectionName, message, replicaTypesVsCount, sessionWrapper)
          .stream()
          .map(replicaPosition -> assignReplicaDetails(ocmh.cloudManager, clusterState, message, replicaPosition))
          .collect(Collectors.toList());
    } finally {
      if (sessionWrapper.get() != null) {
        sessionWrapper.get().release();
      }
    }

    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();
    ZkStateReader zkStateReader = ocmh.zkStateReader;

    final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
    for (CreateReplica createReplica : createReplicas) {
      assert createReplica.coreName != null;
      ModifiableSolrParams params = getReplicaParams(clusterState, message, results, collectionName, coll, skipCreateReplicaInClusterState, asyncId, shardHandler, createReplica);
      shardRequestTracker.sendShardRequest(createReplica.node, params, shardHandler);
    }

    Runnable runnable = () -> {
      shardRequestTracker.processResponses(results, shardHandler, true, "ADDREPLICA failed to create replica");
      for (CreateReplica replica : createReplicas) {
        ocmh.waitForCoreNodeName(collectionName, replica.node, replica.coreName);
      }
      if (onComplete != null) onComplete.run();
    };

    if (!parallel || waitForFinalState) {
      if (waitForFinalState) {
        SolrCloseableLatch latch = new SolrCloseableLatch(totalReplicas, ocmh);
        ActiveReplicaWatcher watcher = new ActiveReplicaWatcher(collectionName, null,
            createReplicas.stream().map(createReplica -> createReplica.coreName).collect(Collectors.toList()), latch);
        try {
          zkStateReader.registerCollectionStateWatcher(collectionName, watcher);
          runnable.run();
          if (!latch.await(timeout, TimeUnit.SECONDS)) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout waiting " + timeout + " seconds for replica to become active.");
          }
        } finally {
          zkStateReader.removeCollectionStateWatcher(collectionName, watcher);
        }
      } else {
        runnable.run();
      }
    } else {
      ocmh.tpe.submit(runnable);
    }

    return createReplicas.stream()
        .map(createReplica -> new ZkNodeProps(
            ZkStateReader.COLLECTION_PROP, createReplica.collectionName,
            ZkStateReader.SHARD_ID_PROP, createReplica.sliceName,
            ZkStateReader.CORE_NAME_PROP, createReplica.coreName,
            ZkStateReader.NODE_NAME_PROP, createReplica.node,
            ZkStateReader.BASE_URL_PROP, zkStateReader.getBaseUrlForNodeName(createReplica.node)
        ))
        .collect(Collectors.toList());
  }

  private ModifiableSolrParams getReplicaParams(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results, String collectionName, DocCollection coll, boolean skipCreateReplicaInClusterState, String asyncId, ShardHandler shardHandler, CreateReplica createReplica) throws IOException, InterruptedException, KeeperException {
    if (coll.getStr(WITH_COLLECTION) != null) {
      String withCollectionName = coll.getStr(WITH_COLLECTION);
      DocCollection withCollection = clusterState.getCollection(withCollectionName);
      if (withCollection.getActiveSlices().size() > 1)  {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The `withCollection` must have only one shard, found: " + withCollection.getActiveSlices().size());
      }
      String withCollectionShard = withCollection.getActiveSlices().iterator().next().getName();

      List<Replica> replicas = withCollection.getReplicas(createReplica.node);
      if (replicas == null || replicas.isEmpty()) {
        // create a replica of withCollection on the identified node before proceeding further
        ZkNodeProps props = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
            ZkStateReader.COLLECTION_PROP, withCollectionName,
            ZkStateReader.SHARD_ID_PROP, withCollectionShard,
            "node", createReplica.node,
            // since we already computed node assignments (which include assigning a node for this withCollection replica) we want to skip the assignment step
            SKIP_NODE_ASSIGNMENT, "true",
            CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.TRUE.toString()); // set to true because we want `withCollection` to be ready after this collection is created
        addReplica(clusterState, props, results, null);
      }
    }

    ModifiableSolrParams params = new ModifiableSolrParams();

    ZkStateReader zkStateReader = ocmh.zkStateReader;
    if (!Overseer.isLegacy(zkStateReader)) {
      if (!skipCreateReplicaInClusterState) {
        ZkNodeProps props = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, ADDREPLICA.toLower(),
            ZkStateReader.COLLECTION_PROP, collectionName,
            ZkStateReader.SHARD_ID_PROP, createReplica.sliceName,
            ZkStateReader.CORE_NAME_PROP, createReplica.coreName,
            ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
            ZkStateReader.NODE_NAME_PROP, createReplica.node,
            ZkStateReader.REPLICA_TYPE, createReplica.replicaType.name());
        if (createReplica.coreNodeName != null) {
          props = props.plus(ZkStateReader.CORE_NODE_NAME_PROP, createReplica.coreNodeName);
        }
        try {
          ocmh.overseer.offerStateUpdate(Utils.toJSON(props));
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception updating Overseer state queue", e);
        }
      }
      params.set(CoreAdminParams.CORE_NODE_NAME,
          ocmh.waitToSeeReplicasInState(collectionName, Collections.singletonList(createReplica.coreName)).get(createReplica.coreName).getName());
    }

    String configName = zkStateReader.readConfigName(collectionName);
    String routeKey = message.getStr(ShardParams._ROUTE_);
    String dataDir = message.getStr(CoreAdminParams.DATA_DIR);
    String ulogDir = message.getStr(CoreAdminParams.ULOG_DIR);
    String instanceDir = message.getStr(CoreAdminParams.INSTANCE_DIR);

    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATE.toString());
    params.set(CoreAdminParams.NAME, createReplica.coreName);
    params.set(COLL_CONF, configName);
    params.set(CoreAdminParams.COLLECTION, collectionName);
    params.set(CoreAdminParams.REPLICA_TYPE, createReplica.replicaType.name());
    if (createReplica.sliceName != null) {
      params.set(CoreAdminParams.SHARD, createReplica.sliceName);
    } else if (routeKey != null) {
      Collection<Slice> slices = coll.getRouter().getSearchSlicesSingle(routeKey, null, coll);
      if (slices.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No active shard serving _route_=" + routeKey + " found");
      } else {
        params.set(CoreAdminParams.SHARD, slices.iterator().next().getName());
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Specify either 'shard' or _route_ param");
    }
    if (dataDir != null) {
      params.set(CoreAdminParams.DATA_DIR, dataDir);
    }
    if (ulogDir != null) {
      params.set(CoreAdminParams.ULOG_DIR, ulogDir);
    }
    if (instanceDir != null) {
      params.set(CoreAdminParams.INSTANCE_DIR, instanceDir);
    }
    if (createReplica.coreNodeName != null) {
      params.set(CoreAdminParams.CORE_NODE_NAME, createReplica.coreNodeName);
    }
    ocmh.addPropertyParams(message, params);

    return params;
  }

  public static CreateReplica assignReplicaDetails(SolrCloudManager cloudManager, ClusterState clusterState,
                                                 ZkNodeProps message, ReplicaPosition replicaPosition) {
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);

    String collection = message.getStr(COLLECTION_PROP);
    String node = replicaPosition.node;
    String shard = message.getStr(SHARD_ID_PROP);
    String coreName = message.getStr(CoreAdminParams.NAME);
    String coreNodeName = message.getStr(CoreAdminParams.CORE_NODE_NAME);
    Replica.Type replicaType = replicaPosition.type;

    if (StringUtils.isBlank(coreName)) {
      coreName = message.getStr(CoreAdminParams.PROPERTY_PREFIX + CoreAdminParams.NAME);
    }

    log.info("Node Identified {} for creating new replica of shard {} for collection {}", node, shard, collection);
    if (!clusterState.liveNodesContain(node)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Node: " + node + " is not live");
    }
    DocCollection coll = clusterState.getCollection(collection);
    if (coreName == null) {
      coreName = Assign.buildSolrCoreName(cloudManager.getDistribStateManager(), coll, shard, replicaType);
    } else if (!skipCreateReplicaInClusterState) {
      //Validate that the core name is unique in that collection
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          String replicaCoreName = replica.getStr(CORE_NAME_PROP);
          if (coreName.equals(replicaCoreName)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Another replica with the same core name already exists" +
                " for this collection");
          }
        }
      }
    }
    log.info("Returning CreateReplica command.");
    return new CreateReplica(collection, shard, node, replicaType, coreName, coreNodeName);
  }

  public static List<ReplicaPosition> buildReplicaPositions(SolrCloudManager cloudManager, ClusterState clusterState,
                                                            String collectionName, ZkNodeProps message,
                                                            EnumMap<Replica.Type, Integer> replicaTypeVsCount,
                                                            AtomicReference< PolicyHelper.SessionWrapper> sessionWrapper) throws IOException, InterruptedException {
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);
    boolean skipNodeAssignment = message.getBool(SKIP_NODE_ASSIGNMENT, false);
    String sliceName = message.getStr(SHARD_ID_PROP);
    DocCollection collection = clusterState.getCollection(collectionName);

    int numNrtReplicas = replicaTypeVsCount.get(Replica.Type.NRT);
    int numPullReplicas = replicaTypeVsCount.get(Replica.Type.PULL);
    int numTlogReplicas = replicaTypeVsCount.get(Replica.Type.TLOG);
    int totalReplicas = numNrtReplicas + numPullReplicas + numTlogReplicas;

    String node = message.getStr(CoreAdminParams.NODE);
    Object createNodeSetStr = message.get(OverseerCollectionMessageHandler.CREATE_NODE_SET);
    if (createNodeSetStr == null) {
      if (node != null) {
        message.getProperties().put(OverseerCollectionMessageHandler.CREATE_NODE_SET, node);
        createNodeSetStr = node;
      }
    }

    List<ReplicaPosition> positions = null;
    if (!skipCreateReplicaInClusterState && !skipNodeAssignment) {

      positions = Assign.getNodesForNewReplicas(clusterState, collection.getName(), sliceName, numNrtReplicas,
                    numTlogReplicas, numPullReplicas, createNodeSetStr, cloudManager);
      sessionWrapper.set(PolicyHelper.getLastSessionWrapper(true));
    }

    if (positions == null)  {
      assert node != null;
      if (node == null) {
        // in case asserts are disabled
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "A node should have been identified to add replica but wasn't. Please inform solr developers at SOLR-9317");
      }
      // it is unlikely that multiple replicas have been requested to be created on
      // the same node, but we've got to accommodate.
      positions = new ArrayList<>(totalReplicas);
      int i = 0;
      for (Map.Entry<Replica.Type, Integer> entry : replicaTypeVsCount.entrySet()) {
        for (int j = 0; j < entry.getValue(); j++) {
          positions.add(new ReplicaPosition(sliceName, i++, entry.getKey(), node));
        }
      }
    }
    return positions;
  }

  /**
   * A data structure to keep all information required to create a new replica in one place.
   * Think of it as a typed ZkNodeProps for replica creation.
   *
   * This is <b>not</b> a public API and can be changed at any time without notice.
   */
  public static class CreateReplica {
    public final String collectionName;
    public final String sliceName;
    public final String node;
    public final Replica.Type replicaType;
    public String coreName;
    public String coreNodeName;

    CreateReplica(String collectionName, String sliceName, String node, Replica.Type replicaType, String coreName, String coreNodeName) {
      this.collectionName = collectionName;
      this.sliceName = sliceName;
      this.node = node;
      this.replicaType = replicaType;
      this.coreName = coreName;
      this.coreNodeName = coreNodeName;
    }
  }

}
