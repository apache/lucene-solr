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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.SKIP_CREATE_REPLICA_IN_CLUSTER_STATE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.TIMEOUT;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;

public class AddReplicaCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * When AddReplica is called with this set to true, then we do not try to find node assignments
   * for the add replica API. If set to true, a valid "node" should be specified.
   */
  public static final String SKIP_NODE_ASSIGNMENT = "skipNodeAssignment";

  private final OverseerCollectionMessageHandler ocmh;
  private final boolean onlyUpdateState;
  private boolean createdShardHandler;

  public AddReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this.onlyUpdateState = false;
    this.ocmh = ocmh;
  }

  public AddReplicaCmd(OverseerCollectionMessageHandler ocmh, boolean onlyUpdateState) {
    this.onlyUpdateState = onlyUpdateState;
    this.ocmh = ocmh;
  }

  @Override
  public Response call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    ShardHandler shardHandler = null;
    ShardRequestTracker shardRequestTracker = null;

    if (!onlyUpdateState) {
      final String asyncId = message.getStr(ASYNC);
      shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);

      shardRequestTracker = ocmh.asyncRequestTracker(asyncId, message.getStr(Overseer.QUEUE_OPERATION));
      createdShardHandler = true;
    }

    Response response = addReplica(state, message, shardHandler, shardRequestTracker, results);
    return response;
  }

  @SuppressWarnings({"unchecked"})
  Response addReplica(ClusterState clusterState, ZkNodeProps message, ShardHandler shardHandler,
      ShardRequestTracker shardRequestTracker, @SuppressWarnings({"rawtypes"})NamedList results)
      throws IOException, InterruptedException, KeeperException {

    log.info("addReplica() : {}", Utils.toJSONString(message));

    String extCollectionName = message.getStr(COLLECTION_PROP);
    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);

    final String collectionName;
    if (followAliases) {
      collectionName =  ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }

    // MRM TODO:
    boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);


    String node = message.getStr(CoreAdminParams.NODE);
    String createNodeSetStr = message.getStr(ZkStateReader.CREATE_NODE_SET);

    if (node != null && createNodeSetStr != null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Both 'node' and 'createNodeSet' parameters cannot be specified together.");
    }

    // MRM TODO:
    int timeout = message.getInt(TIMEOUT, 15); // 10 minutes

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
      if (node != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot create " + totalReplicas + " replicas if 'node' parameter is specified");
      }
    }

    List<CreateReplica> createReplicas = new ArrayList<>();

    DocCollection collection = clusterState.getCollection(collectionName);
    List<ReplicaPosition> positions = buildReplicaPositions(ocmh.cloudManager, clusterState, collection, message, replicaTypesVsCount);
    for (ReplicaPosition replicaPosition : positions) {
      clusterState = new CollectionMutator(ocmh.cloudManager).modifyCollection(clusterState, message);
      collection = clusterState.getCollection(collectionName);
      CreateReplica cr = assignReplicaDetails(collection, message, replicaPosition);

      message = message.plus(NODE_NAME_PROP, replicaPosition.node);
      message = message.plus(ZkStateReader.REPLICA_TYPE, cr.replicaType.name());

      clusterState = new SliceMutator(ocmh.cloudManager).addReplica(clusterState, message);
      createReplicas.add(cr);

     // message.getProperties().put("node_name", cr.node)
    }

//    createReplicas = buildReplicaPositions(ocmh.cloudManager, clusterState, collection, message, replicaTypesVsCount)
//        .stream()
//        .map(replicaPosition -> assignReplicaDetails(collection, message, replicaPosition))
//        .collect(Collectors.toList());


    for (CreateReplica createReplica : createReplicas) {

      ModifiableSolrParams params = getReplicaParams(collection, message, results, skipCreateReplicaInClusterState, shardHandler, createReplica);

      log.info("create replica {} params={}", createReplica, params);
      if (!onlyUpdateState) {
        shardRequestTracker.sendShardRequest(createReplica.node, params, shardHandler);
      }
    }

    Response response = new Response();

    if (!onlyUpdateState) {
      DocCollection finalCollection = collection;
      response.responseProps = createReplicas.stream().map(
          createReplica -> ZkNodeProps.fromKeyVals("id", createReplica.id, "collId", finalCollection.getId(), ZkStateReader.COLLECTION_PROP, createReplica.collectionName, ZkStateReader.SHARD_ID_PROP, createReplica.sliceName, ZkStateReader.CORE_NAME_PROP,
              createReplica.coreName, ZkStateReader.NODE_NAME_PROP, createReplica.node)).collect(Collectors.toList());
      response.results = results;

        ZkNodeProps finalMessage = message;
        response.asyncFinalRunner = new OverseerCollectionMessageHandler.Finalize() {
          @Override
          public Response call() {
            if (!onlyUpdateState && createdShardHandler) {
              try {
                 log.info("Processs responses");
                shardRequestTracker.processResponses(results, shardHandler, true, "ADDREPLICA failed to create replica");
              } catch (Exception e) {
                ParWork.propagateInterrupt(e);
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
              }
            }

            String asyncId = finalMessage.getStr(ASYNC);
            for (CreateReplica createReplica : createReplicas) {
              waitForActiveReplica(createReplica.sliceName, collectionName, asyncId, ocmh.zkStateReader, createReplicas);
            }
            AddReplicaCmd.Response response = new AddReplicaCmd.Response();
            return response;
          }
        };

    }

    response.clusterState = clusterState;

    return response;
  }

  private void waitForActiveReplica(String shard, String collectionName, String asyncId, ZkStateReader zkStateReader, List<CreateReplica> createReplicas) {
    Set<String> coreNames = new HashSet<>(createReplicas.size());
    for (CreateReplica replica : createReplicas) {
      coreNames.add(replica.coreName);
    }
    try {
      log.info("waiting for created replicas shard={} {}", shard, coreNames);
      zkStateReader.waitForState(collectionName, 30, TimeUnit.SECONDS, (liveNodes, collectionState) -> { // MRM TODO: timeout
        if (collectionState == null) {
          return false;
        }

        Slice slice = collectionState.getSlice(shard);
        if (slice == null || slice.getLeader() == null) {
          return false;
        }

        int found = 0;
        for (String name : coreNames) {
          Replica replica = collectionState.getReplica(name);
          if (replica != null) {
            if (replica.getState().equals(Replica.State.ACTIVE)) {
              found++;
            }
          }
        }
        if (found == coreNames.size()) {
          return true;
        }

        return false;
      });
    } catch (TimeoutException | InterruptedException e) {
      log.error("addReplica", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private ModifiableSolrParams getReplicaParams(DocCollection collection, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results,
      boolean skipCreateReplicaInClusterState,
      ShardHandler shardHandler, CreateReplica createReplica) throws IOException, InterruptedException, KeeperException {

    // MRM TODO:
//    if (collection.getStr(WITH_COLLECTION) != null) {
//      String withCollectionName = collection.getStr(WITH_COLLECTION);
//      DocCollection withCollection = clusterState.getCollection(withCollectionName);
//      if (withCollection.getActiveSlices().size() > 1)  {
//        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The `withCollection` must have only one shard, found: " + withCollection.getActiveSlices().size());
//      }
//      String withCollectionShard = withCollection.getActiveSlices().iterator().next().getName();
//
//      List<Replica> replicas = withCollection.getReplicas(createReplica.node);
//      if (replicas == null || replicas.isEmpty()) {
//        // create a replica of withCollection on the identified node before proceeding further
//        ZkNodeProps props = new ZkNodeProps(
//            Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
//            ZkStateReader.COLLECTION_PROP, withCollectionName,
//            ZkStateReader.SHARD_ID_PROP, withCollectionShard,
//            CORE_NAME_PROP, createReplica.coreName,
//            "node", createReplica.node,
//            // since we already computed node assignments (which include assigning a node for this withCollection replica) we want to skip the assignment step
//            SKIP_NODE_ASSIGNMENT, "true",
//            CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.TRUE.toString()); // set to true because we want `withCollection` to be ready after this collection is created
//        addReplica(clusterState, props, results);
//      }
//    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    String collectionName = collection.getName();
    ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower(), ZkStateReader.COLLECTION_PROP, collectionName, ZkStateReader.SHARD_ID_PROP, createReplica.sliceName,
        ZkStateReader.CORE_NAME_PROP, createReplica.coreName, ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString(), ZkStateReader.NODE_NAME_PROP, createReplica.node, ZkStateReader.REPLICA_TYPE, createReplica.replicaType.name());

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

    boolean bufferOnStart = message.getBool(CoreAdminParams.PROPERTY_PREFIX + "bufferOnStart", false);
    if (bufferOnStart) {
      params.set(CoreAdminParams.PROPERTY_PREFIX + "bufferOnStart", "true");
    }
    params.set(CoreAdminParams.PROPERTY_PREFIX + "id", Long.toString(createReplica.id));
    params.set(CoreAdminParams.PROPERTY_PREFIX + "collId", Long.toString(collection.getId()));

    log.info("Creating SolrCore with name={}", createReplica.coreName);
    if (createReplica.sliceName != null) {
      params.set(CoreAdminParams.SHARD, createReplica.sliceName);
    } else if (routeKey != null) {
      Collection<Slice> slices = collection.getRouter().getSearchSlicesSingle(routeKey, null, collection);
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

    ocmh.addPropertyParams(message, params);

    return params;
  }

  public static CreateReplica assignReplicaDetails(DocCollection coll,
                                                 ZkNodeProps message, ReplicaPosition replicaPosition) {

    log.info("assignReplicaDetails {} {}", message, replicaPosition);

    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);

    String collection = message.getStr(COLLECTION_PROP);
    String node = replicaPosition.node;
    String shard = message.getStr(SHARD_ID_PROP);
    String coreName = message.getStr(CoreAdminParams.CORE);
    Replica.Type replicaType = replicaPosition.type;

    if (log.isDebugEnabled()) log.debug("Node Identified {} for creating new replica (core={}) of shard {} for collection {} currentReplicaCount {}", node, coreName, shard, collection, coll.getReplicas().size());

    long id = coll.getHighestReplicaId();
    if (coreName == null) {
      coreName = Assign.buildSolrCoreName(coll, shard, replicaType);
    }
    if (log.isDebugEnabled()) log.debug("Returning CreateReplica command coreName={}", coreName);

    return new CreateReplica(id, collection, shard, node, replicaType, coreName);
  }

  public static List<ReplicaPosition> buildReplicaPositions(SolrCloudManager cloudManager, ClusterState clusterState, DocCollection collection,
                                                            ZkNodeProps message,
                                                            EnumMap<Replica.Type, Integer> replicaTypeVsCount) throws IOException, InterruptedException {
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);
    boolean skipNodeAssignment = message.getBool(SKIP_NODE_ASSIGNMENT, false);
    String sliceName = message.getStr(SHARD_ID_PROP);

    int numNrtReplicas = replicaTypeVsCount.get(Replica.Type.NRT);
    int numPullReplicas = replicaTypeVsCount.get(Replica.Type.PULL);
    int numTlogReplicas = replicaTypeVsCount.get(Replica.Type.TLOG);
    int totalReplicas = numNrtReplicas + numPullReplicas + numTlogReplicas;

    String node = message.getStr(CoreAdminParams.NODE);
    Object createNodeSetStr = message.get(ZkStateReader.CREATE_NODE_SET);
    if (createNodeSetStr == null) {
      if (node != null) {
        message.getProperties().put(ZkStateReader.CREATE_NODE_SET, node);
        createNodeSetStr = node;
      }
    }

    List<ReplicaPosition> positions = null;
    if (!skipCreateReplicaInClusterState && !skipNodeAssignment) {

      positions = Assign.getNodesForNewReplicas(clusterState, collection, sliceName, numNrtReplicas,
                    numTlogReplicas, numPullReplicas, createNodeSetStr, cloudManager);
    }

    if (positions == null)  {
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
    private final long id;
    public String coreName;

    CreateReplica(long id, String collectionName, String sliceName, String node, Replica.Type replicaType, String coreName) {
      this.id = id;
      this.collectionName = collectionName;
      this.sliceName = sliceName;
      this.node = node;
      this.replicaType = replicaType;
      this.coreName = coreName;
    }
  }

  public static class Response {
    List<ZkNodeProps> responseProps;
    OverseerCollectionMessageHandler.Finalize asyncFinalRunner;

    NamedList results;

    ClusterState clusterState;
  }

}
