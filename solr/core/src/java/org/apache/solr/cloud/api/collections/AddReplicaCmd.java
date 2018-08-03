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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.SKIP_CREATE_REPLICA_IN_CLUSTER_STATE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.TIMEOUT;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;

public class AddReplicaCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public AddReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    addReplica(state, message, results, null);
  }

  ZkNodeProps addReplica(ClusterState clusterState, ZkNodeProps message, NamedList results, Runnable onComplete)
      throws IOException, InterruptedException {
    log.debug("addReplica() : {}", Utils.toJSONString(message));

    String collectionName = message.getStr(COLLECTION_PROP);
    DocCollection coll = clusterState.getCollection(collectionName);

    boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);
    final String asyncId = message.getStr(ASYNC);

    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper = new AtomicReference<>();
    message = assignReplicaDetails(ocmh.cloudManager, clusterState, message, sessionWrapper);

    String node = message.getStr(CoreAdminParams.NODE);
    String shard = message.getStr(SHARD_ID_PROP);
    String coreName = message.getStr(CoreAdminParams.NAME);
    String coreNodeName = message.getStr(CoreAdminParams.CORE_NODE_NAME);
    int timeout = message.getInt(TIMEOUT, 10 * 60); // 10 minutes
    Replica.Type replicaType = Replica.Type.valueOf(message.getStr(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name()).toUpperCase(Locale.ROOT));
    boolean parallel = message.getBool("parallel", false);

    if (coll.getStr(WITH_COLLECTION) != null) {
      String withCollectionName = coll.getStr(WITH_COLLECTION);
      DocCollection withCollection = clusterState.getCollection(withCollectionName);
      if (withCollection.getActiveSlices().size() > 1)  {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The `withCollection` must have only one shard, found: " + withCollection.getActiveSlices().size());
      }
      String withCollectionShard = withCollection.getActiveSlices().iterator().next().getName();

      List<Replica> replicas = withCollection.getReplicas(node);
      if (replicas == null || replicas.isEmpty()) {
        // create a replica of withCollection on the identified node before proceeding further
        ZkNodeProps props = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
            ZkStateReader.COLLECTION_PROP, withCollectionName,
            ZkStateReader.SHARD_ID_PROP, withCollectionShard,
            "node", node,
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
            ZkStateReader.SHARD_ID_PROP, shard,
            ZkStateReader.CORE_NAME_PROP, coreName,
            ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
            ZkStateReader.BASE_URL_PROP, zkStateReader.getBaseUrlForNodeName(node),
            ZkStateReader.NODE_NAME_PROP, node,
            ZkStateReader.REPLICA_TYPE, replicaType.name());
        if (coreNodeName != null) {
          props = props.plus(ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);
        }
        try {
          Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(props));
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception updating Overseer state queue", e);
        }
      }
      params.set(CoreAdminParams.CORE_NODE_NAME,
          ocmh.waitToSeeReplicasInState(collectionName, Collections.singletonList(coreName)).get(coreName).getName());
    }

    String configName = zkStateReader.readConfigName(collectionName);
    String routeKey = message.getStr(ShardParams._ROUTE_);
    String dataDir = message.getStr(CoreAdminParams.DATA_DIR);
    String ulogDir = message.getStr(CoreAdminParams.ULOG_DIR);
    String instanceDir = message.getStr(CoreAdminParams.INSTANCE_DIR);

    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATE.toString());
    params.set(CoreAdminParams.NAME, coreName);
    params.set(COLL_CONF, configName);
    params.set(CoreAdminParams.COLLECTION, collectionName);
    params.set(CoreAdminParams.REPLICA_TYPE, replicaType.name());
    if (shard != null) {
      params.set(CoreAdminParams.SHARD, shard);
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
    if (coreNodeName != null) {
      params.set(CoreAdminParams.CORE_NODE_NAME, coreNodeName);
    }
    ocmh.addPropertyParams(message, params);

    // For tracking async calls.
    Map<String,String> requestMap = new HashMap<>();
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();

    ocmh.sendShardRequest(node, params, shardHandler, asyncId, requestMap);

    final String fnode = node;
    final String fcoreName = coreName;

    Runnable runnable = () -> {
      ocmh.processResponses(results, shardHandler, true, "ADDREPLICA failed to create replica", asyncId, requestMap);
      ocmh.waitForCoreNodeName(collectionName, fnode, fcoreName);
      if (sessionWrapper.get() != null) {
        sessionWrapper.get().release();
      }
      if (onComplete != null) onComplete.run();
    };

    if (!parallel || waitForFinalState) {
      if (waitForFinalState) {
        SolrCloseableLatch latch = new SolrCloseableLatch(1, ocmh);
        ActiveReplicaWatcher watcher = new ActiveReplicaWatcher(collectionName, null, Collections.singletonList(coreName), latch);
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


    return new ZkNodeProps(
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.SHARD_ID_PROP, shard,
        ZkStateReader.CORE_NAME_PROP, coreName,
        ZkStateReader.NODE_NAME_PROP, node
    );
  }

  public static ZkNodeProps assignReplicaDetails(SolrCloudManager cloudManager, ClusterState clusterState,
                                                 ZkNodeProps message, AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper) throws IOException, InterruptedException {
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);

    String collection = message.getStr(COLLECTION_PROP);
    String node = message.getStr(CoreAdminParams.NODE);
    String shard = message.getStr(SHARD_ID_PROP);
    String coreName = message.getStr(CoreAdminParams.NAME);
    String coreNodeName = message.getStr(CoreAdminParams.CORE_NODE_NAME);
    Replica.Type replicaType = Replica.Type.valueOf(message.getStr(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name()).toUpperCase(Locale.ROOT));
    if (StringUtils.isBlank(coreName)) {
      coreName = message.getStr(CoreAdminParams.PROPERTY_PREFIX + CoreAdminParams.NAME);
    }

    DocCollection coll = clusterState.getCollection(collection);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
    }
    if (coll.getSlice(shard) == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Collection: " + collection + " shard: " + shard + " does not exist");
    }

    // Kind of unnecessary, but it does put the logic of whether to override maxShardsPerNode in one place.
    if (!skipCreateReplicaInClusterState) {
      if (CloudUtil.usePolicyFramework(coll, cloudManager)) {
        if (node == null) {
          if(coll.getPolicyName() != null) message.getProperties().put(Policy.POLICY, coll.getPolicyName());
          node = Assign.identifyNodes(cloudManager,
              clusterState,
              Collections.emptyList(),
              collection,
              message,
              Collections.singletonList(shard),
              replicaType == Replica.Type.NRT ? 1 : 0,
              replicaType == Replica.Type.TLOG ? 1 : 0,
              replicaType == Replica.Type.PULL ? 1 : 0
          ).get(0).node;
          sessionWrapper.set(PolicyHelper.getLastSessionWrapper(true));
        }
      } else {
        node = Assign.getNodesForNewReplicas(clusterState, collection, shard, 1, node,
            cloudManager).get(0).nodeName;// TODO: use replica type in this logic too
      }
    }
    log.info("Node Identified {} for creating new replica of shard {}", node, shard);

    if (!clusterState.liveNodesContain(node)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Node: " + node + " is not live");
    }
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
    if (coreNodeName != null) {
      message = message.plus(CoreAdminParams.CORE_NODE_NAME, coreNodeName);
    }
    message = message.plus(CoreAdminParams.NAME, coreName);
    message = message.plus(CoreAdminParams.NODE, node);
    return message;
  }
}
