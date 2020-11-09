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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.ConfigSetsHandlerApi;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class CreateShardCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public CreateShardCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public AddReplicaCmd.Response call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    return addShard(clusterState, clusterState.getCollection(message.getStr(COLLECTION_PROP)), message, results);
  }

  AddReplicaCmd.Response addShard(ClusterState clusterState, DocCollection collection, ZkNodeProps message, NamedList results) throws Exception {
    String extCollectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);

    log.info("Create shard invoked: {}", message);
    if (extCollectionName == null || sliceName == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'collection' and 'shard' are required parameters");

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collectionName;
    if (followAliases) {
      collectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }

    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, collection.getInt(NRT_REPLICAS, collection.getInt(REPLICATION_FACTOR, 1))));
    int numPullReplicas = message.getInt(PULL_REPLICAS, collection.getInt(PULL_REPLICAS, 0));
    int numTlogReplicas = message.getInt(TLOG_REPLICAS, collection.getInt(TLOG_REPLICAS, 0));

    if (numNrtReplicas + numTlogReplicas <= 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NRT_REPLICAS + " + " + TLOG_REPLICAS + " must be greater than 0");
    }

    // create shard
    clusterState = new CollectionMutator(ocmh.cloudManager).createShard(clusterState, message);

    log.info("After create shard {}", clusterState);

    String async = message.getStr(ASYNC);
    ZkNodeProps addReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, collectionName,
        SHARD_ID_PROP, sliceName,
        ZkStateReader.NRT_REPLICAS, String.valueOf(numNrtReplicas),
        ZkStateReader.TLOG_REPLICAS, String.valueOf(numTlogReplicas),
        ZkStateReader.PULL_REPLICAS, String.valueOf(numPullReplicas),
        ZkStateReader.CREATE_NODE_SET, message.getStr(ZkStateReader.CREATE_NODE_SET),
        CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.toString(waitForFinalState));

    Map<String, Object> propertyParams = new HashMap<>();
    ocmh.addPropertyParams(message, propertyParams);
    addReplicasProps = addReplicasProps.plus(propertyParams);
    if (async != null) addReplicasProps.getProperties().put(ASYNC, async);

    final String asyncId = message.getStr(ASYNC);
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);

    OverseerCollectionMessageHandler.ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId, message.getStr("operation"));

    final NamedList addResult = new NamedList();
    AddReplicaCmd.Response resp;
    try {
      //ocmh.addReplica(zkStateReader.getClusterState(), addReplicasProps, addResult, () -> {
      resp = new AddReplicaCmd(ocmh)
          .addReplica(clusterState, addReplicasProps, shardHandler, shardRequestTracker, results); //ocmh.addReplica(clusterState, addReplicasProps, addResult).clusterState;
      clusterState = resp.clusterState;
    } catch (Assign.AssignmentException e) {
      // clean up the slice that we created
      // nocommit
//      ZkNodeProps deleteShard = new ZkNodeProps(COLLECTION_PROP, collectionName, SHARD_ID_PROP, sliceName, ASYNC, async);
//      new DeleteShardCmd(ocmh).call(clusterState, deleteShard, results);
      throw e;
    }

//    () -> {
//      Object addResultFailure = addResult.get("failure");
//      if (addResultFailure != null) {
//        SimpleOrderedMap failure = (SimpleOrderedMap) results.get("failure");
//        if (failure == null) {
//          failure = new SimpleOrderedMap();
//          results.add("failure", failure);
//        }
//        failure.addAll((NamedList) addResultFailure);
//      } else {
//        SimpleOrderedMap success = (SimpleOrderedMap) results.get("success");
//        if (success == null) {
//          success = new SimpleOrderedMap();
//          results.add("success", success);
//        }
//        success.addAll((NamedList) addResult.get("success"));
//      }
//    }

    log.info("Finished create command on all shards for collection: {}", collectionName);
    AddReplicaCmd.Response response = new AddReplicaCmd.Response();

    response.asyncFinalRunner = new OverseerCollectionMessageHandler.Finalize() {
      @Override
      public AddReplicaCmd.Response call() {
        try {
          shardRequestTracker.processResponses(results, shardHandler, false, null, Collections.emptySet());
        } catch (KeeperException e) {
          log.error("", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        //  nocommit - put this in finalizer and finalizer after all calls to allow parallel and forward momentum

        if (resp.asyncFinalRunner != null) {
          try {
            resp.asyncFinalRunner.call();
          } catch (Exception e) {
            log.error("Exception waiting for active replicas", e);
          }
        }

        @SuppressWarnings({"rawtypes"}) boolean failure = results.get("failure") != null && ((SimpleOrderedMap) results.get("failure")).size() > 0;
        if (failure) {

        } else {

        }

        //ocmh.zkStateReader.waitForActiveCollection(collectionName, 10, TimeUnit.SECONDS, shardNames.size(), finalReplicaPositions.size());
        AddReplicaCmd.Response response = new AddReplicaCmd.Response();
        return response;
      }

    };

    response.clusterState = clusterState;
    return response;
  }

}
