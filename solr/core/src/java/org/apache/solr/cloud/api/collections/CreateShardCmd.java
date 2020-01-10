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
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class CreateShardCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public CreateShardCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
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
    DocCollection collection = clusterState.getCollection(collectionName);

    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, collection.getInt(NRT_REPLICAS, collection.getInt(REPLICATION_FACTOR, 1))));
    int numPullReplicas = message.getInt(PULL_REPLICAS, collection.getInt(PULL_REPLICAS, 0));
    int numTlogReplicas = message.getInt(TLOG_REPLICAS, collection.getInt(TLOG_REPLICAS, 0));

    if (numNrtReplicas + numTlogReplicas <= 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NRT_REPLICAS + " + " + TLOG_REPLICAS + " must be greater than 0");
    }

    //ZkStateReader zkStateReader = ocmh.zkStateReader;
    ocmh.overseer.offerStateUpdate(Utils.toJSON(message));
    // wait for a while until we see the shard
    //ocmh.waitForNewShard(collectionName, sliceName);
    // wait for a while until we see the shard and update the local view of the cluster state
    clusterState = ocmh.waitForNewShard(collectionName, sliceName);

    String async = message.getStr(ASYNC);
    ZkNodeProps addReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, collectionName,
        SHARD_ID_PROP, sliceName,
        ZkStateReader.NRT_REPLICAS, String.valueOf(numNrtReplicas),
        ZkStateReader.TLOG_REPLICAS, String.valueOf(numTlogReplicas),
        ZkStateReader.PULL_REPLICAS, String.valueOf(numPullReplicas),
        OverseerCollectionMessageHandler.CREATE_NODE_SET, message.getStr(OverseerCollectionMessageHandler.CREATE_NODE_SET),
        CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.toString(waitForFinalState));

    Map<String, Object> propertyParams = new HashMap<>();
    ocmh.addPropertyParams(message, propertyParams);
    addReplicasProps = addReplicasProps.plus(propertyParams);
    if (async != null) addReplicasProps.getProperties().put(ASYNC, async);
    final NamedList addResult = new NamedList();
    try {
      //ocmh.addReplica(zkStateReader.getClusterState(), addReplicasProps, addResult, () -> {
      ocmh.addReplica(clusterState, addReplicasProps, addResult, () -> {
        Object addResultFailure = addResult.get("failure");
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
          success.addAll((NamedList) addResult.get("success"));
        }
      });
    } catch (Assign.AssignmentException e) {
      // clean up the slice that we created
      ZkNodeProps deleteShard = new ZkNodeProps(COLLECTION_PROP, collectionName, SHARD_ID_PROP, sliceName, ASYNC, async);
      new DeleteShardCmd(ocmh).call(clusterState, deleteShard, results);
      throw e;
    }

    log.info("Finished create command on all shards for collection: " + collectionName);
  }

}
