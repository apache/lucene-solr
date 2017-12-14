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
package org.apache.solr.cloud;


import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Assign.getNodesForNewReplicas;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.RANDOM;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class CreateShardCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public CreateShardCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    String collectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);

    log.info("Create shard invoked: {}", message);
    if (collectionName == null || sliceName == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'collection' and 'shard' are required parameters");

    DocCollection collection = clusterState.getCollection(collectionName);

    ZkStateReader zkStateReader = ocmh.zkStateReader;
    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper = new AtomicReference<>();
    SolrCloseableLatch countDownLatch;
    try {
      List<ReplicaPosition> positions = buildReplicaPositions(ocmh.cloudManager, clusterState, collectionName, message, sessionWrapper);
      Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));
      // wait for a while until we see the shard
      ocmh.waitForNewShard(collectionName, sliceName);

      String async = message.getStr(ASYNC);
      countDownLatch = new SolrCloseableLatch(positions.size(), ocmh);
      for (ReplicaPosition position : positions) {
        String nodeName = position.node;
        String coreName = Assign.buildSolrCoreName(ocmh.cloudManager.getDistribStateManager(), collection, sliceName, position.type);
        log.info("Creating replica " + coreName + " as part of slice " + sliceName + " of collection " + collectionName
            + " on " + nodeName);

        // Need to create new params for each request
        ZkNodeProps addReplicasProps = new ZkNodeProps(
            COLLECTION_PROP, collectionName,
            SHARD_ID_PROP, sliceName,
            ZkStateReader.REPLICA_TYPE, position.type.name(),
            CoreAdminParams.NODE, nodeName,
            CoreAdminParams.NAME, coreName,
            CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.toString(waitForFinalState));
        Map<String, Object> propertyParams = new HashMap<>();
        ocmh.addPropertyParams(message, propertyParams);
        addReplicasProps = addReplicasProps.plus(propertyParams);
        if (async != null) addReplicasProps.getProperties().put(ASYNC, async);
        final NamedList addResult = new NamedList();
        ocmh.addReplica(zkStateReader.getClusterState(), addReplicasProps, addResult, () -> {
          countDownLatch.countDown();
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
      }
    } finally {
      if (sessionWrapper.get() != null) sessionWrapper.get().release();
    }

    log.debug("Waiting for create shard action to complete");
    countDownLatch.await(5, TimeUnit.MINUTES);
    log.debug("Finished waiting for create shard action to complete");

    log.info("Finished create command on all shards for collection: " + collectionName);

  }

  public static List<ReplicaPosition> buildReplicaPositions(SolrCloudManager cloudManager, ClusterState clusterState,
         String collectionName, ZkNodeProps message, AtomicReference< PolicyHelper.SessionWrapper> sessionWrapper) throws IOException, InterruptedException {
    String sliceName = message.getStr(SHARD_ID_PROP);
    DocCollection collection = clusterState.getCollection(collectionName);

    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, collection.getInt(NRT_REPLICAS, collection.getInt(REPLICATION_FACTOR, 1))));
    int numPullReplicas = message.getInt(PULL_REPLICAS, collection.getInt(PULL_REPLICAS, 0));
    int numTlogReplicas = message.getInt(TLOG_REPLICAS, collection.getInt(TLOG_REPLICAS, 0));
    int totalReplicas = numNrtReplicas + numPullReplicas + numTlogReplicas;

    if (numNrtReplicas + numTlogReplicas <= 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NRT_REPLICAS + " + " + TLOG_REPLICAS + " must be greater than 0");
    }

    Object createNodeSetStr = message.get(OverseerCollectionMessageHandler.CREATE_NODE_SET);

    boolean usePolicyFramework = CloudUtil.usePolicyFramework(collection, cloudManager);
    List<ReplicaPosition> positions;
    if (usePolicyFramework) {
      if (collection.getPolicyName() != null) message.getProperties().put(Policy.POLICY, collection.getPolicyName());
      positions = Assign.identifyNodes(cloudManager,
          clusterState,
          Assign.getLiveOrLiveAndCreateNodeSetList(clusterState.getLiveNodes(), message, RANDOM),
          collection.getName(),
          message,
          Collections.singletonList(sliceName),
          numNrtReplicas,
          numTlogReplicas,
          numPullReplicas);
      sessionWrapper.set(PolicyHelper.getLastSessionWrapper(true));
    } else {
      List<Assign.ReplicaCount> sortedNodeList = getNodesForNewReplicas(clusterState, collection.getName(), sliceName, totalReplicas,
          createNodeSetStr, cloudManager);
      int i = 0;
      positions = new ArrayList<>();
      for (Map.Entry<Replica.Type, Integer> e : ImmutableMap.of(Replica.Type.NRT, numNrtReplicas,
          Replica.Type.TLOG, numTlogReplicas,
          Replica.Type.PULL, numPullReplicas
      ).entrySet()) {
        for (int j = 0; j < e.getValue(); j++) {
          positions.add(new ReplicaPosition(sliceName, j + 1, e.getKey(), sortedNodeList.get(i % sortedNodeList.size()).nodeName));
          i++;
        }
      }
    }
    return positions;
  }

}
