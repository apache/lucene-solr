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


import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Assign.getNodesForNewReplicas;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
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

    log.info("Create shard invoked: {}", message);
    if (collectionName == null || sliceName == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'collection' and 'shard' are required parameters");

    DocCollection collection = clusterState.getCollection(collectionName);
    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, collection.getInt(NRT_REPLICAS, collection.getInt(REPLICATION_FACTOR, 1))));
    int numPullReplicas = message.getInt(PULL_REPLICAS, collection.getInt(PULL_REPLICAS, 0));
    int numTlogReplicas = message.getInt(TLOG_REPLICAS, collection.getInt(TLOG_REPLICAS, 0));
    int totalReplicas = numNrtReplicas + numPullReplicas + numTlogReplicas;
    
    if (numNrtReplicas + numTlogReplicas <= 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NRT_REPLICAS + " + " + TLOG_REPLICAS + " must be greater than 0");
    }
    
    Object createNodeSetStr = message.get(OverseerCollectionMessageHandler.CREATE_NODE_SET);
    List<Assign.ReplicaCount> sortedNodeList = getNodesForNewReplicas(clusterState, collectionName, sliceName, totalReplicas,
        createNodeSetStr, ocmh.overseer.getZkController().getCoreContainer());

    ZkStateReader zkStateReader = ocmh.zkStateReader;
    Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));
    // wait for a while until we see the shard
    ocmh.waitForNewShard(collectionName, sliceName);

    String async = message.getStr(ASYNC);
    
    int createdNrtReplicas = 0, createdTlogReplicas = 0, createdPullReplicas = 0;
    CountDownLatch countDownLatch = new CountDownLatch(totalReplicas);
    for (int j = 1; j <= totalReplicas; j++) {
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
      }
      String nodeName = sortedNodeList.get(((j - 1)) % sortedNodeList.size()).nodeName;
      String coreName = Assign.buildCoreName(ocmh.zkStateReader.getZkClient(), collection, sliceName, typeToCreate);
//      String coreName = collectionName + "_" + sliceName + "_replica" + j;
      log.info("Creating replica " + coreName + " as part of slice " + sliceName + " of collection " + collectionName
          + " on " + nodeName);

      // Need to create new params for each request
      ZkNodeProps addReplicasProps = new ZkNodeProps(
          COLLECTION_PROP, collectionName,
          SHARD_ID_PROP, sliceName,
          CoreAdminParams.REPLICA_TYPE, typeToCreate.name(),
          CoreAdminParams.NODE, nodeName,
          CoreAdminParams.NAME, coreName);
      Map<String, Object> propertyParams = new HashMap<>();
      ocmh.addPropertyParams(message, propertyParams);;
      addReplicasProps = addReplicasProps.plus(propertyParams);
      if(async!=null) addReplicasProps.getProperties().put(ASYNC, async);
      final NamedList addResult = new NamedList();
      ocmh.addReplica(zkStateReader.getClusterState(), addReplicasProps, addResult, ()-> {
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

    log.debug("Waiting for create shard action to complete");
    countDownLatch.await(5, TimeUnit.MINUTES);
    log.debug("Finished waiting for create shard action to complete");

    log.info("Finished create command on all shards for collection: " + collectionName);

  }
}
