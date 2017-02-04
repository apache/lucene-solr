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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Assign.getNodesForNewReplicas;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.COLL_CONF;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

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
    int numSlices = 1;

    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();
    DocCollection collection = clusterState.getCollection(collectionName);
    int repFactor = message.getInt(REPLICATION_FACTOR, collection.getInt(REPLICATION_FACTOR, 1));
    Object createNodeSetStr = message.get(OverseerCollectionMessageHandler.CREATE_NODE_SET);
    List<Assign.ReplicaCount> sortedNodeList = getNodesForNewReplicas(clusterState, collectionName, sliceName, repFactor,
        createNodeSetStr, ocmh.overseer.getZkController().getCoreContainer());

    ZkStateReader zkStateReader = ocmh.zkStateReader;
    Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));
    // wait for a while until we see the shard
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    boolean created = false;
    while (!timeout.hasTimedOut()) {
      Thread.sleep(100);
      created = zkStateReader.getClusterState().getCollection(collectionName).getSlice(sliceName) != null;
      if (created) break;
    }
    if (!created)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not fully create shard: " + message.getStr(NAME));

    String configName = message.getStr(COLL_CONF);

    String async = message.getStr(ASYNC);
    Map<String, String> requestMap = null;
    if (async != null) {
      requestMap = new HashMap<>(repFactor, 1.0f);
    }

    for (int j = 1; j <= repFactor; j++) {
      String nodeName = sortedNodeList.get(((j - 1)) % sortedNodeList.size()).nodeName;
      String shardName = collectionName + "_" + sliceName + "_replica" + j;
      log.info("Creating shard " + shardName + " as part of slice " + sliceName + " of collection " + collectionName
          + " on " + nodeName);

      // Need to create new params for each request
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATE.toString());
      params.set(CoreAdminParams.NAME, shardName);
      params.set(COLL_CONF, configName);
      params.set(CoreAdminParams.COLLECTION, collectionName);
      params.set(CoreAdminParams.SHARD, sliceName);
      params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);
      ocmh.addPropertyParams(message, params);

      ocmh.sendShardRequest(nodeName, params, shardHandler, async, requestMap);
    }

    ocmh.processResponses(results, shardHandler, true, "Failed to create shard", async, requestMap, Collections.emptySet());

    log.info("Finished create command on all shards for collection: " + collectionName);

  }
}
