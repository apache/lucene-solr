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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.ONLY_IF_DOWN;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;


public class DeleteReplicaCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public DeleteReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings("unchecked")

  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    deleteReplica(clusterState, message, results,null);
  }

  @SuppressWarnings("unchecked")
  void deleteReplica(ClusterState clusterState, ZkNodeProps message, NamedList results, Runnable onComplete)
      throws KeeperException, InterruptedException {
    ocmh.checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP);
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    String replicaName = message.getStr(REPLICA_PROP);
    boolean parallel = message.getBool("parallel", false);

    DocCollection coll = clusterState.getCollection(collectionName);
    Slice slice = coll.getSlice(shard);
    if (slice == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Invalid shard name : " + shard + " in collection : " + collectionName);
    }
    Replica replica = slice.getReplica(replicaName);
    if (replica == null) {
      ArrayList<String> l = new ArrayList<>();
      for (Replica r : slice.getReplicas())
        l.add(r.getName());
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid replica : " + replicaName + " in shard/collection : "
          + shard + "/" + collectionName + " available replicas are " + StrUtils.join(l, ','));
    }

    // If users are being safe and only want to remove a shard if it is down, they can specify onlyIfDown=true
    // on the command.
    if (Boolean.parseBoolean(message.getStr(ONLY_IF_DOWN)) && replica.getState() != Replica.State.DOWN) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Attempted to remove replica : " + collectionName + "/" + shard + "/" + replicaName
              + " with onlyIfDown='true', but state is '" + replica.getStr(ZkStateReader.STATE_PROP) + "'");
    }

    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();
    String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    String asyncId = message.getStr(ASYNC);
    AtomicReference<Map<String, String>> requestMap = new AtomicReference<>(null);
    if (asyncId != null) {
      requestMap.set(new HashMap<>(1, 1.0f));
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.UNLOAD.toString());
    params.add(CoreAdminParams.CORE, core);

    params.set(CoreAdminParams.DELETE_INDEX, message.getBool(CoreAdminParams.DELETE_INDEX, true));
    params.set(CoreAdminParams.DELETE_INSTANCE_DIR, message.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, true));
    params.set(CoreAdminParams.DELETE_DATA_DIR, message.getBool(CoreAdminParams.DELETE_DATA_DIR, true));

    boolean isLive = ocmh.zkStateReader.getClusterState().getLiveNodes().contains(replica.getNodeName());
    if (isLive) {
      ocmh.sendShardRequest(replica.getNodeName(), params, shardHandler, asyncId, requestMap.get());
    }

    Callable<Boolean> callable = () -> {
      try {
        if (isLive) {
          ocmh.processResponses(results, shardHandler, false, null, asyncId, requestMap.get());

          //check if the core unload removed the corenode zk entry
          if (ocmh.waitForCoreNodeGone(collectionName, shard, replicaName, 5000)) return Boolean.TRUE;
        }

        // try and ensure core info is removed from cluster state
        ocmh.deleteCoreNode(collectionName, replicaName, replica, core);
        if (ocmh.waitForCoreNodeGone(collectionName, shard, replicaName, 30000)) return Boolean.TRUE;
        return Boolean.FALSE;
      } catch (Exception e) {
        results.add("failure", "Could not complete delete " + e.getMessage());
        throw e;
      } finally {
        if (onComplete != null) onComplete.run();
      }
    };

    if (!parallel) {
      try {
        if (!callable.call())
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Could not  remove replica : " + collectionName + "/" + shard + "/" + replicaName);
      } catch (InterruptedException | KeeperException e) {
        throw e;
      } catch (Exception ex) {
        throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Error waiting for corenode gone", ex);
      }

    } else {
      ocmh.tpe.submit(callable);
    }
  }

 }
