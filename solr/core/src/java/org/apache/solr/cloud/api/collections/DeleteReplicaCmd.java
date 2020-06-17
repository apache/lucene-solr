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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COUNT_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
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
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeleteReplicaCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public DeleteReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings("unchecked")

  public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    deleteReplica(clusterState, message, results,null);
  }


  @SuppressWarnings("unchecked")
  void deleteReplica(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results, Runnable onComplete)
          throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("deleteReplica() : {}", Utils.toJSONString(message));
    }
    boolean parallel = message.getBool("parallel", false);

    //If a count is specified the strategy needs be different
    if (message.getStr(COUNT_PROP) != null) {
      deleteReplicaBasedOnCount(clusterState, message, results, onComplete, parallel);
      return;
    }


    ocmh.checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP);
    String extCollectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    String replicaName = message.getStr(REPLICA_PROP);

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collectionName;
    if (followAliases) {
      collectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }

    DocCollection coll = clusterState.getCollection(collectionName);
    Slice slice = coll.getSlice(shard);
    if (slice == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Invalid shard name : " +  shard + " in collection : " +  collectionName);
    }

    deleteCore(slice, collectionName, replicaName, message, shard, results, onComplete,  parallel);

  }


  /**
   * Delete replicas based on count for a given collection. If a shard is passed, uses that
   * else deletes given num replicas across all shards for the given collection.
   */
  @SuppressWarnings({"unchecked"})
  void deleteReplicaBasedOnCount(ClusterState clusterState,
                                 ZkNodeProps message,
                                 @SuppressWarnings({"rawtypes"})NamedList results,
                                 Runnable onComplete,
                                 boolean parallel)
          throws KeeperException, InterruptedException {
    ocmh.checkRequired(message, COLLECTION_PROP, COUNT_PROP);
    int count = Integer.parseInt(message.getStr(COUNT_PROP));
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    DocCollection coll = clusterState.getCollection(collectionName);
    Slice slice = null;
    //Validate if shard is passed.
    if (shard != null) {
      slice = coll.getSlice(shard);
      if (slice == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Invalid shard name : " +  shard +  " in collection : " + collectionName);
      }
    }

    Map<Slice, Set<String>> shardToReplicasMapping = new HashMap<Slice, Set<String>>();
    if (slice != null) {
      Set<String> replicasToBeDeleted = pickReplicasTobeDeleted(slice, shard, collectionName, count);
      shardToReplicasMapping.put(slice,replicasToBeDeleted);
    } else {

      //If there are many replicas left, remove the rest based on count.
      Collection<Slice> allSlices = coll.getSlices();
      for (Slice individualSlice : allSlices) {
        Set<String> replicasToBeDeleted = pickReplicasTobeDeleted(individualSlice, individualSlice.getName(), collectionName, count);
        shardToReplicasMapping.put(individualSlice, replicasToBeDeleted);
      }
    }

    for (Map.Entry<Slice, Set<String>> entry : shardToReplicasMapping.entrySet()) {
      Slice shardSlice = entry.getKey();
      String shardId = shardSlice.getName();
      Set<String> replicas = entry.getValue();
      //callDeleteReplica on all replicas
      for (String replica: replicas) {
        log.debug("Deleting replica {}  for shard {} based on count {}", replica, shardId, count);
        deleteCore(shardSlice, collectionName, replica, message, shard, results, onComplete, parallel);
      }
      results.add("shard_id", shardId);
      results.add("replicas_deleted", replicas);
    }

  }


  /**
   * Pick replicas to be deleted. Avoid picking the leader.
   */
  private Set<String> pickReplicasTobeDeleted(Slice slice, String shard, String collectionName, int count) {
    validateReplicaAvailability(slice, shard, collectionName, count);
    Collection<Replica> allReplicas = slice.getReplicas();
    Set<String> replicasToBeRemoved = new HashSet<String>();
    Replica leader = slice.getLeader();
    for (Replica replica: allReplicas) {
      if (count == 0) {
        break;
      }
      //Try avoiding to pick up the leader to minimize activity on the cluster.
      if (leader.getCoreName().equals(replica.getCoreName())) {
        continue;
      }
      replicasToBeRemoved.add(replica.getName());
      count --;
    }
    return replicasToBeRemoved;
  }

  /**
   * Validate if there is less replicas than requested to remove. Also error out if there is
   * only one replica available
   */
  private void validateReplicaAvailability(Slice slice, String shard, String collectionName, int count) {
    //If there is a specific shard passed, validate if there any or just 1 replica left
    if (slice != null) {
      Collection<Replica> allReplicasForShard = slice.getReplicas();
      if (allReplicasForShard == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No replicas found  in shard/collection: " +
                shard + "/"  + collectionName);
      }


      if (allReplicasForShard.size() == 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There is only one replica available in shard/collection: " +
                shard + "/" + collectionName + ". Cannot delete that.");
      }

      if (allReplicasForShard.size() <= count) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There are lesser num replicas requested to be deleted than are available in shard/collection : " +
                shard + "/"  + collectionName  + " Requested: "  + count + " Available: " + allReplicasForShard.size() + ".");
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  void deleteCore(Slice slice, String collectionName, String replicaName,ZkNodeProps message, String shard, @SuppressWarnings({"rawtypes"})NamedList results, Runnable onComplete, boolean parallel) throws KeeperException, InterruptedException {

    Replica replica = slice.getReplica(replicaName);
    if (replica == null) {
      ArrayList<String> l = new ArrayList<>();
      for (Replica r : slice.getReplicas())
        l.add(r.getName());
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid replica : " +  replicaName + " in shard/collection : " +
              shard  + "/" + collectionName + " available replicas are " +  StrUtils.join(l, ','));
    }

    // If users are being safe and only want to remove a shard if it is down, they can specify onlyIfDown=true
    // on the command.
    if (Boolean.parseBoolean(message.getStr(OverseerCollectionMessageHandler.ONLY_IF_DOWN)) && replica.getState() != Replica.State.DOWN) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Attempted to remove replica : " + collectionName + "/"  + shard + "/" + replicaName +
              " with onlyIfDown='true', but state is '" + replica.getStr(ZkStateReader.STATE_PROP) + "'");
    }

    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient());
    String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    String asyncId = message.getStr(ASYNC);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.UNLOAD.toString());
    params.add(CoreAdminParams.CORE, core);

    params.set(CoreAdminParams.DELETE_INDEX, message.getBool(CoreAdminParams.DELETE_INDEX, true));
    params.set(CoreAdminParams.DELETE_INSTANCE_DIR, message.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, true));
    params.set(CoreAdminParams.DELETE_DATA_DIR, message.getBool(CoreAdminParams.DELETE_DATA_DIR, true));
    params.set(CoreAdminParams.DELETE_METRICS_HISTORY, message.getBool(CoreAdminParams.DELETE_METRICS_HISTORY, true));

    boolean isLive = ocmh.zkStateReader.getClusterState().getLiveNodes().contains(replica.getNodeName());
    final ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId);
    if (isLive) {
      shardRequestTracker.sendShardRequest(replica.getNodeName(), params, shardHandler);
    }

    Callable<Boolean> callable = () -> {
      try {
        if (isLive) {
          shardRequestTracker.processResponses(results, shardHandler, false, null);

          //check if the core unload removed the corenode zk entry
          if (ocmh.waitForCoreNodeGone(collectionName, shard, replicaName, 30000)) return Boolean.TRUE;
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
                  "Could not remove replica : " + collectionName + "/" + shard + "/" + replicaName);
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
