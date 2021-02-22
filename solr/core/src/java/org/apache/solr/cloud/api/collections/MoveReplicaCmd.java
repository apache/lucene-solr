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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SKIP_CREATE_REPLICA_IN_CLUSTER_STATE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.IN_PLACE_MOVE;
import static org.apache.solr.common.params.CommonAdminParams.TIMEOUT;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;

public class MoveReplicaCmd implements CollApiCmds.CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionCommandContext ccc;

  public MoveReplicaCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    moveReplica(ccc.getZkStateReader().getClusterState(), message, results);
  }

  private void moveReplica(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("moveReplica() : {}", Utils.toJSONString(message));
    }
    CollectionHandlingUtils.checkRequired(message, COLLECTION_PROP, CollectionParams.TARGET_NODE);
    String extCollection = message.getStr(COLLECTION_PROP);
    String targetNode = message.getStr(CollectionParams.TARGET_NODE);
    boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);
    boolean inPlaceMove = message.getBool(IN_PLACE_MOVE, true);
    int timeout = message.getInt(TIMEOUT, 10 * 60); // 10 minutes

    String async = message.getStr(ASYNC);

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collection;
    if (followAliases) {
      collection = ccc.getSolrCloudManager().getClusterStateProvider().resolveSimpleAlias(extCollection);
    } else {
      collection = extCollection;
    }

    DocCollection coll = clusterState.getCollection(collection);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
    }
    if (!clusterState.getLiveNodes().contains(targetNode)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Target node: " + targetNode + " not in live nodes: " + clusterState.getLiveNodes());
    }
    Replica replica = null;
    if (message.containsKey(REPLICA_PROP)) {
      String replicaName = message.getStr(REPLICA_PROP);
      replica = coll.getReplica(replicaName);
      if (replica == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Collection: " + collection + " replica: " + replicaName + " does not exist");
      }
    } else {
      String sourceNode = message.getStr(CollectionParams.SOURCE_NODE, message.getStr(CollectionParams.FROM_NODE));
      if (sourceNode == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'" + CollectionParams.SOURCE_NODE +
            " or '" + CollectionParams.FROM_NODE + "' is a required param");
      }
      String shardId = message.getStr(SHARD_ID_PROP);
      if (shardId == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'" + SHARD_ID_PROP + "' is a required param");
      }
      Slice slice = coll.getSlice(shardId);
      List<Replica> sliceReplicas = new ArrayList<>(slice.getReplicas(r -> sourceNode.equals(r.getNodeName())));
      if (sliceReplicas.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Collection: " + collection + " node: " + sourceNode + " does not have any replica belonging to shard: " + shardId);
      }
      Collections.shuffle(sliceReplicas, CollectionHandlingUtils.RANDOM);
      replica = sliceReplicas.iterator().next();
    }

    log.info("Replica will be moved to node {}: {}", targetNode, replica);
    Slice slice = null;
    for (Slice s : coll.getSlices()) {
      if (s.getReplicas().contains(replica)) {
        slice = s;
      }
    }
    assert slice != null;
    Object dataDir = replica.get("dataDir");
    boolean isSharedFS = replica.getBool(ZkStateReader.SHARED_STORAGE_PROP, false) && dataDir != null;

    if (isSharedFS && inPlaceMove) {
      log.debug("-- moveHdfsReplica");
      moveHdfsReplica(clusterState, results, dataDir.toString(), targetNode, async, coll, replica, slice, timeout, waitForFinalState);
    } else {
      log.debug("-- moveNormalReplica (inPlaceMove={}, isSharedFS={}", inPlaceMove, isSharedFS);
      moveNormalReplica(clusterState, results, targetNode, async, coll, replica, slice, timeout, waitForFinalState);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void moveHdfsReplica(ClusterState clusterState, @SuppressWarnings({"rawtypes"})NamedList results, String dataDir, String targetNode, String async,
                                 DocCollection coll, Replica replica, Slice slice, int timeout, boolean waitForFinalState) throws Exception {
    String skipCreateReplicaInClusterState = "true";
    if (clusterState.getLiveNodes().contains(replica.getNodeName())) {
      skipCreateReplicaInClusterState = "false";
      ZkNodeProps removeReplicasProps = new ZkNodeProps(
          COLLECTION_PROP, coll.getName(),
          SHARD_ID_PROP, slice.getName(),
          REPLICA_PROP, replica.getName()
      );
      removeReplicasProps.getProperties().put(CoreAdminParams.DELETE_DATA_DIR, false);
      removeReplicasProps.getProperties().put(CoreAdminParams.DELETE_INDEX, false);
      if (async != null) removeReplicasProps.getProperties().put(ASYNC, async);
      @SuppressWarnings({"rawtypes"})
      NamedList deleteResult = new NamedList();
      try {
        new DeleteReplicaCmd(ccc).deleteReplica(clusterState, removeReplicasProps, deleteResult, null);
      } catch (SolrException e) {
        // assume this failed completely so there's nothing to roll back
        deleteResult.add("failure", e.toString());
      }
      if (deleteResult.get("failure") != null) {
        String errorString = String.format(Locale.ROOT, "Failed to cleanup replica collection=%s shard=%s name=%s, failure=%s",
            coll.getName(), slice.getName(), replica.getName(), deleteResult.get("failure"));
        log.warn(errorString);
        results.add("failure", errorString);
        return;
      }

      TimeOut timeOut = new TimeOut(20L, TimeUnit.SECONDS, ccc.getSolrCloudManager().getTimeSource());
      while (!timeOut.hasTimedOut()) {
        coll = ccc.getZkStateReader().getClusterState().getCollection(coll.getName());
        if (coll.getReplica(replica.getName()) != null) {
          timeOut.sleep(100);
        } else {
          break;
        }
      }
      if (timeOut.hasTimedOut()) {
        results.add("failure", "Still see deleted replica in clusterstate!");
        return;
      }

    }

    String ulogDir = replica.getStr(CoreAdminParams.ULOG_DIR);
    ZkNodeProps addReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, coll.getName(),
        SHARD_ID_PROP, slice.getName(),
        CoreAdminParams.NODE, targetNode,
        CoreAdminParams.CORE_NODE_NAME, replica.getName(),
        CoreAdminParams.NAME, replica.getCoreName(),
        WAIT_FOR_FINAL_STATE, String.valueOf(waitForFinalState),
        SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, skipCreateReplicaInClusterState,
        CoreAdminParams.ULOG_DIR, ulogDir.substring(0, ulogDir.lastIndexOf(UpdateLog.TLOG_NAME)),
        CoreAdminParams.DATA_DIR, dataDir,
        ZkStateReader.REPLICA_TYPE, replica.getType().name());

    if(async!=null) addReplicasProps.getProperties().put(ASYNC, async);
    @SuppressWarnings({"rawtypes"})
    NamedList addResult = new NamedList();
    try {
      new AddReplicaCmd(ccc).addReplica(ccc.getZkStateReader().getClusterState(), addReplicasProps, addResult, null);
    } catch (Exception e) {
      // fatal error - try rolling back
      String errorString = String.format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" +
          " on node=%s, failure=%s", coll.getName(), slice.getName(), targetNode, addResult.get("failure"));
      results.add("failure", errorString);
      log.warn("Error adding replica {} - trying to roll back...",  addReplicasProps, e);
      addReplicasProps = addReplicasProps.plus(CoreAdminParams.NODE, replica.getNodeName());
      @SuppressWarnings({"rawtypes"})
      NamedList rollback = new NamedList();
      new AddReplicaCmd(ccc).addReplica(ccc.getZkStateReader().getClusterState(), addReplicasProps, rollback, null);
      if (rollback.get("failure") != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Fatal error during MOVEREPLICA of " + replica
            + ", collection may be inconsistent: " + rollback.get("failure"));
      }
      return;
    }
    if (addResult.get("failure") != null) {
      String errorString = String.format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" +
          " on node=%s, failure=%s", coll.getName(), slice.getName(), targetNode, addResult.get("failure"));
      log.warn(errorString);
      results.add("failure", errorString);
      log.debug("--- trying to roll back...");
      // try to roll back
      addReplicasProps = addReplicasProps.plus(CoreAdminParams.NODE, replica.getNodeName());
      @SuppressWarnings({"rawtypes"})
      NamedList rollback = new NamedList();
      try {
        new AddReplicaCmd(ccc).addReplica(ccc.getZkStateReader().getClusterState(), addReplicasProps, rollback, null);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Fatal error during MOVEREPLICA of " + replica
            + ", collection may be inconsistent!", e);
      }
      if (rollback.get("failure") != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Fatal error during MOVEREPLICA of " + replica
            + ", collection may be inconsistent! Failure: " + rollback.get("failure"));
      }
      return;
    } else {
      String successString = String.format(Locale.ROOT, "MOVEREPLICA action completed successfully, moved replica=%s at node=%s " +
          "to replica=%s at node=%s", replica.getCoreName(), replica.getNodeName(), replica.getCoreName(), targetNode);
      results.add("success", successString);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void moveNormalReplica(ClusterState clusterState, @SuppressWarnings({"rawtypes"})NamedList results, String targetNode, String async,
                                 DocCollection coll, Replica replica, Slice slice, int timeout, boolean waitForFinalState) throws Exception {
    String newCoreName = Assign.buildSolrCoreName(ccc.getSolrCloudManager().getDistribStateManager(), coll, slice.getName(), replica.getType());
    ZkNodeProps addReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, coll.getName(),
        SHARD_ID_PROP, slice.getName(),
        CoreAdminParams.NODE, targetNode,
        CoreAdminParams.NAME, newCoreName,
        ZkStateReader.REPLICA_TYPE, replica.getType().name());

    if (async != null) addReplicasProps.getProperties().put(ASYNC, async);
    @SuppressWarnings({"rawtypes"})
    NamedList addResult = new NamedList();
    SolrCloseableLatch countDownLatch = new SolrCloseableLatch(1, ccc.getCloseableToLatchOn());
    ActiveReplicaWatcher watcher = null;
    ZkNodeProps props = new AddReplicaCmd(ccc).addReplica(clusterState, addReplicasProps, addResult, null).get(0);
    log.debug("props {}", props);
    if (replica.equals(slice.getLeader()) || waitForFinalState) {
      watcher = new ActiveReplicaWatcher(coll.getName(), null, Collections.singletonList(newCoreName), countDownLatch);
      log.debug("-- registered watcher {}", watcher);
      ccc.getZkStateReader().registerCollectionStateWatcher(coll.getName(), watcher);
    }
    if (addResult.get("failure") != null) {
      String errorString = String.format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" +
          " on node=%s, failure=%s", coll.getName(), slice.getName(), targetNode, addResult.get("failure"));
      log.warn(errorString);
      results.add("failure", errorString);
      if (watcher != null) { // unregister
        ccc.getZkStateReader().removeCollectionStateWatcher(coll.getName(), watcher);
      }
      return;
    }
    // wait for the other replica to be active if the source replica was a leader
    if (watcher != null) {
      try {
        log.debug("Waiting for leader's replica to recover.");
        if (!countDownLatch.await(timeout, TimeUnit.SECONDS)) {
          String errorString = String.format(Locale.ROOT, "Timed out waiting for leader's replica to recover, collection=%s shard=%s" +
              " on node=%s", coll.getName(), slice.getName(), targetNode);
          log.warn(errorString);
          results.add("failure", errorString);
          return;
        } else {
          if (log.isDebugEnabled()) {
            log.debug("Replica {} is active - deleting the source...", watcher.getActiveReplicas());
          }
        }
      } finally {
        ccc.getZkStateReader().removeCollectionStateWatcher(coll.getName(), watcher);
      }
    }

    ZkNodeProps removeReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, coll.getName(),
        SHARD_ID_PROP, slice.getName(),
        REPLICA_PROP, replica.getName());
    if (async != null) removeReplicasProps.getProperties().put(ASYNC, async);
    @SuppressWarnings({"rawtypes"})
    NamedList deleteResult = new NamedList();
    try {
      new DeleteReplicaCmd(ccc).deleteReplica(clusterState, removeReplicasProps, deleteResult, null);
    } catch (SolrException e) {
      deleteResult.add("failure", e.toString());
    }
    if (deleteResult.get("failure") != null) {
      String errorString = String.format(Locale.ROOT, "Failed to cleanup replica collection=%s shard=%s name=%s, failure=%s",
          coll.getName(), slice.getName(), replica.getName(), deleteResult.get("failure"));
      log.warn(errorString);
      results.add("failure", errorString);
    } else {
      String successString = String.format(Locale.ROOT, "MOVEREPLICA action completed successfully, moved replica=%s at node=%s " +
          "to replica=%s at node=%s", replica.getCoreName(), replica.getNodeName(), newCoreName, targetNode);
      results.add("success", successString);
    }
  }
}
