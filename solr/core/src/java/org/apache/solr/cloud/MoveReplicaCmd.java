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
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.*;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class MoveReplicaCmd implements Cmd{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public MoveReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    moveReplica(ocmh.zkStateReader.getClusterState(), message, results);
  }

  private void moveReplica(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    log.info("moveReplica() : {}", Utils.toJSONString(message));
    ocmh.checkRequired(message, COLLECTION_PROP, "targetNode");
    String collection = message.getStr(COLLECTION_PROP);
    String targetNode = message.getStr("targetNode");

    String async = message.getStr(ASYNC);

    DocCollection coll = clusterState.getCollection(collection);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
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
      ocmh.checkRequired(message, SHARD_ID_PROP, "fromNode");
      String fromNode = message.getStr("fromNode");
      String shardId = message.getStr(SHARD_ID_PROP);
      Slice slice = clusterState.getCollection(collection).getSlice(shardId);
      List<Replica> sliceReplicas = new ArrayList<>(slice.getReplicas());
      Collections.shuffle(sliceReplicas, RANDOM);
      for (Replica r : slice.getReplicas()) {
        if (r.getNodeName().equals(fromNode)) {
          replica = r;
        }
      }
      if (replica == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Collection: " + collection + " node: " + fromNode + " do not have any replica belong to shard: " + shardId);
      }
    }

    log.info("Replica will be moved {}", replica);
    Slice slice = null;
    for (Slice s : coll.getSlices()) {
      if (s.getReplicas().contains(replica)) {
        slice = s;
      }
    }
    assert slice != null;
    Object dataDir = replica.get("dataDir");
    if (dataDir != null && dataDir.toString().startsWith("hdfs:/")) {
      moveHdfsReplica(clusterState, results, dataDir.toString(), targetNode, async, coll, replica, slice);
    } else {
      moveNormalReplica(clusterState, results, targetNode, async, coll, replica, slice);
    }
  }

  private void moveHdfsReplica(ClusterState clusterState, NamedList results, String dataDir, String targetNode, String async,
                                 DocCollection coll, Replica replica, Slice slice) throws Exception {
    String newCoreName = Assign.buildCoreName(coll, slice.getName());

    ZkNodeProps removeReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, coll.getName(),
        SHARD_ID_PROP, slice.getName(),
        REPLICA_PROP, replica.getName()
        );
    removeReplicasProps.getProperties().put(CoreAdminParams.DELETE_DATA_DIR, false);
    removeReplicasProps.getProperties().put(CoreAdminParams.DELETE_INDEX, false);
    if(async!=null) removeReplicasProps.getProperties().put(ASYNC, async);
    NamedList deleteResult = new NamedList();
    ocmh.deleteReplica(clusterState, removeReplicasProps, deleteResult, null);
    if (deleteResult.get("failure") != null) {
      String errorString = String.format(Locale.ROOT, "Failed to cleanup replica collection=%s shard=%s name=%s",
          coll.getName(), slice.getName(), replica.getName());
      log.warn(errorString);
      results.add("failure", errorString + ", because of : " + deleteResult.get("failure"));
      return;
    }

    ZkNodeProps addReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, coll.getName(),
        SHARD_ID_PROP, slice.getName(),
        CoreAdminParams.NODE, targetNode,
        CoreAdminParams.NAME, newCoreName,
        CoreAdminParams.DATA_DIR, dataDir);
    if(async!=null) addReplicasProps.getProperties().put(ASYNC, async);
    NamedList addResult = new NamedList();
    ocmh.addReplica(clusterState, addReplicasProps, addResult, null);
    if (addResult.get("failure") != null) {
      String errorString = String.format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" +
          " on node=%s", coll.getName(), slice.getName(), targetNode);
      log.warn(errorString);
      results.add("failure", errorString);
      return;
    } else {
      String successString = String.format(Locale.ROOT, "MOVEREPLICA action completed successfully, moved replica=%s at node=%s " +
          "to replica=%s at node=%s", replica.getCoreName(), replica.getNodeName(), newCoreName, targetNode);
      results.add("success", successString);
    }
  }

  private void moveNormalReplica(ClusterState clusterState, NamedList results, String targetNode, String async,
                                 DocCollection coll, Replica replica, Slice slice) throws Exception {
    String newCoreName = Assign.buildCoreName(coll, slice.getName());
    ZkNodeProps addReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, coll.getName(),
        SHARD_ID_PROP, slice.getName(),
        CoreAdminParams.NODE, targetNode,
        CoreAdminParams.NAME, newCoreName);
    if(async!=null) addReplicasProps.getProperties().put(ASYNC, async);
    NamedList addResult = new NamedList();
    ocmh.addReplica(clusterState, addReplicasProps, addResult, null);
    if (addResult.get("failure") != null) {
      String errorString = String.format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" +
          " on node=%s", coll.getName(), slice.getName(), targetNode);
      log.warn(errorString);
      results.add("failure", errorString);
      return;
    }

    ZkNodeProps removeReplicasProps = new ZkNodeProps(
        COLLECTION_PROP, coll.getName(),
        SHARD_ID_PROP, slice.getName(),
        REPLICA_PROP, replica.getName());
    if(async!=null) removeReplicasProps.getProperties().put(ASYNC, async);
    NamedList deleteResult = new NamedList();
    ocmh.deleteReplica(clusterState, removeReplicasProps, deleteResult, null);
    if (deleteResult.get("failure") != null) {
      String errorString = String.format(Locale.ROOT, "Failed to cleanup replica collection=%s shard=%s name=%s",
          coll.getName(), slice.getName(), replica.getName());
      log.warn(errorString);
      results.add("failure", errorString + ", because of : " + deleteResult.get("failure"));
    } else {
      String successString = String.format(Locale.ROOT, "MOVEREPLICA action completed successfully, moved replica=%s at node=%s " +
          "to replica=%s at node=%s", replica.getCoreName(), replica.getNodeName(), newCoreName, targetNode);
      results.add("success", successString);
    }
  }
}
