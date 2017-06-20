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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.SnapshotStatus;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.component.ShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the functionality of creating a collection level snapshot.
 */
public class CreateSnapshotCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public CreateSnapshotCmd (OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    String collectionName =  message.getStr(COLLECTION_PROP);
    String commitName =  message.getStr(CoreAdminParams.COMMIT_NAME);
    String asyncId = message.getStr(ASYNC);
    SolrZkClient zkClient = this.ocmh.overseer.getZkController().getZkClient();
    Date creationDate = new Date();

    if(SolrSnapshotManager.snapshotExists(zkClient, collectionName, commitName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Snapshot with name " + commitName
          + " already exists for collection " + collectionName);
    }

    log.info("Creating a snapshot for collection={} with commitName={}", collectionName, commitName);

    // Create a node in ZK to store the collection level snapshot meta-data.
    SolrSnapshotManager.createCollectionLevelSnapshot(zkClient, collectionName, new CollectionSnapshotMetaData(commitName));
    log.info("Created a ZK path to store snapshot information for collection={} with commitName={}", collectionName, commitName);

    Map<String, String> requestMap = new HashMap<>();
    NamedList shardRequestResults = new NamedList();
    Map<String, Slice> shardByCoreName = new HashMap<>();
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler();

    for (Slice slice : ocmh.zkStateReader.getClusterState().getCollection(collectionName).getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        if (replica.getState() != State.ACTIVE) {
          log.info("Replica {} is not active. Hence not sending the createsnapshot request", replica.getCoreName());
          continue; // Since replica is not active - no point sending a request.
        }

        String coreName = replica.getStr(CORE_NAME_PROP);

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATESNAPSHOT.toString());
        params.set(NAME, slice.getName());
        params.set(CORE_NAME_PROP, coreName);
        params.set(CoreAdminParams.COMMIT_NAME, commitName);

        ocmh.sendShardRequest(replica.getNodeName(), params, shardHandler, asyncId, requestMap);
        log.debug("Sent createsnapshot request to core={} with commitName={}", coreName, commitName);

        shardByCoreName.put(coreName, slice);
      }
    }

    // At this point we want to make sure that at-least one replica for every shard
    // is able to create the snapshot. If that is not the case, then we fail the request.
    // This is to take care of the situation where e.g. entire shard is unavailable.
    Set<String> failedShards = new HashSet<>();

    ocmh.processResponses(shardRequestResults, shardHandler, false, null, asyncId, requestMap);
    NamedList success = (NamedList) shardRequestResults.get("success");
    List<CoreSnapshotMetaData> replicas = new ArrayList<>();
    if (success != null) {
      for ( int i = 0 ; i < success.size() ; i++) {
        NamedList resp = (NamedList)success.getVal(i);

        // Check if this core is the leader for the shard. The idea here is that during the backup
        // operation we preferably use the snapshot of the "leader" replica since it is most likely
        // to have latest state.
        String coreName = (String)resp.get(CoreAdminParams.CORE);
        Slice slice = shardByCoreName.remove(coreName);
        boolean leader = (slice.getLeader() != null && slice.getLeader().getCoreName().equals(coreName));
        resp.add(SolrSnapshotManager.SHARD_ID, slice.getName());
        resp.add(SolrSnapshotManager.LEADER, leader);

        CoreSnapshotMetaData c = new CoreSnapshotMetaData(resp);
        replicas.add(c);
        log.info("Snapshot with commitName {} is created successfully for core {}", commitName, c.getCoreName());
      }
    }

    if (!shardByCoreName.isEmpty()) { // One or more failures.
      log.warn("Unable to create a snapshot with name {} for following cores {}", commitName, shardByCoreName.keySet());

      // Count number of failures per shard.
      Map<String, Integer> failuresByShardId = new HashMap<>();
      for (Map.Entry<String,Slice> entry : shardByCoreName.entrySet()) {
        int f = 0;
        if (failuresByShardId.get(entry.getValue().getName()) != null) {
          f = failuresByShardId.get(entry.getValue().getName());
        }
        failuresByShardId.put(entry.getValue().getName(), f + 1);
      }

      // Now that we know number of failures per shard, we can figure out
      // if at-least one replica per shard was able to create a snapshot or not.
      DocCollection collectionStatus = ocmh.zkStateReader.getClusterState().getCollection(collectionName);
      for (Map.Entry<String,Integer> entry : failuresByShardId.entrySet()) {
        int replicaCount = collectionStatus.getSlice(entry.getKey()).getReplicas().size();
        if (replicaCount <= entry.getValue()) {
          failedShards.add(entry.getKey());
        }
      }
    }

    if (failedShards.isEmpty()) { // No failures.
      CollectionSnapshotMetaData meta = new CollectionSnapshotMetaData(commitName, SnapshotStatus.Successful, creationDate, replicas);
      SolrSnapshotManager.updateCollectionLevelSnapshot(zkClient, collectionName, meta);
      log.info("Saved following snapshot information for collection={} with commitName={} in Zookeeper : {}", collectionName,
          commitName, meta.toNamedList());
    } else {
      log.warn("Failed to create a snapshot for collection {} with commitName = {}. Snapshot could not be captured for following shards {}",
          collectionName, commitName, failedShards);
      // Update the ZK meta-data to include only cores with the snapshot. This will enable users to figure out
      // which cores have the named snapshot.
      CollectionSnapshotMetaData meta = new CollectionSnapshotMetaData(commitName, SnapshotStatus.Failed, creationDate, replicas);
      SolrSnapshotManager.updateCollectionLevelSnapshot(zkClient, collectionName, meta);
      log.info("Saved following snapshot information for collection={} with commitName={} in Zookeeper : {}", collectionName,
          commitName, meta.toNamedList());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to create snapshot on shards " + failedShards);
    }
  }
}
