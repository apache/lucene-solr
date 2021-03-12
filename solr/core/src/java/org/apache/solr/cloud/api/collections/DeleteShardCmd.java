
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

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DeleteShardCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;
  private final TimeSource timeSource;

  public DeleteShardCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
    this.timeSource = ocmh.cloudManager.getTimeSource();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public CollectionCmdResponse.Response call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    String extCollectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collectionName;
    if (followAliases) {
      collectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }


    log.info("Delete shard invoked {}", message);

    boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);

    Slice slice = clusterState.getCollection(collectionName).getSlice(sliceId);
    if (slice == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Invalid shard. No shard with name " + sliceId + " exists for collection " + collectionName);

    // For now, only allow for deletions of Inactive slices or custom hashes (range==null).
    // TODO: Add check for range gaps on Slice deletion
    final Slice.State state = slice.getState();



    boolean force = message.getBool("force", false);

    if (!force && !(slice.getRange() == null || state == Slice.State.INACTIVE || state == Slice.State.RECOVERY
        || state == Slice.State.CONSTRUCTION) || state == Slice.State.RECOVERY_FAILED) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The slice: " + slice.getName() + " is currently " + state
          + ". Only non-active (or custom-hashed) slices can be deleted.");
    }

    if (state == Slice.State.RECOVERY)  {
      // mark the slice as 'construction' and only then try to delete the cores
      // see SOLR-9455
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      propMap.put(sliceId, Slice.State.CONSTRUCTION.toString());
      propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
     // ZkNodeProps m = new ZkNodeProps(propMap);
      clusterState = new SliceMutator(ocmh.cloudManager).updateShardState(clusterState, message);
    }

    slice = clusterState.getCollection(collectionName).getSlice(sliceId);

    String asyncId = message.getStr(ASYNC);
    List<CollectionCmdResponse.Response> finalizers = new ArrayList<>();

    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);
    OverseerCollectionMessageHandler.ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(message.getStr("async"), message.getStr(Overseer.QUEUE_OPERATION));
    try {
      List<ZkNodeProps> replicas = getReplicasForSlice(collectionName, slice);

      for (ZkNodeProps r : replicas) {
        final ZkNodeProps replica = r.plus(message.getProperties()).plus("parallel", "true").plus(ASYNC, asyncId);
        if (log.isInfoEnabled()) {
          log.info("Deleting replica for collection={} shard={} on node={}", replica.getStr(COLLECTION_PROP), replica.getStr(SHARD_ID_PROP), replica.getStr(CoreAdminParams.NODE));
        }
        @SuppressWarnings({"rawtypes"}) NamedList deleteResult = new NamedList();
        try {

          // MRM TODO: - return results from deleteReplica cmd
          CollectionCmdResponse.Response resp = ((DeleteReplicaCmd) ocmh.commandMap.get(DELETEREPLICA)).deleteReplica(
              clusterState, replica, shardHandler, shardRequestTracker, deleteResult, collectionName, sliceId);
          if (resp.asyncFinalRunner != null) {
            finalizers.add(resp);
          }
          clusterState = resp.clusterState;
        } catch (KeeperException e) {
          log.warn("Error deleting replica: {}", r, e);
          throw e;
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          log.warn("Error deleting replica: {}", r, e);
          throw e;
        }
      } log.debug("Waiting for delete shard action to complete");

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, DELETESHARD.toLower(), ZkStateReader.COLLECTION_PROP, collectionName, ZkStateReader.SHARD_ID_PROP, sliceId);

      clusterState = new CollectionMutator(ocmh.cloudManager).deleteShard(clusterState, m);


      log.info("Successfully deleted collection: {} , shard: {}", collectionName, sliceId);
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error executing delete operation for collection: " + collectionName + " shard: " + sliceId, e);
    }

    CollectionCmdResponse.Response response = new CollectionCmdResponse.Response();
    ClusterState finalClusterState = clusterState;
    response.asyncFinalRunner = () -> {
      CollectionCmdResponse.Response resp = new CollectionCmdResponse.Response();

      for (CollectionCmdResponse.Response finalize : finalizers) {
        finalize.asyncFinalRunner.call();
        if (finalize.writeFuture != null) {
          finalize.writeFuture.get();
        }
      }
      try {
        if (log.isDebugEnabled())  log.debug("Processs responses");
        shardRequestTracker.processResponses(results, shardHandler, true, "Delete shard command failed");
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
      if (waitForFinalState) {
        ocmh.overseer.getZkStateWriter().enqueueUpdate(finalClusterState.getCollection(collectionName), null, false).get();
        ocmh.overseer.writePendingUpdates(collectionName).get();
        ocmh.overseer.getZkStateReader().waitForState(collectionName, 10, TimeUnit.SECONDS, (liveNodes, coll) -> {
          if (coll == null) {
            return true;
          }
          Slice s = coll.getSlice(sliceId);
          if (s == null) {
            return true;
          }
          return false;
        });
      }

      return resp;
    };
    response.clusterState = clusterState;
    return response;
  }

  private List<ZkNodeProps> getReplicasForSlice(String collectionName, Slice slice) {
    List<ZkNodeProps> sourceReplicas = new ArrayList<>();
    for (Replica replica : slice.getReplicas()) {
      ZkNodeProps props = new ZkNodeProps(
          COLLECTION_PROP, collectionName,
          SHARD_ID_PROP, slice.getName(),
          ZkStateReader.REPLICA_PROP, replica.getName(),
          CoreAdminParams.NODE, replica.getNodeName());
      sourceReplicas.add(props);
    }
    return sourceReplicas;
  }
}
