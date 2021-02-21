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

import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplaceNodeCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public ReplaceNodeCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public AddReplicaCmd.Response call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    String source = message.getStr(CollectionParams.SOURCE_NODE, message.getStr("source"));
    String target = message.getStr(CollectionParams.TARGET_NODE, message.getStr("target"));
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);
    if (source == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "sourceNode is a required param");
    }
    String async = message.getStr("async");
    int timeout = message.getInt("timeout", 10 * 60); // 10 minutes
    boolean parallel = message.getBool("parallel", false);

    if (!zkStateReader.isNodeLive(source)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Source Node: " + source + " is not live");
    }
    if (target != null && !zkStateReader.isNodeLive(target)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Target Node: " + target + " is not live");
    }
    List<ZkNodeProps> sourceReplicas = getReplicasOfNode(source, clusterState);
    // how many leaders are we moving? for these replicas we have to make sure that either:
    // * another existing replica can become a leader, or
    // * we wait until the newly created replica completes recovery (and can become the new leader)
    // If waitForFinalState=true we wait for all replicas
    int numLeaders = 0;
    for (ZkNodeProps props : sourceReplicas) {
      if (props.getBool(ZkStateReader.LEADER_PROP, false) || waitForFinalState) {
        numLeaders++;
      }
    }
    // map of collectionName_coreNodeName to watchers

    List<ZkNodeProps> createdReplicas = new ArrayList<>();

    AtomicBoolean anyOneFailed = new AtomicBoolean(false);

    List<Runnable> runners = new ArrayList<>();


    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);
    OverseerCollectionMessageHandler.ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(async, message.getStr(Overseer.QUEUE_OPERATION));
    for (ZkNodeProps sourceReplica : sourceReplicas) {
      @SuppressWarnings({"rawtypes"}) NamedList nl = new NamedList();
      String sourceCollection = sourceReplica.getStr(COLLECTION_PROP);
      if (log.isInfoEnabled()) {
        log.info("Going to create replica for collection={} shard={} on node={}", sourceCollection, sourceReplica.getStr(SHARD_ID_PROP), target);
      }
      String targetNode = target;
      if (targetNode == null) {
        Replica.Type replicaType = Replica.Type.get(sourceReplica.getStr(ZkStateReader.REPLICA_TYPE));
        int numNrtReplicas = replicaType == Replica.Type.NRT ? 1 : 0;
        int numTlogReplicas = replicaType == Replica.Type.TLOG ? 1 : 0;
        int numPullReplicas = replicaType == Replica.Type.PULL ? 1 : 0;
        Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder().forCollection(sourceCollection).forShard(Collections.singletonList(sourceReplica.getStr(SHARD_ID_PROP)))
            .assignNrtReplicas(numNrtReplicas).assignTlogReplicas(numTlogReplicas).assignPullReplicas(numPullReplicas)
            .onNodes(new ArrayList<>(ocmh.cloudManager.getClusterStateProvider().getLiveNodes())).build();
        Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(ocmh.cloudManager);
        Assign.AssignStrategy assignStrategy = assignStrategyFactory.create();
        targetNode = assignStrategy.assign(ocmh.cloudManager, assignRequest).get(0).node;
      }
      ZkNodeProps msg = sourceReplica.plus("parallel", String.valueOf(parallel)).plus(CoreAdminParams.NODE, targetNode);
      log.info("Add replacement replica {}", msg);
      AddReplicaCmd.Response response = new AddReplicaCmd(ocmh).addReplica(clusterState, msg, shardHandler, shardRequestTracker, nl);
      clusterState = response.clusterState;
      Runnable runner = () -> {
        final ZkNodeProps addedReplica = response.responseProps.get(0);
        log.info("Response props for replica are {} {}", msg, addedReplica);
        if (addedReplica != null) {
          createdReplicas.add(addedReplica);
          if (sourceReplica.getBool(ZkStateReader.LEADER_PROP, false) || waitForFinalState) {
            String shardName = sourceReplica.getStr(SHARD_ID_PROP);
            String replicaName = sourceReplica.getStr(ZkStateReader.REPLICA_PROP);
            String collectionName = sourceCollection;
            String key = collectionName + "_" + replicaName;
            CollectionStateWatcher watcher;
          //  if (waitForFinalState) {
              watcher = new ActiveReplicaWatcher(collectionName, Collections.singletonList(replicaName), null, null);
           // } else {
         //     watcher = new LeaderRecoveryWatcher(collectionName, shardName, replicaName, addedReplica.getStr(ZkStateReader.CORE_NAME_PROP), null);
         //   }
            log.debug("--- adding {}, {}", key, watcher);

          } else {
            log.debug("--- not waiting for {}", addedReplica);
          }
        }
      };
      runners.add(runner);
    }

    ocmh.overseer.getZkStateWriter().enqueueUpdate(clusterState, null, false);
    ocmh.overseer.writePendingUpdates();

    AddReplicaCmd.Response response = new AddReplicaCmd.Response();
    response.results = results;
    response.clusterState = clusterState;

    int finalNumLeaders = numLeaders;
    ClusterState finalClusterState = clusterState;
    response.asyncFinalRunner = () -> {

      if (log.isDebugEnabled()) log.debug("Waiting for replicas to be added");

      try {
        shardRequestTracker.processResponses(results, shardHandler, false, null, Collections.emptySet());
      } catch (KeeperException e) {
        log.error("", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

      for (Runnable runner : runners) {
        runner.run();
      }

      if (log.isDebugEnabled()) log.debug("Finished waiting for replicas to be added");

      if (anyOneFailed.get()) {
        log.info("Failed to create some replicas. Cleaning up all replicas on target node");

        for (ZkNodeProps createdReplica : createdReplicas) {
          @SuppressWarnings({"rawtypes"}) NamedList deleteResult = new NamedList();
          try {
            // MRM TODO: - return results from deleteReplica cmd, update clusterstate
            AddReplicaCmd.Response dr = ocmh.deleteReplica(finalClusterState, createdReplica.plus("parallel", "true"), deleteResult);

          } catch (KeeperException e) {

            log.warn("Error deleting replica ", e);
          } catch (Exception e) {
            ParWork.propagateInterrupt(e);
            log.warn("Error deleting replica ", e);

            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
          }
        }

      }

      // we have reached this far means all replicas could be recreated
      //now cleanup the replicas in the source node
      try {
        ShardHandler sh = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);
        OverseerCollectionMessageHandler.ShardRequestTracker srt = ocmh.asyncRequestTracker(message.getStr("async"), message.getStr(Overseer.QUEUE_OPERATION));

        log.info("Cleanup replicas {}", sourceReplicas);
        AddReplicaCmd.Response r = DeleteNodeCmd.cleanupReplicas(results, finalClusterState, sourceReplicas, ocmh, source, null, sh, srt);

        try {
          if (log.isDebugEnabled())  log.debug("Processs responses");
          shardRequestTracker.processResponses(results, shardHandler, true, "Delete node command failed");
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        r.asyncFinalRunner.call();

        results.add("success", "REPLACENODE action completed successfully from  : " + source + " to : " + target);
        AddReplicaCmd.Response resp = new AddReplicaCmd.Response();
        resp.clusterState = r.clusterState;
        return resp;
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    };

    return response;
  }

  static List<ZkNodeProps> getReplicasOfNode(String source, ClusterState state) {
    List<ZkNodeProps> sourceReplicas = new ArrayList<>();
    for (Map.Entry<String, DocCollection> e : state.getCollectionsMap().entrySet()) {
      for (Slice slice : e.getValue().getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (source.equals(replica.getNodeName())) {
            ZkNodeProps props = new ZkNodeProps(
                COLLECTION_PROP, e.getKey(),
                SHARD_ID_PROP, slice.getName(),
                ZkStateReader.REPLICA_PROP, replica.getName(),
                ZkStateReader.REPLICA_TYPE, replica.getType().name(),
                ZkStateReader.LEADER_PROP, String.valueOf(replica.equals(slice.getLeader())),
                CoreAdminParams.NODE, source);
            sourceReplicas.add(props);
          }
        }
      }
    }
    return sourceReplicas;
  }

}
