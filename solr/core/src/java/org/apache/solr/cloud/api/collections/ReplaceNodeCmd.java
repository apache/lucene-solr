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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.common.SolrCloseableLatch;
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
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class ReplaceNodeCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public ReplaceNodeCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
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
    ClusterState clusterState = zkStateReader.getClusterState();

    if (!clusterState.liveNodesContain(source)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Source Node: " + source + " is not live");
    }
    if (target != null && !clusterState.liveNodesContain(target)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Target Node: " + target + " is not live");
    } else if (clusterState.getLiveNodes().size() <= 1) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No nodes other than the source node: " + source + " are live, therefore replicas cannot be moved");
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
    Map<String, CollectionStateWatcher> watchers = new HashMap<>();
    List<ZkNodeProps> createdReplicas = new ArrayList<>();

    AtomicBoolean anyOneFailed = new AtomicBoolean(false);
    SolrCloseableLatch countDownLatch = new SolrCloseableLatch(sourceReplicas.size(), ocmh);

    SolrCloseableLatch replicasToRecover = new SolrCloseableLatch(numLeaders, ocmh);
    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapperRef = new AtomicReference<>();
    try {
      for (ZkNodeProps sourceReplica : sourceReplicas) {
        @SuppressWarnings({"rawtypes"})
        NamedList nl = new NamedList();
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
          Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
              .forCollection(sourceCollection)
              .forShard(Collections.singletonList(sourceReplica.getStr(SHARD_ID_PROP)))
              .assignNrtReplicas(numNrtReplicas)
              .assignTlogReplicas(numTlogReplicas)
              .assignPullReplicas(numPullReplicas)
              .onNodes(new ArrayList<>(ocmh.cloudManager.getClusterStateProvider().getLiveNodes().stream().filter(node -> !node.equals(source)).collect(Collectors.toList())))
              .build();
          Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(ocmh.cloudManager);
          Assign.AssignStrategy assignStrategy = assignStrategyFactory.create(clusterState, clusterState.getCollection(sourceCollection));
          targetNode = assignStrategy.assign(ocmh.cloudManager, assignRequest).get(0).node;
          sessionWrapperRef.set(PolicyHelper.getLastSessionWrapper(true));
        }
        ZkNodeProps msg = sourceReplica.plus("parallel", String.valueOf(parallel)).plus(CoreAdminParams.NODE, targetNode);
        if (async != null) msg.getProperties().put(ASYNC, async);
        final ZkNodeProps addedReplica = ocmh.addReplica(clusterState,
            msg, nl, () -> {
              countDownLatch.countDown();
              if (nl.get("failure") != null) {
                String errorString = String.format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" +
                    " on node=%s", sourceCollection, sourceReplica.getStr(SHARD_ID_PROP), target);
                log.warn(errorString);
                // one replica creation failed. Make the best attempt to
                // delete all the replicas created so far in the target
                // and exit
                synchronized (results) {
                  results.add("failure", errorString);
                  anyOneFailed.set(true);
                }
              } else {
                if (log.isDebugEnabled()) {
                  log.debug("Successfully created replica for collection={} shard={} on node={}",
                      sourceCollection, sourceReplica.getStr(SHARD_ID_PROP), target);
                }
              }
            }).get(0);

        if (addedReplica != null) {
          createdReplicas.add(addedReplica);
          if (sourceReplica.getBool(ZkStateReader.LEADER_PROP, false) || waitForFinalState) {
            String shardName = sourceReplica.getStr(SHARD_ID_PROP);
            String replicaName = sourceReplica.getStr(ZkStateReader.REPLICA_PROP);
            String collectionName = sourceCollection;
            String key = collectionName + "_" + replicaName;
            CollectionStateWatcher watcher;
            if (waitForFinalState) {
              watcher = new ActiveReplicaWatcher(collectionName, null,
                  Collections.singletonList(addedReplica.getStr(ZkStateReader.CORE_NAME_PROP)), replicasToRecover);
            } else {
              watcher = new LeaderRecoveryWatcher(collectionName, shardName, replicaName,
                  addedReplica.getStr(ZkStateReader.CORE_NAME_PROP), replicasToRecover);
            }
            watchers.put(key, watcher);
            log.debug("--- adding {}, {}", key, watcher);
            zkStateReader.registerCollectionStateWatcher(collectionName, watcher);
          } else {
            log.debug("--- not waiting for {}", addedReplica);
          }
        }
      }

      log.debug("Waiting for replicas to be added");
      if (!countDownLatch.await(timeout, TimeUnit.SECONDS)) {
        log.info("Timed out waiting for replicas to be added");
        anyOneFailed.set(true);
      } else {
        log.debug("Finished waiting for replicas to be added");
      }
    } finally {
      PolicyHelper.SessionWrapper sw = sessionWrapperRef.get();
      if (sw != null) sw.release();
    }
    // now wait for leader replicas to recover
    log.debug("Waiting for {} leader replicas to recover", numLeaders);
    if (!replicasToRecover.await(timeout, TimeUnit.SECONDS)) {
      if (log.isInfoEnabled()) {
        log.info("Timed out waiting for {} leader replicas to recover", replicasToRecover.getCount());
      }
      anyOneFailed.set(true);
    } else {
      log.debug("Finished waiting for leader replicas to recover");
    }
    // remove the watchers, we're done either way
    for (Map.Entry<String, CollectionStateWatcher> e : watchers.entrySet()) {
      zkStateReader.removeCollectionStateWatcher(e.getKey(), e.getValue());
    }
    if (anyOneFailed.get()) {
      log.info("Failed to create some replicas. Cleaning up all replicas on target node");
      SolrCloseableLatch cleanupLatch = new SolrCloseableLatch(createdReplicas.size(), ocmh);
      for (ZkNodeProps createdReplica : createdReplicas) {
        @SuppressWarnings({"rawtypes"})
        NamedList deleteResult = new NamedList();
        try {
          ocmh.deleteReplica(zkStateReader.getClusterState(), createdReplica.plus("parallel", "true"), deleteResult, () -> {
            cleanupLatch.countDown();
            if (deleteResult.get("failure") != null) {
              synchronized (results) {
                results.add("failure", "Could not cleanup, because of : " + deleteResult.get("failure"));
              }
            }
          });
        } catch (KeeperException e) {
          cleanupLatch.countDown();
          log.warn("Error deleting replica ", e);
        } catch (Exception e) {
          log.warn("Error deleting replica ", e);
          cleanupLatch.countDown();
          throw e;
        }
      }
      cleanupLatch.await(5, TimeUnit.MINUTES);
      return;
    }


    // we have reached this far means all replicas could be recreated
    //now cleanup the replicas in the source node
    DeleteNodeCmd.cleanupReplicas(results, state, sourceReplicas, ocmh, source, async);
    results.add("success", "REPLACENODE action completed successfully from  : " + source + " to : " + target);
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
                ZkStateReader.CORE_NAME_PROP, replica.getCoreName(),
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
