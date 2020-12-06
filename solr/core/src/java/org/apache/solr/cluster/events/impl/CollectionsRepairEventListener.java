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
package org.apache.solr.cluster.events.impl;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.NodesDownEvent;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an illustration how to re-implement the combination of Solr 8x
 * NodeLostTrigger and AutoAddReplicasPlanAction to maintain the collection's replicas when
 * nodes are lost.
 * <p>The notion of <code>waitFor</code> delay between detection and repair action is
 * implemented as a scheduled execution of the repair method, which is called every 1 sec
 * to check whether there are any lost nodes that exceeded their <code>waitFor</code> period.</p>
 * <p>NOTE: this functionality would be probably more reliable when executed also as a
 * periodically scheduled check - both as a reactive (listener) and proactive (scheduled) measure.</p>
 */
public class CollectionsRepairEventListener implements ClusterEventListener, ClusterSingleton, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PLUGIN_NAME = "collectionsRepairListener";
  public static final int DEFAULT_WAIT_FOR_SEC = 30;

  private static final String ASYNC_ID_PREFIX = "_async_" + PLUGIN_NAME;
  private static final AtomicInteger counter = new AtomicInteger();

  private final SolrClient solrClient;
  private final SolrCloudManager solrCloudManager;

  private State state = State.STOPPED;

  private int waitForSecond = DEFAULT_WAIT_FOR_SEC;

  private ScheduledThreadPoolExecutor waitForExecutor;
  private final PlacementPluginFactory<? extends PlacementPluginConfig> placementPluginFactory;

  public CollectionsRepairEventListener(CoreContainer cc) {
    this.solrClient = cc.getSolrClientCache().getCloudSolrClient(cc.getZkController().getZkClient().getZkServerAddress());
    this.solrCloudManager = cc.getZkController().getSolrCloudManager();
    this.placementPluginFactory = cc.getPlacementPluginFactory();
  }

  @VisibleForTesting
  public void setWaitForSecond(int waitForSecond) {
    if (log.isDebugEnabled()) {
      log.debug("-- setting waitFor={}", waitForSecond);
    }
    this.waitForSecond = waitForSecond;
  }

  @Override
  public String getName() {
    return PLUGIN_NAME;
  }

  @Override
  public void onEvent(ClusterEvent event) {
    if (state != State.RUNNING) {
      // ignore the event
      return;
    }
    switch (event.getType()) {
      case NODES_DOWN:
        handleNodesDown((NodesDownEvent) event);
        break;
      default:
        log.warn("Unsupported event {}, ignoring...", event);
    }
  }

  private final Map<String, Long> nodeNameVsTimeRemoved = new ConcurrentHashMap<>();

  private void handleNodesDown(NodesDownEvent event) {

    // tracking for the purpose of "waitFor" delay

    // have any nodes that we were tracking been added to the cluster?
    // if so, remove them from the tracking map
    Set<String> trackingKeySet = nodeNameVsTimeRemoved.keySet();
    trackingKeySet.removeAll(solrCloudManager.getClusterStateProvider().getLiveNodes());
    // add any new lost nodes (old lost nodes are skipped)
    event.getNodeNames().forEachRemaining(lostNode -> nodeNameVsTimeRemoved.computeIfAbsent(lostNode, n -> solrCloudManager.getTimeSource().getTimeNs()));
  }

  private void runRepair() {
    if (nodeNameVsTimeRemoved.isEmpty()) {
      // nothing to do
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("-- runRepair for {} lost nodes", nodeNameVsTimeRemoved.size());
    }
    Set<String> reallyLostNodes = new HashSet<>();
    nodeNameVsTimeRemoved.forEach((lostNode, timeRemoved) -> {
      long now = solrCloudManager.getTimeSource().getTimeNs();
      long te = TimeUnit.SECONDS.convert(now - timeRemoved, TimeUnit.NANOSECONDS);
      if (te >= waitForSecond) {
        reallyLostNodes.add(lostNode);
      }
    });
    if (reallyLostNodes.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug("--- skipping repair, {} nodes are still in waitFor period", nodeNameVsTimeRemoved.size());
      }
      return;
    } else {
      if (log.isDebugEnabled()) {
        log.debug("--- running repair for nodes that are still lost after waitFor: {}", reallyLostNodes);
      }
    }
    // collect all lost replicas
    // collection / positions
    Map<String, List<ReplicaPosition>> newPositions = new HashMap<>();
    try {
      ClusterState clusterState = solrCloudManager.getClusterStateProvider().getClusterState();
      clusterState.forEachCollection(coll -> {
        // shard / type / count
        Map<String, Map<Replica.Type, AtomicInteger>> lostReplicas = new HashMap<>();
        coll.forEachReplica((shard, replica) -> {
          if (reallyLostNodes.contains(replica.getNodeName())) {
            lostReplicas.computeIfAbsent(shard, s -> new HashMap<>())
                .computeIfAbsent(replica.type, t -> new AtomicInteger())
                .incrementAndGet();
          }
        });
        Assign.AssignStrategy assignStrategy = Assign.createAssignStrategy(placementPluginFactory.createPluginInstance(), clusterState, coll);
        lostReplicas.forEach((shard, types) -> {
          Assign.AssignRequestBuilder assignRequestBuilder = new Assign.AssignRequestBuilder()
              .forCollection(coll.getName())
              .forShard(Collections.singletonList(shard));
          types.forEach((type, count) -> {
            switch (type) {
              case NRT:
                assignRequestBuilder.assignNrtReplicas(count.get());
                break;
              case PULL:
                assignRequestBuilder.assignPullReplicas(count.get());
                break;
              case TLOG:
                assignRequestBuilder.assignTlogReplicas(count.get());
                break;
            }
          });
          Assign.AssignRequest assignRequest = assignRequestBuilder.build();
          try {
            List<ReplicaPosition> positions = assignStrategy.assign(solrCloudManager, assignRequest);
            newPositions.put(coll.getName(), positions);
          } catch (Exception e) {
            log.warn("Exception computing positions for {}/{}: {}", coll.getName(), shard, e);
          }
        });
      });
    } catch (IOException e) {
      log.warn("Exception getting cluster state", e);
      return;
    }

    // remove all nodes with expired waitFor from the tracking set
    nodeNameVsTimeRemoved.keySet().removeAll(reallyLostNodes);

    // send ADDREPLICA admin requests for each lost replica
    // XXX should we use 'async' for that, to avoid blocking here?
    List<CollectionAdminRequest.AddReplica> addReplicas = new ArrayList<>();
    newPositions.forEach((collection, positions) -> positions.forEach(position -> {
      CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest
          .addReplicaToShard(collection, position.shard, position.type);
      addReplica.setNode(position.node);
      addReplica.setAsyncId(ASYNC_ID_PREFIX + counter.incrementAndGet());
      addReplicas.add(addReplica);
    }));
    addReplicas.forEach(addReplica -> {
      try {
        solrClient.request(addReplica);
      } catch (Exception e) {
        log.warn("Exception calling ADDREPLICA {}: {}", addReplica.getParams().toQueryString(), e);
      }
    });
  }

  @Override
  public void start() throws Exception {
    state = State.STARTING;
    waitForExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1,
        new SolrNamedThreadFactory("collectionsRepair_waitFor"));
    waitForExecutor.setRemoveOnCancelPolicy(true);
    waitForExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    waitForExecutor.scheduleAtFixedRate(this::runRepair, 0, waitForSecond, TimeUnit.SECONDS);
    state = State.RUNNING;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void stop() {
    state = State.STOPPING;
    waitForExecutor.shutdownNow();
    try {
      waitForExecutor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.warn("Failed to shut down the waitFor executor - interrupted...");
      Thread.currentThread().interrupt();
    }
    waitForExecutor = null;
    state = State.STOPPED;
  }

  @Override
  public void close() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("-- close() called");
    }
    stop();
  }
}
