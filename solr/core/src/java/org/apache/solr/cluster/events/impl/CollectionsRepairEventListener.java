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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cluster.events.NodesDownEvent;
import org.apache.solr.cluster.events.ReplicasDownEvent;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an (incomplete) illustration how to re-implement the combination of 8x
 * NodeLostTrigger and AutoAddReplicasPlanAction to maintain the collection's replication factor.
 */
public class CollectionsRepairEventListener implements ClusterSingleton, ClusterEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Set<ClusterEvent.EventType> EVENT_TYPES = new HashSet<>(
      Arrays.asList(
          ClusterEvent.EventType.NODES_DOWN,
          ClusterEvent.EventType.REPLICAS_DOWN
      ));

  private final CoreContainer cc;
  private final SolrClientCache solrClientCache;
  private final SolrCloudManager solrCloudManager;

  private boolean running = false;

  public CollectionsRepairEventListener(CoreContainer cc) {
    this.cc = cc;
    this.solrClientCache = cc.getSolrClientCache();
    this.solrCloudManager = cc.getZkController().getSolrCloudManager();
  }

  @Override
  public Set<ClusterEvent.EventType> getEventTypes() {
    return EVENT_TYPES;
  }

  @Override
  public void onEvent(ClusterEvent event) {
    if (!isRunning()) {
      // ignore the event
      return;
    }
    switch (event.getType()) {
      case NODES_DOWN:
        handleNodesDown((NodesDownEvent) event);
        break;
      case NODES_UP:
        // ignore? rebalance replicas?
        break;
      case REPLICAS_DOWN:
        handleReplicasDown((ReplicasDownEvent) event);
        break;
      default:
        log.warn("Unsupported event {}, ignoring...", event);
    }
  }

  private void handleNodesDown(NodesDownEvent event) {
    // collect all lost replicas
    List<Replica> lostReplicas = new ArrayList<>();
    try {
      ClusterState clusterState = solrCloudManager.getClusterStateProvider().getClusterState();
      clusterState.forEachCollection(coll -> {
        coll.forEachReplica((shard, replica) -> {
          if (event.getNodeNames().contains(replica.getNodeName())) {
            lostReplicas.add(replica);
          }
        });
      });
    } catch (IOException e) {
      log.warn("Exception getting cluster state", e);
      return;
    }

    // compute new placements for all replicas from lost nodes
    // send MOVEREPLICA admin requests for each lost replica
  }

  private void handleReplicasDown(ReplicasDownEvent event) {
    // compute new placements for all replicas that went down
    // send ADDREPLICA admin request for each lost replica
  }

  @Override
  public void start() throws Exception {
    running = true;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void stop() {
    running = false;
  }
}
