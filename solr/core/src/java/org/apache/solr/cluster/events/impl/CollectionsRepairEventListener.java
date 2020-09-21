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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cluster.events.NodesDownEvent;
import org.apache.solr.cluster.events.ReplicasDownEvent;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an illustration how to re-implement the combination of 8x
 * NodeLostTrigger and AutoAddReplicasPlanAction to maintain the collection's replication factor.
 * <p>NOTE: there's no support for 'waitFor' yet.</p>
 * <p>NOTE 2: this functionality would be probably more reliable when executed also as a
 * periodically scheduled check - both as a reactive (listener) and proactive (scheduled) measure.</p>
 */
public class CollectionsRepairEventListener implements ClusterSingleton, ClusterEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String ASYNC_ID_PREFIX = "_col_repair_";
  private static final AtomicInteger counter = new AtomicInteger();

  private final SolrClient solrClient;
  private final SolrCloudManager solrCloudManager;

  private boolean running = false;

  public CollectionsRepairEventListener(CoreContainer cc) {
    this.solrClient = cc.getSolrClientCache().getCloudSolrClient(cc.getZkController().getZkClient().getZkServerAddress());
    this.solrCloudManager = cc.getZkController().getSolrCloudManager();
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
      case REPLICAS_DOWN:
        handleReplicasDown((ReplicasDownEvent) event);
        break;
      default:
        log.warn("Unsupported event {}, ignoring...", event);
    }
  }

  private void handleNodesDown(NodesDownEvent event) {
    // collect all lost replicas
    // collection / positions
    Map<String, List<ReplicaPosition>> newPositions = new HashMap<>();
    try {
      ClusterState clusterState = solrCloudManager.getClusterStateProvider().getClusterState();
      Set<String> lostNodeNames = new HashSet<>();
      event.getNodeNames().forEachRemaining(lostNodeNames::add);
      clusterState.forEachCollection(coll -> {
        // shard / type / count
        Map<String, Map<Replica.Type, AtomicInteger>> lostReplicas = new HashMap<>();
        coll.forEachReplica((shard, replica) -> {
          if (lostNodeNames.contains(replica.getNodeName())) {
            lostReplicas.computeIfAbsent(shard, s -> new HashMap<>())
                .computeIfAbsent(replica.type, t -> new AtomicInteger())
                .incrementAndGet();
          }
        });
        Assign.AssignStrategy assignStrategy = Assign.createAssignStrategy(solrCloudManager, clusterState, coll);
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
            log.warn("Exception computing positions for " + coll.getName() + "/" + shard, e);
            return;
          }
        });
      });
    } catch (IOException e) {
      log.warn("Exception getting cluster state", e);
      return;
    }

    // send ADDREPLICA admin requests for each lost replica
    // XXX should we use 'async' for that, to avoid blocking here?
    List<CollectionAdminRequest.AddReplica> addReplicas = new ArrayList<>();
    newPositions.forEach((collection, positions) -> {
      positions.forEach(position -> {
        CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest
            .addReplicaToShard(collection, position.shard, position.type);
        addReplica.setNode(position.node);
        addReplica.setAsyncId(ASYNC_ID_PREFIX + counter.incrementAndGet());
        addReplicas.add(addReplica);
      });
    });
    addReplicas.forEach(addReplica -> {
      try {
        solrClient.request(addReplica);
      } catch (Exception e) {
        log.warn("Exception calling ADDREPLICA " + addReplica.getParams().toQueryString(), e);
      }
    });

    // ... and DELETERPLICA for lost ones?
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
