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

package org.apache.solr.cluster.placement.impl;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.PlacementRequest;

public class PlacementRequestImpl implements PlacementRequest {
  private final SolrCollection solrCollection;
  private final Set<String> shardNames;
  private final Set<Node> targetNodes;
  private final EnumMap<Replica.ReplicaType, Integer> countReplicas = new EnumMap<>(Replica.ReplicaType.class);

  public PlacementRequestImpl(SolrCollection solrCollection,
                              Set<String> shardNames, Set<Node> targetNodes,
                              int countNrtReplicas, int countTlogReplicas, int countPullReplicas) {
    this.solrCollection = solrCollection;
    this.shardNames = shardNames;
    this.targetNodes = targetNodes;
    // Initializing map for all values of enum, so unboxing always possible later without checking for null
    countReplicas.put(Replica.ReplicaType.NRT, countNrtReplicas);
    countReplicas.put(Replica.ReplicaType.TLOG, countTlogReplicas);
    countReplicas.put(Replica.ReplicaType.PULL, countPullReplicas);
  }

  @Override
  public SolrCollection getCollection() {
    return solrCollection;
  }

  @Override
  public Set<String> getShardNames() {
    return shardNames;
  }

  @Override
  public Set<Node> getTargetNodes() {
    return targetNodes;
  }

  @Override
  public int getCountReplicasToCreate(Replica.ReplicaType replicaType) {
    return countReplicas.get(replicaType);

  }

  /**
   * Returns a {@link PlacementRequest} that can be consumed by a plugin based on an internal Assign.AssignRequest
   * for adding replicas + additional info (upon creation of a new collection or adding replicas to an existing one).
   */
  static PlacementRequestImpl toPlacementRequest(Cluster cluster, SolrCollection solrCollection,
                                                 Assign.AssignRequest assignRequest) throws Assign.AssignmentException {
    Set<String> shardNames = new HashSet<>(assignRequest.shardNames);
    if (shardNames.size() < 1) {
      throw new Assign.AssignmentException("Bad assign request: no shards specified for collection " + solrCollection.getName());
    }

    final Set<Node> nodes;
    // If no nodes specified, use all live nodes. If nodes are specified, use specified list.
    if (assignRequest.nodes != null) {
      nodes = SimpleClusterAbstractionsImpl.NodeImpl.getNodes(assignRequest.nodes);
      if (nodes.isEmpty()) {
        throw new Assign.AssignmentException("Bad assign request: empty list of nodes for collection " + solrCollection.getName());
      }
    } else {
      nodes = cluster.getLiveNodes();
      if (nodes.isEmpty()) {
        throw new Assign.AssignmentException("Impossible assign request: no live nodes for collection " + solrCollection.getName());
      }
    }

    return new PlacementRequestImpl(solrCollection, shardNames, nodes,
        assignRequest.numNrtReplicas, assignRequest.numTlogReplicas, assignRequest.numPullReplicas);
  }
}
