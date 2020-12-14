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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.common.cloud.ReplicaPosition;

class ReplicaPlacementImpl implements ReplicaPlacement {
  private final SolrCollection solrCollection;
  private final String shardName;
  private final Node node;
  private final Replica.ReplicaType replicaType;

  ReplicaPlacementImpl(SolrCollection solrCollection, String shardName, Node node, Replica.ReplicaType replicaType) {
    this.solrCollection = solrCollection;
    this.shardName = shardName;
    this.node = node;
    this.replicaType = replicaType;
  }

  @Override
  public SolrCollection getCollection() {
    return solrCollection;
  }

  @Override
  public String getShardName() {
    return shardName;
  }

  @Override
  public Node getNode() {
    return node;
  }

  @Override
  public Replica.ReplicaType getReplicaType() {
    return replicaType;
  }

  @Override
  public String toString() {
    return solrCollection.getName() + "/" + shardName + "/" + replicaType + "->" + node.getName();
  }

  /**
   * Translates a set of {@link ReplicaPlacement} returned by a plugin into a list of {@link ReplicaPosition} expected
   * by {@link org.apache.solr.cloud.api.collections.Assign.AssignStrategy}
   */
  static List<ReplicaPosition> toReplicaPositions(Set<ReplicaPlacement> replicaPlacementSet) {
    // The replica index in ReplicaPosition is not as strict a concept as it might seem. It is used in rules
    // based placement (for sorting replicas) but its presence in ReplicaPosition is not justified (and when the code
    // is executing here, it means rules based placement is not used).
    // Looking at ReplicaAssigner.tryAllPermutations, it is well possible to create replicas with same index
    // living on a given node for the same shard. This likely never happens because of the way replicas are
    // placed on nodes (never two on the same node for same shard). Adopting the same shortcut/bad design here,
    // but index should be removed at some point from ReplicaPosition.
    List<ReplicaPosition> replicaPositions = new ArrayList<>(replicaPlacementSet.size());
    int index = 0; // This really an arbitrary value when adding replicas and a possible source of core name collisions
    for (ReplicaPlacement placement : replicaPlacementSet) {
      replicaPositions.add(new ReplicaPosition(placement.getShardName(), index++, SimpleClusterAbstractionsImpl.ReplicaImpl.toCloudReplicaType(placement.getReplicaType()), placement.getNode().getName()));
    }

    return replicaPositions;
  }
}
